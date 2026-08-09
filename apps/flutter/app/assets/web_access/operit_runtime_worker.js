import { MessagePack } from "./operit_messagepack.js";
const workerGlobal = globalThis;
const controlStateIndex = 0;
const controlLengthIndex = 1;
const controlReady = 1;
const controlPayloadReady = 2;
const runtimeStorageDataFileName = "operit_runtime_storage.data";
const runtimeStorageIndexFileName = "operit_runtime_storage.index";
const archiveStagingDataFileName = "operit_archive_staging.data";
const runtimeStoragePrefix = "operit2.runtime.";
const legacySqlitePrefix = "operit2.sqlite.";
const runtimeStoragePrefixes = [runtimeStoragePrefix, "operit2.files.", legacySqlitePrefix];
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
let runtimeStorage = null;
let archiveStaging = null;
let runtimeStorageInitialization = null;
let nextHostCallId = 0;
const mainHostModules = new Map();
let activeHostClientId = null;
let runtimeOperationQueue = Promise.resolve();
workerGlobal.__OPERIT_RUNTIME_WORKER__ = true;
workerGlobal.__operitRuntimeWorkerEnsureStorage = initializeRuntimeWorkerStorage;
workerGlobal.__operitRuntimeWorkerStorage = runtimeStorageBridge();
workerGlobal.__operitRuntimeWorkerArchiveStaging = archiveStagingBridge();
const workerReady = initializeRuntimeWorker();
workerGlobal.addEventListener("message", handleWorkerMessage);
async function initializeRuntimeWorker() {
    await initializeRuntimeWorkerStorage();
    await importWorkerScript("./operit_runtime_bridge.js");
    installWorkerHostProxy();
}
function importWorkerScript(path) {
    const dynamicImport = new Function("path", "return import(path)");
    return dynamicImport(path);
}
function handleWorkerMessage(event) {
    const message = event.data;
    if (isWorkerHostPayload(message)) {
        receiveMainHostPayload(message);
        return;
    }
    if (isWorkerCoreRequest(message)) {
        enqueueCoreRequest(message);
    }
}
function enqueueCoreRequest(message) {
    const execute = async () => {
        try {
            await workerReady;
            await executeCoreRequest(message);
        }
        catch (error) {
            postWorkerMessage({ type: "coreError", id: message.id, message: errorMessage(error), clientId: message.clientId });
        }
    };
    runtimeOperationQueue = runtimeOperationQueue.then(execute, execute);
}
function isWorkerCoreRequest(value) {
    if (typeof value !== "object" || value === null) {
        return false;
    }
    const message = value;
    return message.type === "coreRequest" &&
        typeof message.id === "number" &&
        typeof message.operation === "string" &&
        (message.payload instanceof Uint8Array || typeof message.payload === "string");
}
function isWorkerHostPayload(value) {
    if (typeof value !== "object" || value === null) {
        return false;
    }
    const message = value;
    return message.type === "hostPayload" &&
        typeof message.id === "number" &&
        message.payload instanceof SharedArrayBuffer &&
        message.control instanceof SharedArrayBuffer;
}
async function executeCoreRequest(message) {
    const previousClientId = activeHostClientId;
    activeHostClientId = message.clientId ?? null;
    try {
        const runtime = workerGlobal.__operitRuntime;
        if (runtime === undefined) {
            throw new Error("worker runtime bridge is unavailable");
        }
        const result = await invokeRuntimeOperation(runtime, message);
        const response = Uint8Array.from(result);
        postWorkerMessage({ type: "coreResult", id: message.id, response, clientId: message.clientId }, [response.buffer]);
    }
    catch (error) {
        postWorkerMessage({ type: "coreError", id: message.id, message: errorMessage(error), clientId: message.clientId });
    }
    finally {
        activeHostClientId = previousClientId;
    }
}
function invokeRuntimeOperation(runtime, message) {
    if (message.operation === "call") {
        return runtime.call(requireBytesPayload(message));
    }
    if (message.operation === "pushOpen") {
        return runtime.pushOpen(requireBytesPayload(message));
    }
    if (message.operation === "pushItem") {
        return runtime.pushItem(requireBytesPayload(message));
    }
    if (message.operation === "pushClose") {
        return runtime.pushClose(requireStringPayload(message));
    }
    if (message.operation === "watchSnapshot") {
        return runtime.watchSnapshot(requireBytesPayload(message));
    }
    if (message.operation === "watchStream") {
        return runtime.watchStream(requireBytesPayload(message), event => {
            const copiedEvent = Uint8Array.from(event);
            postWorkerMessage({ type: "coreWatchEvent", id: message.id, event: copiedEvent, clientId: message.clientId }, [copiedEvent.buffer]);
        });
    }
    return runtime.closeWatchStream(requireStringPayload(message));
}
function requireBytesPayload(message) {
    if (!(message.payload instanceof Uint8Array)) {
        throw new Error(`worker Core operation ${message.operation} requires binary payload`);
    }
    return message.payload;
}
function requireStringPayload(message) {
    if (typeof message.payload !== "string") {
        throw new Error(`worker Core operation ${message.operation} requires string payload`);
    }
    return message.payload;
}
function installWorkerHostProxy() {
    const localHost = workerGlobal.__operitHost;
    if (localHost === undefined) {
        throw new Error("worker runtime did not install a local host bridge");
    }
    const localModules = localHost;
    const directModules = new Set(["archiveStaging", "fileSystem", "runtimeStorage", "sqlite"]);
    workerGlobal.__operitHost = new Proxy(localModules, {
        get(target, property) {
            if (typeof property !== "string") {
                return Reflect.get(target, property);
            }
            if (property === "archiveStaging") {
                return workerArchiveModule();
            }
            if (property === "runtimeStorage") {
                const storage = target[property];
                return {
                    ...storage,
                    createWriteSession(path) {
                        return requiredRuntimeStorage().createWriteSession(storageKey(runtimeStoragePrefix, path));
                    },
                    writeSessionChunk(sessionId, content) {
                        requiredRuntimeStorage().writeSessionChunk(sessionId, content);
                    },
                    commitWriteSession(sessionId) {
                        requiredRuntimeStorage().commitWriteSession(sessionId);
                    },
                    discardWriteSession(sessionId) {
                        requiredRuntimeStorage().discardWriteSession(sessionId);
                    },
                };
            }
            if (directModules.has(property)) {
                return target[property];
            }
            return mainHostModule(property);
        },
    });
}
function mainHostModule(module) {
    const cached = mainHostModules.get(module);
    if (cached !== undefined) {
        return cached;
    }
    const proxy = new Proxy({}, {
        get(_target, property) {
            if (typeof property !== "string") {
                return undefined;
            }
            return (...args) => callMainHost(module, property, args);
        },
    });
    mainHostModules.set(module, proxy);
    return proxy;
}
function callMainHost(module, method, args) {
    const id = ++nextHostCallId;
    const controlBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 2);
    const control = new Int32Array(controlBuffer);
    postWorkerMessage({
        type: "hostCall",
        id,
        module,
        method,
        args,
        control: controlBuffer,
        clientId: activeHostClientId ?? undefined,
    });
    Atomics.wait(control, controlStateIndex, 0);
    if (Atomics.load(control, controlStateIndex) !== controlReady) {
        throw new Error("main-thread host did not provide response metadata");
    }
    const payloadLength = Atomics.load(control, controlLengthIndex);
    if (payloadLength <= 0) {
        throw new Error("main-thread host returned an invalid response length");
    }
    const payloadBuffer = new SharedArrayBuffer(payloadLength);
    Atomics.store(control, controlStateIndex, 0);
    postWorkerMessage({
        type: "hostPayload",
        id,
        payload: payloadBuffer,
        control: controlBuffer,
        clientId: activeHostClientId ?? undefined,
    });
    Atomics.wait(control, controlStateIndex, 0);
    if (Atomics.load(control, controlStateIndex) !== controlPayloadReady) {
        throw new Error("main-thread host did not provide response payload");
    }
    const envelope = MessagePack.decode(new Uint8Array(payloadBuffer));
    if (!Array.isArray(envelope) || envelope.length !== 2 || typeof envelope[0] !== "number") {
        throw new Error("main-thread host response is invalid");
    }
    if (envelope[0] !== 0) {
        throw new Error(String(envelope[1]));
    }
    return envelope[1];
}
function receiveMainHostPayload(message) {
    const control = new Int32Array(message.control);
    Atomics.store(control, controlStateIndex, controlPayloadReady);
    Atomics.notify(control, controlStateIndex);
}
function workerArchiveModule() {
    return {
        createArchive(archiveId, expectedByteLength) {
            requiredArchiveStaging().create(archiveId, expectedByteLength);
        },
        appendArchive(archiveId, content) {
            requiredArchiveStaging().append(archiveId, content);
        },
        sealArchive(archiveId) {
            return requiredArchiveStaging().seal(archiveId);
        },
        readArchive(archiveId, offset, length) {
            return requiredArchiveStaging().read(archiveId, offset, length);
        },
        removeArchive(archiveId) {
            requiredArchiveStaging().remove(archiveId);
        },
    };
}
async function initializeRuntimeWorkerStorage() {
    if (runtimeStorage !== null && archiveStaging !== null) {
        return;
    }
    const activeInitialization = runtimeStorageInitialization;
    if (activeInitialization !== null) {
        return activeInitialization;
    }
    const initialization = initializeRuntimeWorkerStorageOnce();
    runtimeStorageInitialization = initialization;
    try {
        await initialization;
    }
    finally {
        if (runtimeStorageInitialization === initialization) {
            runtimeStorageInitialization = null;
        }
    }
}
async function initializeRuntimeWorkerStorageOnce() {
    const root = await runtimeOpfsRoot();
    const storage = await RuntimeWorkerStorage.open(root);
    let staging = null;
    try {
        staging = await RuntimeWorkerArchiveStaging.open(root);
        await storage.migrateLegacyEntries();
        runtimeStorage = storage;
        archiveStaging = staging;
    }
    catch (error) {
        staging?.close();
        storage.close();
        throw error;
    }
}
async function runtimeOpfsRoot() {
    const storage = navigator.storage;
    if (storage === undefined || typeof storage.getDirectory !== "function") {
        throw new Error("OPFS is unavailable for the Web runtime worker");
    }
    return (await storage.getDirectory());
}
function requiredRuntimeStorage() {
    if (runtimeStorage === null) {
        throw new Error("runtime OPFS storage is not initialized");
    }
    return runtimeStorage;
}
function requiredArchiveStaging() {
    if (archiveStaging === null) {
        throw new Error("archive OPFS staging is not initialized");
    }
    return archiveStaging;
}
function runtimeStorageBridge() {
    return {
        read(prefix, path) {
            return requiredRuntimeStorage().read(storageKey(prefix, path));
        },
        readRange(prefix, path, offset, length) {
            return requiredRuntimeStorage().readRange(storageKey(prefix, path), offset, length);
        },
        write(prefix, path, content) {
            requiredRuntimeStorage().write(storageKey(prefix, path), content);
        },
        hasFile(prefix, path) {
            return requiredRuntimeStorage().hasFile(storageKey(prefix, path));
        },
        exists(prefix, path) {
            return requiredRuntimeStorage().exists(storageKey(prefix, path));
        },
        delete(prefix, path, recursive) {
            requiredRuntimeStorage().delete(storageKey(prefix, path), recursive);
        },
        list(prefix, path) {
            return requiredRuntimeStorage().list(prefix, path);
        },
        createWriteSession(path) {
            return requiredRuntimeStorage().createWriteSession(storageKey(runtimeStoragePrefix, path));
        },
        writeSessionChunk(sessionId, content) {
            requiredRuntimeStorage().writeSessionChunk(sessionId, content);
        },
        commitWriteSession(sessionId) {
            requiredRuntimeStorage().commitWriteSession(sessionId);
        },
        discardWriteSession(sessionId) {
            requiredRuntimeStorage().discardWriteSession(sessionId);
        },
    };
}
function archiveStagingBridge() {
    return {
        createArchive(archiveId, expectedByteLength) {
            requiredArchiveStaging().create(archiveId, expectedByteLength);
        },
        appendArchive(archiveId, content) {
            requiredArchiveStaging().append(archiveId, content);
        },
        sealArchive(archiveId) {
            return requiredArchiveStaging().seal(archiveId);
        },
        readArchive(archiveId, offset, length) {
            return requiredArchiveStaging().read(archiveId, offset, length);
        },
        removeArchive(archiveId) {
            requiredArchiveStaging().remove(archiveId);
        },
    };
}
function storageKey(prefix, path) {
    const normalized = path.replaceAll("\\", "/").replace(/^\/+/, "");
    return `${prefix}${normalized}`;
}
class RuntimeWorkerStorage {
    data;
    index;
    records;
    sessions = new Map();
    migrated;
    constructor(data, index, records, migrated) {
        this.data = data;
        this.index = index;
        this.records = records;
        this.migrated = migrated;
    }
    static async open(root) {
        let data = null;
        let index = null;
        try {
            data = await openSyncAccessHandle(root, runtimeStorageDataFileName);
            index = await openSyncAccessHandle(root, runtimeStorageIndexFileName);
            const loaded = readRuntimeStorageIndex(index, data.getSize());
            return new RuntimeWorkerStorage(data, index, loaded.records, loaded.migrated);
        }
        catch (error) {
            index?.close();
            data?.close();
            throw error;
        }
    }
    close() {
        this.index.close();
        this.data.close();
    }
    async migrateLegacyEntries() {
        if (this.migrated) {
            return;
        }
        const database = await openLegacyStorageDatabase();
        await new Promise((resolve, reject) => {
            const transaction = database.transaction("entries", "readonly");
            const request = transaction.objectStore("entries").openCursor();
            request.onsuccess = () => {
                const cursor = request.result;
                if (cursor === null) {
                    return;
                }
                const itemKey = String(cursor.key);
                if (runtimeStoragePrefixes.some(prefix => itemKey.startsWith(prefix))) {
                    const targetKey = itemKey.startsWith(legacySqlitePrefix)
                        ? `${runtimeStoragePrefix}${itemKey.slice(legacySqlitePrefix.length)}`
                        : itemKey;
                    this.writeRecord(targetKey, new Uint8Array(cursor.value), false);
                }
                cursor.continue();
            };
            request.onerror = () => reject(request.error || new Error("legacy storage cursor failed"));
            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error || new Error("legacy storage read failed"));
        });
        this.migrated = true;
        this.persistIndex();
    }
    read(itemKey) {
        const record = this.records.get(itemKey);
        return record === undefined ? new Uint8Array() : readRecord(this.data, record);
    }
    readRange(itemKey, offset, length) {
        const record = this.records.get(itemKey);
        if (record === undefined) {
            throw new Error("runtime OPFS storage file does not exist");
        }
        if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 0) {
            throw new Error("runtime OPFS storage range is invalid");
        }
        if (offset > record.byteLength) {
            throw new Error("runtime OPFS storage range starts after the file");
        }
        const byteLength = Math.min(length, record.byteLength - offset);
        return readRecord(this.data, { offset: record.offset + offset, byteLength });
    }
    write(itemKey, content) {
        this.writeRecord(itemKey, content, true);
    }
    exists(itemKey) {
        if (this.records.has(itemKey)) {
            return true;
        }
        const directory = itemKey.endsWith("/") ? itemKey : `${itemKey}/`;
        return Array.from(this.records.keys()).some(key => key.startsWith(directory));
    }
    hasFile(itemKey) {
        return this.records.has(itemKey);
    }
    delete(itemKey, recursive) {
        const directory = itemKey.endsWith("/") ? itemKey : `${itemKey}/`;
        this.records.delete(itemKey);
        if (recursive) {
            for (const key of Array.from(this.records.keys())) {
                if (key.startsWith(directory)) {
                    this.records.delete(key);
                }
            }
        }
        this.persistIndex();
    }
    list(prefix, path) {
        const root = storageKey(prefix, path);
        const directory = root.endsWith(".") || root.endsWith("/") ? root : `${root}/`;
        const entries = new Map();
        for (const [itemKey, record] of this.records) {
            if (!itemKey.startsWith(directory)) {
                continue;
            }
            const remainder = itemKey.slice(directory.length);
            const separator = remainder.indexOf("/");
            if (separator < 0) {
                const path = itemKey.slice(prefix.length);
                entries.set(path, { path, isDirectory: false, size: record.byteLength });
                continue;
            }
            const path = `${directory}${remainder.slice(0, separator)}`.slice(prefix.length);
            entries.set(path, { path, isDirectory: true, size: 0 });
        }
        return Array.from(entries.values()).sort((left, right) => left.path.localeCompare(right.path));
    }
    createWriteSession(itemKey) {
        const sessionId = `runtime-write-${crypto.randomUUID()}`;
        this.sessions.set(sessionId, {
            key: itemKey,
            offset: this.data.getSize(),
            byteLength: 0,
        });
        return sessionId;
    }
    writeSessionChunk(sessionId, content) {
        const session = this.requiredSession(sessionId);
        writeExact(this.data, content, session.offset + session.byteLength);
        session.byteLength += content.byteLength;
        this.data.flush();
    }
    commitWriteSession(sessionId) {
        const session = this.requiredSession(sessionId);
        this.records.set(session.key, {
            offset: session.offset,
            byteLength: session.byteLength,
        });
        this.sessions.delete(sessionId);
        this.persistIndex();
    }
    discardWriteSession(sessionId) {
        if (!this.sessions.delete(sessionId)) {
            throw new Error("runtime storage write session does not exist");
        }
    }
    writeRecord(itemKey, content, persist) {
        const offset = this.data.getSize();
        writeExact(this.data, content, offset);
        this.data.flush();
        this.records.set(itemKey, { offset, byteLength: content.byteLength });
        if (persist) {
            this.persistIndex();
        }
    }
    requiredSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session === undefined) {
            throw new Error("runtime storage write session does not exist");
        }
        return session;
    }
    persistIndex() {
        const serialized = textEncoder.encode(JSON.stringify({
            migrated: this.migrated,
            records: Array.from(this.records.entries()),
        }));
        this.index.truncate(0);
        writeExact(this.index, serialized, 0);
        this.index.flush();
    }
}
class RuntimeWorkerArchiveStaging {
    data;
    archives = new Map();
    constructor(data) {
        this.data = data;
    }
    static async open(root) {
        const data = await openSyncAccessHandle(root, archiveStagingDataFileName);
        try {
            data.truncate(0);
            data.flush();
            return new RuntimeWorkerArchiveStaging(data);
        }
        catch (error) {
            data.close();
            throw error;
        }
    }
    close() {
        this.data.close();
    }
    create(archiveId, expectedByteLength) {
        validateArchiveId(archiveId);
        if (!Number.isSafeInteger(expectedByteLength) || expectedByteLength < 0) {
            throw new Error("archive staging byte length is invalid");
        }
        if (this.archives.has(archiveId)) {
            throw new Error("archive staging ID already exists");
        }
        const offset = this.data.getSize();
        const end = offset + expectedByteLength;
        if (!Number.isSafeInteger(end)) {
            throw new Error("archive staging capacity exceeds OPFS numeric range");
        }
        this.data.truncate(end);
        this.data.flush();
        this.archives.set(archiveId, {
            offset,
            expectedByteLength,
            byteLength: 0,
            sealed: false,
        });
    }
    append(archiveId, content) {
        const archive = this.requiredArchive(archiveId);
        if (archive.sealed) {
            throw new Error("archive staging upload is already sealed");
        }
        if (content.byteLength > archive.expectedByteLength - archive.byteLength) {
            throw new Error("archive staging upload exceeds its declared byte length");
        }
        writeExact(this.data, content, archive.offset + archive.byteLength);
        archive.byteLength += content.byteLength;
        this.data.flush();
    }
    seal(archiveId) {
        const archive = this.requiredArchive(archiveId);
        if (archive.byteLength !== archive.expectedByteLength) {
            throw new Error("archive staging upload does not match its declared byte length");
        }
        archive.sealed = true;
        this.data.flush();
        return archive.byteLength;
    }
    read(archiveId, offset, length) {
        const archive = this.requiredArchive(archiveId);
        if (!archive.sealed) {
            throw new Error("archive staging upload is not sealed");
        }
        if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 0) {
            throw new Error("archive staging range is invalid");
        }
        if (offset > archive.byteLength) {
            throw new Error("archive staging range starts after the sealed archive");
        }
        const byteLength = Math.min(length, archive.byteLength - offset);
        return readRecord(this.data, { offset: archive.offset + offset, byteLength });
    }
    remove(archiveId) {
        if (!this.archives.delete(archiveId)) {
            throw new Error("archive staging ID does not exist");
        }
        if (this.archives.size === 0) {
            this.data.truncate(0);
            this.data.flush();
        }
    }
    requiredArchive(archiveId) {
        validateArchiveId(archiveId);
        const archive = this.archives.get(archiveId);
        if (archive === undefined) {
            throw new Error("archive staging ID does not exist");
        }
        return archive;
    }
}
async function openSyncAccessHandle(root, fileName) {
    const handle = await root.getFileHandle(fileName, { create: true });
    return handle.createSyncAccessHandle();
}
function readRuntimeStorageIndex(handle, dataSize) {
    const indexSize = handle.getSize();
    if (indexSize === 0) {
        return { records: new Map(), migrated: false };
    }
    const bytes = new Uint8Array(indexSize);
    if (handle.read(bytes, { at: 0 }) !== indexSize) {
        throw new Error("runtime OPFS index could not be read completely");
    }
    const parsed = JSON.parse(textDecoder.decode(bytes));
    if (typeof parsed.migrated !== "boolean" || !Array.isArray(parsed.records)) {
        throw new Error("runtime OPFS index is invalid");
    }
    const records = new Map();
    for (const entry of parsed.records) {
        if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
            throw new Error("runtime OPFS index entry is invalid");
        }
        const record = entry[1];
        if (!isValidStorageRecord(record, dataSize)) {
            throw new Error("runtime OPFS index record is invalid");
        }
        records.set(entry[0], { offset: record.offset, byteLength: record.byteLength });
    }
    return { records, migrated: parsed.migrated };
}
function isValidStorageRecord(value, dataSize) {
    return Number.isSafeInteger(value.offset) &&
        Number.isSafeInteger(value.byteLength) &&
        value.offset >= 0 &&
        value.byteLength >= 0 &&
        value.offset + value.byteLength <= dataSize;
}
function openLegacyStorageDatabase() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open("operit2.host.storage", 2);
        request.onupgradeneeded = () => {
            if (!request.result.objectStoreNames.contains("entries")) {
                request.result.createObjectStore("entries");
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error("legacy storage open failed"));
    });
}
function readRecord(handle, record) {
    const bytes = new Uint8Array(record.byteLength);
    if (record.byteLength > 0 && handle.read(bytes, { at: record.offset }) !== record.byteLength) {
        throw new Error("OPFS data record could not be read completely");
    }
    return bytes;
}
function writeExact(handle, content, offset) {
    if (!Number.isSafeInteger(offset) || offset < 0) {
        throw new Error("OPFS write offset is invalid");
    }
    if (content.byteLength > 0 && handle.write(content, { at: offset }) !== content.byteLength) {
        throw new Error("OPFS data record could not be written completely");
    }
}
function validateArchiveId(archiveId) {
    if (!/^[A-Za-z0-9_-]+$/.test(archiveId)) {
        throw new Error("archive staging ID is invalid");
    }
}
function postWorkerMessage(message, transferables = []) {
    const post = workerGlobal.postMessage;
    post.call(workerGlobal, message, transferables);
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
