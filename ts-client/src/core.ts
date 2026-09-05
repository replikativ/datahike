const tagSymbol: unique symbol = Symbol("datahike.tag");
const rawSymbol: unique symbol = Symbol("datahike.raw");
const peerSymbol: unique symbol = Symbol("datahike.peer");
const uuidSymbol: unique symbol = Symbol("datahike.uuid");

const resultCache = new Map<string, Promise<any>>();
let maxCacheEntries = 128;

export type Keyword = `:${string}`;
export type Attribute = Keyword;
export type UuidValue = string;

export interface DatahikeUuid {
  readonly [uuidSymbol]: true;
}

export type BranchName = Keyword;
export type VersionRef = BranchName | DatahikeUuid;

export interface StoreConfig {
  backend: Keyword;
  id?: DatahikeUuid;
  path?: string;
  [key: string]: any;
}

export interface WriterConfig {
  backend: Keyword;
  [key: string]: any;
}

export interface ConnectOptions {
  "sync?"?: boolean;
  [key: string]: any;
}

export interface DatabaseConfig {
  store: StoreConfig;
  writer?: WriterConfig;
  branch?: Keyword;
  "keep-history?"?: boolean;
  "schema-flexibility"?: ":read" | ":write";
  "initial-tx"?: Transaction[];
  name?: string;
  "remote-peer"?: RemotePeer;
  [key: string]: any;
}

export interface RemotePeer {
  backend: ":datahike-server";
  url: string;
  token?: string;
  [key: string]: any;
}

declare const connectionBrand: unique symbol;
export interface Connection {
  readonly [connectionBrand]: true;
}

declare const databaseBrand: unique symbol;
export interface Database {
  readonly [databaseBrand]: true;
}

export type EntityId = number | string | [Attribute, any];
export type EntityMap = Record<string, any>;
export type Transaction =
  | [":db/add", EntityId, Attribute, any]
  | [":db/retract", EntityId, Attribute, any]
  | [Keyword, ...any[]]
  | EntityMap;

export interface WithArgs {
  "tx-data": Transaction[];
  "tx-meta"?: any;
  "tx-options"?: { "allow-index-backfill?"?: boolean } | null;
}

export interface QueryArgs {
  query: string | any[] | Record<string, any>;
  args?: any[];
  limit?: number;
  offset?: number;
}

export interface PullOptions {
  selector: any[];
  eid: EntityId | EntityId[];
}

export type Index = ":eavt" | ":aevt" | ":avet";
export interface IndexLookupArgs {
  index: Index;
  components?: any[] | null;
}

export interface IndexRangeArgs {
  attrid: Attribute;
  start: any;
  end: any;
}

export interface Datom {
  e: number;
  a: Attribute;
  v: any;
  tx: number;
  added: boolean;
}

export interface TransactionReport {
  "db-before": Database | null;
  "db-after": Database;
  "tx-data": Datom[];
  tempids: { [key: string]: number };
  "tx-meta"?: any;
  "commit-id"?: UuidValue;
}

export interface Schema {
  [key: string]: {
    "db/valueType": Keyword;
    "db/cardinality": Keyword;
    "db/unique"?: Keyword;
    "db/index"?: boolean;
    [key: string]: any;
  };
}

export interface Metrics {
  count: number;
  "avet-count": number;
  "per-attr-counts": Record<string, number>;
  "per-entity-counts"?: Record<string, number>;
  "temporal-count"?: number;
  "temporal-avet-count"?: number;
}

export interface GcOptions {
  "min-age-ms"?: number;
}

export interface RemoteReport {
  "db-after"?: Database;
  "db-before"?: null;
  "tx-data"?: Datom[];
  tempids?: { [key: string]: number };
  "commit-id"?: UuidValue;
  resync?: boolean;
  deleted?: boolean;
  truncated?: boolean;
  error?: Error;
  status?: number;
}

type TaggedObject = Record<string, any> & {
  readonly [tagSymbol]: string;
  readonly [rawSymbol]: unknown;
  readonly [peerSymbol]?: RemotePeer;
};

type UuidObject = DatahikeUuid & { readonly value: string };

const handleTags = new Set([
  "!datahike/Connection",
  "!datahike/DB",
  "!datahike/HistoricalDB",
  "!datahike/SinceDB",
  "!datahike/AsOfDB",
  "!datahike/Entity",
  "!datahike/TxReport",
]);

function taggedObject(tag: string, raw: unknown, peer: RemotePeer | undefined): TaggedObject {
  const decoded = decodeValue(raw, peer);
  const value: Record<string, any> =
    decoded !== null && typeof decoded === "object" && !Array.isArray(decoded)
      ? { ...decoded }
      : { value: decoded };
  Object.defineProperties(value, {
    [tagSymbol]: { value: tag },
    [rawSymbol]: { value: raw },
    [peerSymbol]: { value: peer },
  });
  return value as TaggedObject;
}

function isTaggedObject(value: unknown): value is TaggedObject {
  return value !== null && typeof value === "object" && tagSymbol in value;
}

function isUuid(value: unknown): value is UuidObject {
  return value !== null && typeof value === "object" && uuidSymbol in value;
}

function decodeValue(value: unknown, peer?: RemotePeer): any {
  if (Array.isArray(value)) {
    if (value.length === 2 && typeof value[0] === "string") {
      const tag = value[0];
      const payload = value[1];
      if (handleTags.has(tag)) return taggedObject(tag, payload, peer);
      switch (tag) {
        case "!kw":
          return `:${String(payload)}`;
        case "!sym":
          return String(payload);
        case "!set":
          return Array.isArray(payload) ? payload.map((item) => decodeValue(item, peer)) : [];
        case "!uuid":
          return String(payload);
        case "!date":
          return new Date(Number(payload));
        case "!datahike/Datom": {
          const [e, a, v, tx, added] = (payload as unknown[]).map((item) => decodeValue(item, peer));
          return { e, a, v, tx, added } as Datom;
        }
      }
    }
    return value.map((item) => decodeValue(item, peer));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, decodeValue(item, peer)]),
    );
  }
  return value;
}

function encodeValue(value: unknown): unknown {
  if (isTaggedObject(value)) return [value[tagSymbol], value[rawSymbol]];
  if (isUuid(value)) return ["!uuid", value.value];
  if (value instanceof Date) return ["!date", String(value.getTime())];
  if (typeof value === "string") {
    if (value.startsWith("\\:")) return value.slice(1);
    if (value.startsWith(":")) return ["!kw", value.slice(1)];
    return value;
  }
  if (Array.isArray(value)) return value.map(encodeValue);
  if (value instanceof Set) return ["!set", Array.from(value, encodeValue)];
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key.startsWith(":") ? key.slice(1) : key,
        encodeValue(item),
      ]),
    );
  }
  return value;
}

function cacheableArguments(value: unknown): boolean {
  let database = false;
  let connection = false;
  const visit = (item: unknown): void => {
    if (!Array.isArray(item)) {
      if (item !== null && typeof item === "object") Object.values(item).forEach(visit);
      return;
    }
    if (item[0] === "!datahike/Connection") connection = true;
    if (["!datahike/DB", "!datahike/HistoricalDB", "!datahike/SinceDB", "!datahike/AsOfDB"].includes(item[0])) {
      database = true;
    } else {
      item.forEach(visit);
    }
  };
  visit(value);
  return database && !connection;
}

export function configureCache({ maxEntries }: { maxEntries: number }): void {
  if (!Number.isInteger(maxEntries) || maxEntries < 0) throw new RangeError("maxEntries must be a non-negative integer.");
  maxCacheEntries = maxEntries;
  while (resultCache.size > maxEntries) resultCache.delete(resultCache.keys().next().value!);
}

export function clearCache(): void {
  resultCache.clear();
}

function peerFromArgs(args: unknown[]): RemotePeer {
  const peers = new Set<RemotePeer>();
  const first = args[0];
  if (first !== null && typeof first === "object") {
    const peer = (first as Record<string, any>)["remote-peer"] ??
      (first as Record<string, any>)[":remote-peer"] ??
      (first as TaggedObject)[peerSymbol];
    if (peer) peers.add(peer);
  }
  for (const arg of args.slice(1)) {
    if (isTaggedObject(arg) && arg[peerSymbol]) peers.add(arg[peerSymbol]!);
  }
  if (peers.size !== 1) {
    throw new Error(peers.size === 0
      ? "No remote-peer found in arguments."
      : "Arguments refer to more than one remote-peer.");
  }
  return peers.values().next().value!;
}

function withoutCredentials(args: unknown[]): unknown[] {
  return args.map((arg) => {
    if (arg === null || typeof arg !== "object" || Array.isArray(arg) || isTaggedObject(arg)) return arg;
    const copy = { ...(arg as Record<string, unknown>) };
    for (const key of ["remote-peer", ":remote-peer", "writer", ":writer"]) {
      const value = copy[key];
      if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        const nested = { ...(value as Record<string, unknown>) };
        delete nested.token;
        delete nested[":token"];
        copy[key] = nested;
      }
    }
    return copy;
  });
}

function normalizeSetArguments(route: string, args: unknown[]): unknown[] {
  const copy = [...args];
  if ((route === "force-branch" && Array.isArray(copy[2])) ||
      (route === "merge-db" && Array.isArray(copy[1]))) {
    const index = route === "force-branch" ? 2 : 1;
    copy[index] = new Set(copy[index] as unknown[]);
  }
  return copy;
}

function base64url(bytes: Uint8Array): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  let encoded = "";
  for (let index = 0; index < bytes.length; index += 3) {
    const value = (bytes[index] << 16) |
      ((bytes[index + 1] ?? 0) << 8) |
      (bytes[index + 2] ?? 0);
    encoded += alphabet[(value >>> 18) & 63] + alphabet[(value >>> 12) & 63];
    if (index + 1 < bytes.length) encoded += alphabet[(value >>> 6) & 63];
    if (index + 2 < bytes.length) encoded += alphabet[value & 63];
  }
  return encoded;
}

async function responseValue(response: Response, target: string, peer: RemotePeer): Promise<any> {
  const text = await response.text();
  let decoded: any = null;
  if (text) {
    try {
      decoded = decodeValue(JSON.parse(text), peer);
    } catch (cause) {
      throw new Error(`Undecodable response from ${target}`, { cause });
    }
  }
  if (!response.ok) {
    const error = new Error(decoded?.msg ?? `HTTP ${response.status} from ${target}`);
    Object.assign(error, decoded?.["ex-data"] ?? {}, { status: response.status, url: target });
    throw error;
  }
  return decoded;
}

export function request(
  route: string,
  referentiallyTransparent: boolean,
  args: unknown[],
  create = false,
): Promise<any> {
  const peer = peerFromArgs(args);
  const encodedArgs = encodeValue(normalizeSetArguments(route, withoutCredentials(args)));
  const encodedString = JSON.stringify(encodedArgs);
  const encoded = new TextEncoder().encode(encodedString);
  const cacheKey = `${route}\0${encodedString}`;
  const cacheable = referentiallyTransparent && maxCacheEntries > 0 && cacheableArguments(encodedArgs);
  if (cacheable) {
    const cached = resultCache.get(cacheKey);
    if (cached) {
      resultCache.delete(cacheKey);
      resultCache.set(cacheKey, cached);
      return cached;
    }
  }
  const asGet = referentiallyTransparent && encoded.byteLength <= 2048;
  const base = `${peer.url}/${route}`;
  const target = asGet ? `${base}?args=${base64url(encoded)}&f=json` : base;
  const headers: Record<string, string> = { Accept: "application/json" };
  if (!asGet) headers["Content-Type"] = "application/json";
  if (peer.token) headers.Authorization = `token ${peer.token}`;
  const promise = fetch(target, {
    method: asGet ? "GET" : "POST",
    headers,
    ...(asGet ? {} : { body: encoded }),
  }).then((response) => responseValue(response, target, peer)).then((result) => {
    if (create && result !== null && typeof result === "object") result["remote-peer"] = peer;
    return result;
  });
  if (cacheable) {
    const cached = promise.catch((error) => {
      if (resultCache.get(cacheKey) === cached) resultCache.delete(cacheKey);
      throw error;
    });
    resultCache.set(cacheKey, cached);
    if (resultCache.size > maxCacheEntries) resultCache.delete(resultCache.keys().next().value!);
    return cached;
  }
  return promise;
}

export function uuid(value: string): DatahikeUuid {
  const out = { value } as UuidObject;
  Object.defineProperty(out, uuidSymbol, { value: true });
  return out;
}

export function randomUuid(): DatahikeUuid {
  const bytes = new Uint8Array(16);
  if (globalThis.crypto?.getRandomValues) globalThis.crypto.getRandomValues(bytes);
  else for (let index = 0; index < bytes.length; index++) bytes[index] = Math.floor(Math.random() * 256);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return uuid(`${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`);
}

export function isPromise(value: any): value is Promise<unknown> {
  return value instanceof Promise;
}

interface Listener {
  conn: Connection;
  callback: (report: RemoteReport) => void;
  lastCommit?: string;
  backoff: number;
  controller?: AbortController;
  timer?: ReturnType<typeof setTimeout>;
  stopped: boolean;
}

const listeners = new Map<string, Listener>();

function connectionParts(conn: Connection): { peer: RemotePeer; store: string; branch: string } {
  if (!isTaggedObject(conn) || conn[tagSymbol] !== "!datahike/Connection" || !conn[peerSymbol]) {
    throw new Error("listen requires a remote connection handle.");
  }
  const storeId = decodeValue(conn[rawSymbol], conn[peerSymbol]);
  const [store, branch = ":db"] = Array.isArray(storeId) ? storeId : [storeId, ":db"];
  return { peer: conn[peerSymbol]!, store: String(store), branch: String(branch).replace(/^:/, "") };
}

function listenerId(conn: Connection, key: string): string {
  const { peer, store } = connectionParts(conn);
  return `${peer.url}\0${store}\0${key}`;
}

function stopListener(id: string, listener: Listener): void {
  listener.stopped = true;
  if (listener.timer !== undefined) clearTimeout(listener.timer);
  listener.controller?.abort();
  if (listeners.get(id) === listener) listeners.delete(id);
}

function remoteDbFromHead(conn: Connection, rawData: Record<string, any>): Database {
  const { peer } = connectionParts(conn);
  const raw = {
    "store-id": [rawData["store-id"], rawData.branch],
    "commit-id": rawData["commit-id"],
    "max-eid": rawData["max-eid"],
    "max-tx": rawData["max-tx"],
  };
  return taggedObject("!datahike/DB", raw, peer) as unknown as Database;
}

function deliverEvent(id: string, listener: Listener, event: string, raw: unknown): void {
  if (listener.stopped || listeners.get(id) !== listener) return;
  const { peer } = connectionParts(listener.conn);
  const data = decodeValue(raw, peer) as Record<string, any>;
  const rawData = raw as Record<string, any>;
  if (event === "report") {
    listener.lastCommit = data["commit-id"];
    listener.callback({
      "db-after": remoteDbFromHead(listener.conn, rawData),
      "db-before": null,
      tempids: data.tempids,
      "commit-id": data["commit-id"],
      ...(Object.hasOwn(data, "tx-data") ? { "tx-data": data["tx-data"] } : {}),
      ...(data.truncated ? { truncated: true } : {}),
    });
  } else if (event === "resync" || event === "coalesced") {
    listener.lastCommit = data["commit-id"];
    listener.callback({ resync: true, "db-after": remoteDbFromHead(listener.conn, rawData) });
  } else if (event === "deleted") {
    listener.callback({ deleted: true });
    stopListener(id, listener);
  }
}

function parseFrame(id: string, listener: Listener, frame: string): void {
  let event: string | undefined;
  const data: string[] = [];
  for (const line of frame.split("\n")) {
    if (!line || line.startsWith(":")) continue;
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
  }
  if (event && data.length) deliverEvent(id, listener, event, JSON.parse(data.join("\n")));
}

async function readStream(id: string, listener: Listener, body: ReadableStream<Uint8Array>): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  while (!listener.stopped) {
    const result = await reader.read();
    if (result.done) break;
    const raw = pending + decoder.decode(result.value, { stream: true });
    const trailingCr = raw.endsWith("\r");
    const complete = trailingCr ? raw.slice(0, -1) : raw;
    const frames = complete.replace(/\r\n?/g, "\n").split("\n\n");
    pending = frames.pop()! + (trailingCr ? "\r" : "");
    for (const frame of frames) if (frame) parseFrame(id, listener, frame);
  }
  if (listener.stopped) await reader.cancel();
}

function reconnect(id: string, listener: Listener): void {
  if (listener.stopped || listeners.get(id) !== listener) return;
  const delay = listener.backoff;
  listener.backoff = Math.min(30_000, delay * 2);
  listener.timer = setTimeout(() => {
    listener.timer = undefined;
    void connectListener(id, listener);
  }, delay);
}

async function connectListener(id: string, listener: Listener): Promise<void> {
  if (listener.stopped || listeners.get(id) !== listener) return;
  const { peer, store, branch } = connectionParts(listener.conn);
  const query = new URLSearchParams({ store, branch });
  if (listener.lastCommit) query.set("since", listener.lastCommit);
  const target = `${peer.url}/listen?${query}`;
  const controller = new AbortController();
  listener.controller = controller;
  try {
    const response = await fetch(target, {
      method: "GET",
      headers: {
        Accept: "text/event-stream",
        ...(peer.token ? { Authorization: `token ${peer.token}` } : {}),
      },
      signal: controller.signal,
    });
    if (!response.ok) {
      if ([401, 403, 404].includes(response.status)) {
        const error = new Error(`HTTP ${response.status} from ${target}`);
        Object.assign(error, { status: response.status, url: target });
        listener.callback({ error, status: response.status });
        stopListener(id, listener);
        return;
      }
      throw new Error(`HTTP ${response.status} from ${target}`);
    }
    listener.backoff = 500;
    if (!response.body) throw new Error(`No response body from ${target}`);
    await readStream(id, listener, response.body);
    reconnect(id, listener);
  } catch (error) {
    if (!listener.stopped) reconnect(id, listener);
  }
}

export function listen(conn: Connection, callback: (report: RemoteReport) => void): string {
  const key = (randomUuid() as UuidObject).value;
  const id = listenerId(conn, key);
  const listener: Listener = { conn, callback, backoff: 500, stopped: false };
  listeners.set(id, listener);
  void connectListener(id, listener);
  return key;
}

export function unlisten(conn: Connection, key: string): void {
  const id = listenerId(conn, key);
  const listener = listeners.get(id);
  if (listener) stopListener(id, listener);
}
