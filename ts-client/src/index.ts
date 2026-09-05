import * as api from "./api.generated.js";
import { clearCache, configureCache, isPromise, listen, randomUuid, unlisten, uuid } from "./core.js";

export * from "./api.generated.js";
export type * from "./core.js";
export { clearCache, configureCache, isPromise, listen, randomUuid, unlisten, uuid };

export default { ...api, clearCache, configureCache, isPromise, listen, randomUuid, unlisten, uuid };
