import * as api from "./api.generated.js";
import { isPromise, listen, randomUuid, unlisten, uuid } from "./core.js";

export * from "./api.generated.js";
export type * from "./core.js";
export { isPromise, listen, randomUuid, unlisten, uuid };

export default { ...api, isPromise, listen, randomUuid, unlisten, uuid };
