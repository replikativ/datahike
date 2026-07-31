(ns ^:no-doc datahike.kabel.boring-handlers
  "kabel serialization middleware carrying datahike types over boring (CBOR),
  frame **14**.

  Deliberately thin. The encoding itself lives in `datahike.boring`, in `src/`,
  because konserve's `BoringSerializer` needs exactly the same one and two
  definitions would drift — for a persisted format that means a store written by
  one side is misread by the other. This namespace only wires that codec to a
  peer.

  Compare `datahike.kabel.fressian-handlers`, which has to carry both the
  encoding and the wiring, in two platform dialects, because fressian's JVM and
  ClojureScript implementations are different libraries.

  ## Rollout

  A peer that does not know frame 14 does NOT error on it — kabel's decoding
  table returns nil and the raw payload reaches application middleware, which is
  silent corruption. So the sequence is fixed:

    1. deploy EVERY peer on `dual-read-fressian-write` — understands 14, still
       writes 13, so the wire does not change;
    2. once no peer predates step 1, switch to `dual-read-boring-write`;
    3. optionally, much later, drop to plain `datahike-boring-middleware`.

  There is no way to skip step 1. See `kabel.middleware.boring` for why the
  composition below gives dual-format reading for free."
  (:require [datahike.boring :as dboring]
            [datahike.kabel.fressian-handlers :as fh]
            [kabel.middleware.boring :as boring-mw]
            [kabel.middleware.fressian :refer [fressian]]))

;; Re-exported so a caller can register a store without caring which format is
;; in use. Both formats resolve a root's storage through the SAME
;; persistent-sorted-set registry, keyed by the store-config :id, so a store
;; registered once serves either wire.
(def register-store!   dboring/register-store!)
(def unregister-store! dboring/unregister-store!)
(def get-store         dboring/get-store)

(defn registry
  "datahike's boring registry: PSS nodes and roots plus Datom/DB/TxReport.

  Built fresh per call because a boring registry is an immutable VALUE — there
  is no process-global registry to mutate, which is the point."
  ([] (dboring/registry))
  ([opts] (dboring/registry opts)))

(defn datahike-boring-middleware
  "boring (CBOR) serialization middleware with datahike's handlers. Pass as the
  serialization middleware for kabel peers.

  Signature matches `datahike.kabel.fressian-handlers/datahike-fressian-middleware`
  so the swap is a one-word edit at each call site."
  [peer-config]
  (boring-mw/boring (atom (registry)) (atom {}) peer-config))

(defn datahike-fressian-middleware
  "Re-exported so a call site can name both middlewares from one namespace
  during the rollout."
  [peer-config]
  (fh/datahike-fressian-middleware peer-config))

;; ---------------------------------------------------------------------------
;; Dual-format composition — the rollout mechanism.
;;
;; Both middlewares guard their in-branch on the frame's serialization and pass
;; anything else through, and both short-circuit their out-branch when
;; :kabel/serialization is already set. Stacking them yields a peer that READS
;; both and WRITES whichever is outermost. No new code; the cost is two extra
;; channels and two go-loops per connection.
;; ---------------------------------------------------------------------------

(defn dual-read-fressian-write
  "Reads frames 13 and 14; writes 13. **Step 1** of the rollout — safe to deploy
  anywhere, because it does not change the wire."
  [peer-config]
  (datahike-fressian-middleware (datahike-boring-middleware peer-config)))

(defn dual-read-boring-write
  "Reads frames 13 and 14; writes 14. **Step 2** — deploy only once no peer
  predates step 1."
  [peer-config]
  (datahike-boring-middleware (datahike-fressian-middleware peer-config)))
