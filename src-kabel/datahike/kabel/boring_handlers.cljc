(ns ^:no-doc datahike.kabel.boring-handlers
  "kabel serialization middleware carrying datahike types over boring (CBOR),
  frame **14**. This is datahike's wire format; there is no fressian path.

  Deliberately thin. The encoding itself lives in `datahike.boring`, in `src/`,
  because konserve's `BoringSerializer` needs exactly the same one and two
  definitions would drift — for a persisted format that means a store written by
  one side is misread by the other. This namespace only wires that codec to a
  peer.

  For contrast, the fressian module this replaced had to carry both the encoding
  and the wiring, in two platform dialects, because fressian's JVM and
  ClojureScript implementations are separate libraries with separate handler
  protocols.

  ## Compatibility

  Frame 14 is not readable by a peer built before it existed. That peer will not
  error either — kabel's decoding table returns nil for an unknown id and the
  raw payload reaches application middleware — so **both ends must be upgraded
  together**. That is a deliberate choice for datahike: the kabel wire is a live
  connection, not a persisted store, so there is no old data to keep readable,
  only old peers. `kabel.middleware.boring` still ships the dual-format
  composition for consumers who cannot upgrade both ends at once."
  (:require [datahike.boring :as dboring]
            [kabel.middleware.boring :as boring-mw]))

;; Re-exported for call-site convenience. Note these are NOT format-specific:
;; the registry belongs to persistent-sorted-set and is keyed by the
;; store-config :id, which is the `:pss/storage-id` a flushed root stamps into
;; its meta. Registering a store once serves any codec that reads PSS roots.
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
  serialization middleware for kabel peers."
  [peer-config]
  (boring-mw/boring (atom (registry)) (atom {}) peer-config))
