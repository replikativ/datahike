(ns ^:no-doc datahike.backfill.job
  "Durable AVET job descriptors and capability checks. No worker ownership,
   journal cursor, proof object or candidate root is persisted in a descriptor."
  (:require [clojure.set :as set]
            [datahike.db.interface :as dbi]
            [hasch.core :refer [uuid]]
            [konserve.core :as k]))

(defn supported!
  "Require a local shared writer with an explicitly requested CAS domain.
   Call at writer admission, after its authoritative head/revision reload."
  [database]
  (let [config (:config database)
        writer (:writer config)
        required (:require-fencing writer)]
    (when-not (and (= :datahike.index/persistent-set (:index config))
                   (false? (:crypto-hash? config))
                   (zero? (get (:store database) :datahike/diff-buf-size 0))
                   (= :self (get writer :backend :self))
                   (= :shared (get writer :writer-ownership :shared))
                   (contains? (set k/conditional-write-domains) required)
                   (k/conditional-write? (:store database) required)
                   (:datahike.writing/head-revision database))
      (throw (ex-info
              "Background AVET requires local shared ownership, explicit supported fencing, a revisioned head, and non-crypto PSS without diff buffers."
              {:type ::unsupported :index (:index config)
               :crypto-hash? (:crypto-hash? config)
               :writer (select-keys writer [:backend :writer-ownership :require-fencing])}))))
  nil)

(defn schema-token [database]
  (uuid [(:schema database) (:ident-ref-map database) (:ref-ident-map database)]))

(defn descriptor
  "Create one canonical durable descriptor from an admission/plan result.
   Existing effective indexing coverage includes implicit/unique attributes;
   historical-only removals are captured separately by the effect hooks."
  [database plan]
  (when (:avet-build database)
    (throw (ex-info "An AVET build is already active." {:type ::already-building})))
  {:id (random-uuid)
   :status :building
   :patch (:patch plan)
   :attrs (set/union (set (dbi/-attrs-by database :db/index)) (:attrs plan))
   :schema-token (schema-token database)
   :branch (get-in database [:config :branch])})

(defn matches?
  "Validate an active descriptor against the writer's current schema and branch.
   This is a prerequisite check, not the storage CAS or a proof of replay."
  [database descriptor]
  (and (uuid? (:id descriptor))
       (= :building (:status descriptor))
       (= (:branch descriptor) (get-in database [:config :branch]))
       (= (:schema-token descriptor) (schema-token database))))
