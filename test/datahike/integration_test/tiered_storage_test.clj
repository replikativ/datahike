(ns datahike.integration-test.tiered-storage-test
  "Regression test for the tiered-store storage-locator bug.

   A tiered store (memory frontend + durable backend) writes through to both
   tiers, but the memory frontend returns stored index roots BY REFERENCE
   (no deserialization). Those roots carried the create-database connection's
   storage handle in their `_storage` field, so a later connection flushed new
   index nodes into an orphaned pending-writes buffer that commit never drained
   — the durable backend ended up with a root referencing node blobs that were
   never written. It stayed invisible while reads hit the live frontend, and
   surfaced as \"Node not found in storage\" on any cold read of the backend
   (process restart, a second peer, or dropping the frontend).

   The fix (`datahike.index/with-storage`) detaches storage from index roots
   before they enter the store and (re)binds them to the reading connection's
   storage on materialization, so every flush targets the connection's own
   storage regardless of how `k/get` returned the value."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [konserve.store :as ks]))

(def store-id #uuid "0e7000b9-0000-0000-0000-00000e100001")

(def frontend-cfg {:backend :memory :id store-id})

(def tiered-cfg
  {:store {:backend :tiered
           :id store-id
           :frontend-config frontend-cfg
           :backend-config {:backend :file
                            :path "/tmp/datahike-tiered-storage-test"
                            :id store-id}}
   :keep-history? false
   :schema-flexibility :read
   :index :datahike.index/persistent-set})

(deftest ^:integration frontend-deletion-recovery-test
  ;; clean slate
  (d/delete-database tiered-cfg)

  (d/create-database tiered-cfg)
  (let [conn (d/connect tiered-cfg)]
    (d/transact conn [{:db/ident :name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:name "Alice"}])
    (d/release conn))

  ;; Drop the in-memory frontend — equivalent to a process restart / cold
  ;; read: subsequent reads must come from the durable backend alone.
  (ks/delete-store frontend-cfg)

  (let [conn (d/connect tiered-cfg)]
    (is (= #{["Alice"]}
           (d/q '[:find ?n :where [_ :name ?n]] @conn))
        "index nodes transacted through a tiered store must reach the durable backend")
    ;; a further transact on the recovered connection must also persist
    (d/transact conn [{:name "Bob"}])
    (d/release conn))

  (ks/delete-store frontend-cfg)

  (let [conn (d/connect tiered-cfg)]
    (is (= #{["Alice"] ["Bob"]}
           (d/q '[:find ?n :where [_ :name ?n]] @conn))
        "writes after a cold reconnect must also survive a frontend drop")
    (d/release conn))

  (d/delete-database tiered-cfg))
