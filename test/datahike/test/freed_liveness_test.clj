(ns datahike.test.freed-liveness-test
  "A node the current db still points at must never be on the freed list.

   PSS reports a free at MUTATION time: `PersistentSortedSet.cons`/`disjoin` hand
   the old root address to `markFreed` before `store` has decided what the new
   addresses will be. `IStorage.markFreed`'s own contract says this is \"a HINT,
   not a reachability claim\" and that a consumer \"must establish for itself that
   no live version needs an address before acting on it\". `CachedStorage` did not
   — it recorded every reported address as freed.

   That is harmless under the default `squuid` addressing, where each write gets a
   fresh address and a freed one is dead forever. Under `:crypto-hash?` an address
   is a pure function of content, so any commit that ends holding content it
   started with republishes the address it just superseded.

   Measured before the fix, file store, bf 8, one entity transacted and then
   retractEntity'd:

       after seed          freed 2    reachable 39   overlap 0
       after add+retract   freed 14   reachable 39   OVERLAP 6

   And it is not benign in that configuration. `freelist-pop!` is already gated
   off for crypto-hash, but the branch online-gc takes INSTEAD when crypto-hash is
   on is `delete-freed-addresses!` (online_gc.cljc:199 and :219) — so those six
   blobs are deleted once the grace period expires, while the db still points at
   them.

   The fix is in `CachedStorage.store`: publishing an address makes it live, so it
   is removed from the freed bookkeeping. Both configurations are asserted below,
   because the squuid case is what makes the crypto-hash number meaningful."
  (:require [clojure.set :as cset]
            [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.index.persistent-set :as dips]))

(defn- config [crypto?]
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir")
                      "/dh-freed-liveness-" (java.util.UUID/randomUUID))
           :id (java.util.UUID/randomUUID)}
   :schema-flexibility :write
   :keep-history? false
   :crypto-hash? crypto?
   ;; a file store, so the indexes are actually flushed and carry addresses —
   ;; `mark` refuses an unflushed index, and an in-memory db never gets one
   :index-config {:diff-buf-size 0 :branching-factor 8}})

(defn- reachable [conn]
  (into #{} (mapcat #(dips/mark (get @conn %)) [:eavt :aevt :avet])))

(defn- freed [conn] @(:freed-set (-> @conn :store :storage)))

(defn- run [crypto?]
  (let [cfg (config crypto?)]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :a :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn (vec (for [i (range 60)] {:db/id (+ 1000 i) :a i})))
        (let [seed-overlap (count (cset/intersection (freed conn) (reachable conn)))]
          ;; add a datom and take it away again: the indexes end the pair of
          ;; commits holding exactly the content they started with, so a
          ;; content-addressed store hands back the addresses it just superseded
          (d/transact conn [{:db/id 9999 :a 12345}])
          (d/transact conn [[:db/retractEntity 9999]])
          {:seed-overlap seed-overlap
           :overlap (cset/intersection (freed conn) (reachable conn))
           :reachable (count (reachable conn))
           :datoms (count (d/datoms @conn :eavt))})
        (finally (d/release conn) (d/delete-database cfg))))))

(deftest a-live-node-is-never-left-on-the-freed-list
  (doseq [crypto? [false true]]
    (testing (str ":crypto-hash? " crypto?
                  " — an address the current db still points at must not be freed")
      (let [{:keys [seed-overlap overlap reachable datoms]} (run crypto?)]
        (is (pos? reachable)
            "precondition: the indexes are flushed and reachable addresses exist")
        (is (zero? seed-overlap)
            "precondition: the seed phase is clean, so any overlap below comes
             from the content-returning commit and not from the setup")
        (is (= 63 datoms) "precondition: the retraction really happened")
        (is (empty? overlap)
            (str ":crypto-hash? " crypto? ": " (count overlap) " of " reachable
                 " reachable addresses were marked freed. Under crypto-hash "
                 "online-gc DELETES these (online_gc.cljc:199/:219 pick that "
                 "branch precisely when crypto-hash? is on), so they are live "
                 "nodes queued for deletion."))))))
