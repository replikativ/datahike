(ns datahike.test.search-cache-test
  "Pins the WIRING of the datom search cache to the :search-cache-size config
   key.

   History that motivates this test: the cache was introduced in 2021 gated
   on `:cache-size`; #503 (Nov 2022) renamed the config key to
   `:search-cache-size` without updating the reader in db.search/memoize-for,
   so the cache was silently dead for three years — the knob stayed spec'd,
   documented and env-configurable while the gate read a key nothing set.
   No test asserted cache behavior, so nothing failed. This test makes the
   config-key ↔ reader contract executable: a future rename that breaks one
   side fails here instead of silently disabling the feature."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.db.search :as search]))

(defn- conn-with [extra-cfg]
  (let [cfg (merge {:store {:backend :memory :id (random-uuid)}
                    :schema-flexibility :write}
                   extra-cfg)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id 100 :name "a"} {:db/id 101 :name "b"}])
      conn)))

(defn- misses-for
  "Run the same -search twice through memoize-for and count how often the
   underlying lookup actually executes. 1 = cached, 2 = uncached."
  [db]
  (let [calls (atom 0)
        f (fn [] (swap! calls inc) [:datoms])
        key [:search [100 :name nil nil]]]
    (search/memoize-for db key f)
    (search/memoize-for db key f)
    @calls))

(deftest search-cache-size-controls-memoization
  (testing ":search-cache-size > 0 enables the cache"
    (let [conn (conn-with {:search-cache-size 1000})]
      (is (= 1000 (:search-cache-size (:config @conn)))
          "config carries the knob down to the DB record")
      (is (= 1 (misses-for @conn))
          "second identical search is served from the cache")))
  (testing ":search-cache-size 0 (the default) disables the cache"
    (let [conn (conn-with {:search-cache-size 0})]
      (is (= 2 (misses-for @conn))
          "every search executes the lookup")))
  (testing "the default config disables the cache"
    (let [conn (conn-with {})]
      (is (= 2 (misses-for @conn))))))

(deftest search-cache-is-scoped-to-the-snapshot
  (testing "entries do not leak across database snapshots (transact → new key)"
    (let [conn (conn-with {:search-cache-size 1000})
          db-before @conn
          before (set (map :v (d/datoms db-before {:index :eavt :components [100 :name]})))
          _ (d/transact conn [[:db/add 100 :name "changed"]])
          db-after @conn
          after (set (map :v (d/datoms db-after {:index :eavt :components [100 :name]})))]
      (is (= #{"a"} before))
      (is (= #{"changed"} after)
          "the new snapshot must not be served the old snapshot's datoms")
      (is (= #{"a"} (set (map :v (d/datoms db-before {:index :eavt :components [100 :name]}))))
          "the old snapshot keeps its own view"))))

(deftest search-cache-results-match-uncached
  (testing "cached and uncached configurations agree on query results"
    (let [c1 (conn-with {:search-cache-size 1000})
          c2 (conn-with {:search-cache-size 0})
          q '[:find ?n :where [?e :name ?n]]]
      ;; run twice on the cached conn so the second pass hits the cache
      (is (= (d/q q @c1) (d/q q @c1) (d/q q @c2) #{["a"] ["b"]})))))
