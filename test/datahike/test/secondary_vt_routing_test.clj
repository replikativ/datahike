(ns datahike.test.secondary-vt-routing-test
  "Verifies the routing contract of `sec/search-with-vt` /
   `sec/slice-ordered-with-vt`:

     - vt-aware index (IValidTimeAware)         → -search-at-vt (native)
     - vt-stable index (IValidTimeStable)       → -search (no filter)
     - plain index                              → -search + post-filter

   Uses mock indices to isolate the routing logic from any specific
   secondary's behavior."
  (:require [clojure.test :as t :refer [is deftest testing]]
            [datahike.api :as d]
            [datahike.index.secondary :as sec]
            [datahike.index.entity-set :as es]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defrecord MockBitsetIndex [bitset call-log]
  sec/ISecondaryIndex
  (-search [_ _query-spec _entity-filter]
    (swap! call-log conj :-search)
    bitset)
  (-estimate [_ _] (es/entity-bitset-cardinality bitset))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _qs _ef _attr _dir _limit]
    (swap! call-log conj :-slice-ordered)
    ;; Mimic proximum's vec-of-maps shape
    (mapv (fn [eid] {:entity-id eid :score 0.99})
          (es/entity-bitset-seq bitset)))
  (-indexed-attrs [_] #{})
  (-transact [this _] this))

(defrecord MockVtAwareIndex [bitset call-log]
  sec/ISecondaryIndex
  (-search [_ _qs _ef]
    (swap! call-log conj :-search)
    bitset)
  (-estimate [_ _] (es/entity-bitset-cardinality bitset))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _qs _ef _attr _dir _limit]
    (swap! call-log conj :-slice-ordered)
    (mapv (fn [eid] {:entity-id eid :score 0.99})
          (es/entity-bitset-seq bitset)))
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IValidTimeAware
  (-search-at-vt [_ _qs _ef _at]
    (swap! call-log conj :-search-at-vt)
    ;; Native fast path — pretend we filtered correctly
    bitset))

(defrecord MockVtStableIndex [bitset call-log]
  sec/ISecondaryIndex
  (-search [_ _qs _ef]
    (swap! call-log conj :-search)
    bitset)
  (-estimate [_ _] (es/entity-bitset-cardinality bitset))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _qs _ef _attr _dir _limit]
    (swap! call-log conj :-slice-ordered)
    (mapv (fn [eid] {:entity-id eid :score 0.99})
          (es/entity-bitset-seq bitset)))
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IValidTimeStable
  (-vt-stable? [_] true))

(defn- setup-data []
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}])
    ;; Bob is asserted with vt in [Jan, Apr)
    (d/transact conn {:tx-data [{:emp/name "Bob"}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-04-01"}})
    ;; Alice in [Apr, Jul)
    (d/transact conn {:tx-data [{:emp/name "Alice"}]
                      :tx-meta {:db.valid/from #inst "2024-04-01"
                                :db.valid/to   #inst "2024-07-01"}})
    conn))

(deftest vt-aware-index-routes-to-search-at-vt
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockVtAwareIndex bitset log)
        vt-db (d/valid-at @conn #inst "2024-02-15")]
    (testing "marker present + vt-aware → -search-at-vt"
      (sec/search-with-vt vt-db idx {} nil)
      (is (= [:-search-at-vt] @log)))))

(deftest vt-stable-index-bypasses-post-filter
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        bitset (es/entity-bitset-from-longs [bob])
        log (atom [])
        idx (->MockVtStableIndex bitset log)
        vt-db (d/valid-at @conn #inst "2024-02-15")]
    (testing "marker present + vt-stable → plain -search"
      (let [result (sec/search-with-vt vt-db idx {} nil)]
        (is (= [:-search] @log))
        (testing "result not filtered — Bob comes through even at vt outside his window"
          ;; sanity: caller passed Bob's eid, vt-stable means we trust the index
          (is (= [bob] (vec (es/entity-bitset-seq result)))))))))

(deftest plain-index-applies-post-hoc-filter
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        ;; Mock index returns BOTH Bob and Alice unconditionally
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockBitsetIndex bitset log)]
    (testing "history view + valid-at Feb-15 → only Bob (in his vt-window)"
      (let [vt-db   (d/valid-at (d/history @conn) #inst "2024-02-15")
            result  (sec/search-with-vt vt-db idx {} nil)]
        (is (= [:-search] @log))
        (testing "post-filter drops Alice (her vt is [Apr, Jul))"
          (is (= [bob] (vec (es/entity-bitset-seq result)))))))
    (testing "no marker → -search, full bitset returned"
      (reset! log [])
      (let [result (sec/search-with-vt @conn idx {} nil)]
        (is (= [:-search] @log))
        (is (= #{bob alice} (set (es/entity-bitset-seq result))))))))

(deftest post-hoc-filter-works-on-vec-of-maps
  ;; slice-ordered returns vec of {:entity-id ... :score ...}; the
  ;; post-filter must preserve the :score column for survivors.
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockBitsetIndex bitset log)
        vt-db (d/valid-at (d/history @conn) #inst "2024-02-15")
        result (sec/slice-ordered-with-vt vt-db idx {} nil nil nil nil)]
    (testing "slice-ordered + post-filter keeps survivors with their score"
      (is (= [:-slice-ordered] @log))
      (is (= 1 (count result)))
      (is (= bob (:entity-id (first result))))
      (is (= 0.99 (:score (first result))))
      (testing "Alice was dropped"
        (is (not-any? #(= alice (:entity-id %)) result))))))
