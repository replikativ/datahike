(ns datahike.test.secondary-vt-routing-test
  "Verifies the routing contract of `sec/search-with-vt` /
   `sec/slice-ordered-with-vt`:

     - vt-aware index (IValidTimeAware)         → -search-at-vt (native)
     - vt-stable index (IValidTimeStable)       → -search (no filter)
     - plain/current-value index                → explicit fail-closed error

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
  (-native-valid-time? [_] true)
  (-search-at-vt [_ _qs _ef temporal-request]
    (swap! call-log conj [:-search-at-vt temporal-request])
    ;; Native fast path — pretend we filtered correctly
    bitset)

  sec/IValidTimeOrdered
  (-slice-ordered-at-vt [_ _qs _ef _attr _dir _limit temporal-request]
    (swap! call-log conj [:-slice-ordered-at-vt temporal-request])
    (mapv (fn [eid] {:entity-id eid :score 0.99})
          (es/entity-bitset-seq bitset))))

(defrecord MockDisabledVtIndex [bitset call-log]
  sec/ISecondaryIndex
  (-search [_ _query-spec _entity-filter]
    (swap! call-log conj :-search)
    bitset)
  (-estimate [_ _] (es/entity-bitset-cardinality bitset))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _qs _ef _attr _dir _limit] nil)
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IValidTimeAware
  (-native-valid-time? [_] false)
  (-search-at-vt [_ _qs _ef temporal-request]
    (swap! call-log conj [:-search-at-vt temporal-request])
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
      (is (= [[:-search-at-vt
               {:system {:mode :current}
                :valid {:mode :at :at #inst "2024-02-15"}}]]
             @log)))))

(defn- error-type [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(deftest disabled-native-valid-time-fails-closed
  (let [conn (setup-data)
        bob (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockDisabledVtIndex bitset log)
        vt-db (d/valid-at (d/history @conn) #inst "2024-02-15")]
    (is (not (sec/vt-aware? idx)))
    (is (= :secondary/temporal-view-unsupported
           (error-type #(sec/search-with-vt vt-db idx {} nil))))
    (is (= [] @log)
        "a current-value generation is not queried before rejection")))

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

(deftest plain-index-fails-closed-for-historical-values
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        ;; Mock index returns BOTH Bob and Alice unconditionally
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockBitsetIndex bitset log)]
    (testing "history requires actual value versions, not visible entity IDs"
      (let [vt-db (d/valid-at (d/history @conn) #inst "2024-02-15")]
        (is (= :secondary/temporal-view-unsupported
               (error-type #(sec/search-with-vt vt-db idx {} nil))))
        (is (= [] @log))))
    (testing "no marker → -search, full bitset returned"
      (reset! log [])
      (let [result (sec/search-with-vt @conn idx {} nil)]
        (is (= [:-search] @log))
        (is (= #{bob alice} (set (es/entity-bitset-seq result))))))))

(deftest native-valid-time-ordering-runs-before-limit
  (let [conn (setup-data)
        bob  (d/q '[:find ?e . :where [?e :emp/name "Bob"]] @conn)
        alice (d/q '[:find ?e . :where [?e :emp/name "Alice"]] @conn)
        bitset (es/entity-bitset-from-longs [bob alice])
        log (atom [])
        idx (->MockVtAwareIndex bitset log)
        vt-db (d/valid-at (d/history @conn) #inst "2024-02-15")
        result (sec/slice-ordered-with-vt vt-db idx {} nil nil nil nil)]
    (is (= [[:-slice-ordered-at-vt
             {:system {:mode :current}
              :valid {:mode :at :at #inst "2024-02-15"}}]]
           @log))
    (is (= #{bob alice} (set (map :entity-id result))))
    (is (every? #(= 0.99 (:score %)) result))))

(deftest plain-ordered-index-fails-before-limit
  (let [conn (setup-data)
        bitset (es/entity-bitset-from-longs [1 2])
        log (atom [])
        idx (->MockBitsetIndex bitset log)
        vt-db (d/valid-at (d/history @conn) #inst "2024-02-15")]
    (is (= :secondary/temporal-view-unsupported
           (error-type #(sec/slice-ordered-with-vt
                         vt-db idx {} nil nil :asc 1))))
    (is (= [] @log))))

(deftest valid-intervals-route-only-to-unordered-native-search
  (let [conn (setup-data)
        bitset (es/entity-bitset-from-longs [1])
        log (atom [])
        idx (->MockVtAwareIndex bitset log)
        between (d/valid-between (d/history @conn)
                                 #inst "2024-01-01" #inst "2024-07-01")]
    (sec/search-with-vt between idx {} nil)
    (is (= [[:-search-at-vt
             {:system {:mode :current}
              :valid {:mode :between
                      :from #inst "2024-01-01"
                      :to #inst "2024-07-01"}}]]
           @log))
    (reset! log [])
    (is (= :secondary/temporal-view-unsupported
           (error-type #(sec/slice-ordered-with-vt
                         between idx {} nil nil :asc 1))))
    (is (= [] @log))))

(deftest system-time-and-arbitrary-filter-views-fail-closed
  (let [conn (setup-data)
        idx (->MockVtStableIndex (es/entity-bitset-from-longs [1]) (atom []))]
    (doseq [view [(d/history @conn)
                  (d/as-of @conn (:max-tx @conn))
                  (d/since @conn (:max-tx @conn))
                  (d/filter @conn (constantly true))]]
      (is (= :secondary/temporal-view-unsupported
             (error-type #(sec/search-with-vt view idx {} nil)))))))
