(ns datahike.test.listen-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datahike.api :as d]
   [datahike.datom :as dd]
   [datahike.constants :as const]
   [datahike.test.utils :as du]
   [datahike.test.core-test]))

(deftest test-listen!
  (let [conn    (du/setup-db)
        reports (atom [])]
    (d/transact conn {:tx-data [[:db/add -1 :name "Alex"]
                                [:db/add -2 :name "Boris"]]})
    (d/listen conn :test #(swap! reports conj %))
    (d/transact conn {:tx-data [[:db/add -1 :name "Dima"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Evgeny"]]
                      :tx-meta {:some-metadata 1}})
    (d/transact conn {:tx-data [[:db/add -1 :name "Fedor"]
                                [:db/add 1 :name "Alex2"]         ;; should update
                                [:db/retract 2 :name "Not Boris"] ;; should be skipped
                                [:db/retract 4 :name "Evgeny"]]})
    (d/unlisten conn :test)
    (d/transact conn {:tx-data [[:db/add -1 :name "Georgy"]]})

    (is (= [(dd/datom (+ const/tx0 2) :some-metadata 1 (+ const/tx0 2) true)
            (dd/datom 3 :name "Dima"   (+ const/tx0 2) true)
            (dd/datom 3 :age 19        (+ const/tx0 2) true)
            (dd/datom 4 :name "Evgeny" (+ const/tx0 2) true)]
           (rest (:tx-data (first @reports)))))
    (is (= {:some-metadata 1}
           (dissoc (:tx-meta (first @reports)) :db/txInstant :db/commitId)))
    ;; Entity 1 already had :name "Alex"; `:name` is cardinality-one, so the
    ;; supersession retraction is reported alongside the new assertion.
    (is (= [(dd/datom 5 :name "Fedor"  (+ const/tx0 3) true)
            (dd/datom 1 :name "Alex"   (+ const/tx0 3) false)
            (dd/datom 1 :name "Alex2"  (+ const/tx0 3) true)
            (dd/datom 4 :name "Evgeny" (+ const/tx0 3) false)]
           (rest (:tx-data (second @reports)))))
    (is (= (dissoc (:tx-meta (second @reports)) :db/txInstant :db/commitId)
           {}))
    (d/release conn)))

(deftest test-listen-commits!
  (let [conn   (du/setup-db)
        events (atom [])
        key    (d/listen-commits conn :test #(swap! events conj %))
        report (d/transact conn {:tx-data [[:db/add -1 :name "durable"]]})
        event  (first @events)]
    (is (= :test key))
    (is (= 1 (count @events)))
    (is (= :datahike/commit (:type event)))
    (is (= (get-in report [:tx-meta :db/commitId]) (:commit-id event)))
    (is (= (d/commit-id @conn) (:commit-id event)))
    (is (= (d/parent-commit-ids @conn) (:parent-commit-ids event)))
    (is (= (get-in @conn [:config :store :id]) (:store-id event)))
    (is (= (get-in @conn [:config :branch]) (:branch event)))
    (is (= (:max-tx @conn) (:max-tx event)))
    (is (= 1 (:tx-count event)))
    (is (= @conn (:db-after event)))
    (is (= (:db-before report) (:db-before event)))
    (is (= [report] (:tx-reports event)))
    (is (= (:commit-id event)
           (get-in (first (:tx-reports event)) [:tx-meta :db/commitId])))
    (d/unlisten-commits conn :test)
    (d/transact conn {:tx-data [[:db/add -1 :name "not observed"]]})
    (is (= 1 (count @events)))
    (d/release conn)))

(deftest commit-listener-failure-does-not-kill-writer-or-other-listeners
  (let [conn   (du/setup-db)
        events (atom [])]
    (d/listen-commits conn :broken #(throw (ex-info "listener failed" {:event %})))
    (d/listen-commits conn :healthy #(swap! events conj %))
    (is (map? (d/transact conn {:tx-data [[:db/add -1 :name "first"]]})))
    (is (map? (d/transact conn {:tx-data [[:db/add -1 :name "second"]]})))
    (is (= 2 (count @events)))
    (d/release conn)))

#?(:clj
   (deftest durable-listener-matches-writer-batch-groups
     (let [conn (du/setup-db
                 {:writer {:backend :self
                           :writer-ownership :exclusive
                           ;; Give the transaction loop time to fill the commit
                           ;; queue so this exercises multi-transaction groups.
                           :commit-wait-time 50}})
           events (atom [])
           _ (d/listen-commits conn :batches #(swap! events conj %))
           pending (mapv (fn [n]
                           (d/transact! conn
                                        {:tx-data [[:db/add (- (inc n))
                                                    :batch/value n]]}))
                         (range 24))
           reports (mapv deref pending)
           report-groups (frequencies
                          (map #(get-in % [:tx-meta :db/commitId]) reports))
           event-groups (into {}
                              (map (juxt :commit-id :tx-count))
                              @events)]
       (is (= report-groups event-groups)
           "one event describes each exact durable commit group")
       (is (= 24 (reduce + (map :tx-count @events))))
       (is (< (count @events) 24)
           "the fixture actually exercised writer batching")
       (doseq [event @events]
         (is (= (:tx-count event) (count (:tx-reports event))))
         (is (= (:db-before (first (:tx-reports event)))
                (:db-before event)))
         (is (every? #(identical? (:db-after event) (:db-after %))
                     (:tx-reports event)))
         (is (every? #(= (:commit-id event)
                         (get-in % [:tx-meta :db/commitId]))
                     (:tx-reports event))))
       (d/release conn))))
