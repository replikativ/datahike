(ns datahike.test.backfill-online-history-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.backfill.avet :as avet]
            [datahike.db.utils :as dbu]
            [datahike.test.utils :as utils]
            [datahike.writer :as writer]))

(defn- invoke! [conn op patch]
  (let [channel (writer/dispatch! (:writer @conn) {:op op :args [patch]})
        [result selected] (async/alts!! [channel (async/timeout 10000)])]
    (when-not (= selected channel) (throw (ex-info "History test writer timed out" {:op op})))
    (when (instance? Throwable result) (throw result))
    result))

(defn- await! [f]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (if-let [result (f)] result
              (if (< (System/nanoTime) deadline)
                (do (Thread/sleep 10) (recur))
                (throw (ex-info "History test build timed out" {})))))))

(defn- rows [database tree ident]
  (let [a (dbu/attr-ref-or-ident database ident)]
    (set (map #(vec (seq %)) (filter #(= a (:a %)) (get database tree))))))

(defn- gated-build! [conn patch writes check-ready]
  (let [entered (promise) proceed (promise) build avet/build!]
    (with-redefs [avet/build! (fn [& args]
                                (deliver entered true)
                                (when (= ::timeout (deref proceed 10000 ::timeout))
                                  (throw (ex-info "History test build gate timed out" {})))
                                (apply build args))]
      (try
        (let [id (get-in (invoke! conn 'begin-avet-build! patch) [:db-after :avet-build :id])]
          (is (true? (deref entered 10000 false)))
          (let [expected (writes)]
            (deliver proceed true)
            (await! #(when (= id (get-in @conn [:avet-build-result :id]))
                       (get-in @conn [:avet-build-result :status])))
            (is (= :ready (get-in @conn [:avet-build-result :status])))
            (check-ready expected)
            (await! #(empty? (:jobs @(:state (:avet-coordinator (:writer @conn))))))))
        (finally (deliver proceed true))))))

(deftest concurrent-purge-removes-disabled-history-and-reenable-catches-new-history
  (doseq [refs? [false true]]
    (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                                :attribute-refs? refs? :keep-history? true
                                :index :datahike.index/persistent-set
                                :writer {:backend :self :writer-ownership :shared
                                         :require-fencing :process :max-batch 8}})
          config (:config @conn)]
      (try
        (d/transact conn [{:db/ident :target :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                          {:db/ident :old :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
                          {:db/id 10000 :old 1 :target 10} {:db/id 10001 :old 3}])
        (d/transact conn [[:db/add 10000 :old 2]])
        (d/transact conn [[:db/add (dbu/entid @conn :old) :db/index false]])
        (is (empty? (rows @conn :avet :old)))
        (is (seq (rows @conn :temporal-avet :old)))
        (gated-build!
         conn {:target {:db/index true}}
         (fn []
           ;; :old is not in the generation's current-index coverage. Its
           ;; retained AVET history must nevertheless receive purge removals.
           (is (not (contains? (get-in @conn [:avet-build :attrs]) :old)))
           (d/transact conn [[:db/purge 10000 :old 1]
                             [:db.purge/attribute 10001 :old]
                             [:db/retract 10000 :old 2]
                             [:db/add 10000 :target 11]])
           (rows @conn :temporal-avet :old))
         (fn [expected]
           (is (not (get-in @conn [:schema :old :db/index])))
           (is (empty? (rows @conn :avet :old)))
           (is (= expected (rows @conn :temporal-avet :old)))
           (is (seq expected))
           (is (every? #(and (= 10000 (nth % 0)) (= 2 (nth % 2))) expected))
           (is (= (rows @conn :eavt :target) (rows @conn :avet :target)))))
        (d/transact conn [[:db/add 10000 :old 4]])
        (gated-build!
         conn {:old {:db/index true}}
         (fn []
           (d/transact conn [[:db/purge 10000 :old 2]
                             [:db/add 10000 :old 5]])
           nil)
         (fn [_]
           (is (= (rows @conn :eavt :old) (rows @conn :avet :old)))
           (is (= (rows @conn :temporal-eavt :old) (rows @conn :temporal-avet :old)))
           (is (= #{5} (set (map #(nth % 2) (rows @conn :avet :old)))))
           (is (not-any? #(contains? #{1 2 3} (nth % 2)) (rows @conn :temporal-avet :old)))))
        (finally (d/release conn) (d/delete-database config))))))
