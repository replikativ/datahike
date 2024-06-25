(ns datahike.test.time-variance-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api :as d]
   #?(:cljs [datahike.cljs :refer [Throwable]])
   [datahike.constants :as const]
   [datahike.db.interface :as dbi]
   [datahike.test.utils :refer [setup-db sleep]])
  (:import [java.util Date]))

(set! *print-namespace-maps* false)

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/unique      :db.unique/identity
              :db/index       true
              :db/cardinality :db.cardinality/one}
             {:db/ident       :age
              :db/valueType   :db.type/long
              :db/cardinality :db.cardinality/one}
             {:name "Alice"
              :age  25}
             {:name "Bob"
              :age  35}])

(def cfg-template {:store {:backend :mem
                           :id "time-variance"}
                   :keep-history? true
                   :schema-flexibility :write
                   :initial-tx schema})

(defn now []
  (Date.))

(defn permute-and-repeat [max-repeat elements]
  (let [elements (vec elements)
        n (count elements)]
    (loop [stack [[[] (zipmap (range n) (repeat 0))]]
           result []]
      (if (empty? stack)
        result
        (let [[[fs freqs] & stack] stack]
          (recur (into stack
                       (keep (fn [[index counter]]
                               (when (< counter max-repeat)
                                 [(conj fs index) (update freqs index inc)])))
                       freqs)
                 (if (every? pos? (vals freqs))
                   (conj result (mapv #(nth elements %) fs))
                   result)))))))

(defn vary-db-ops [db & ops]
  {:post [(seq %)
          (<= (count ops) (count %))]}
  (for [fs (permute-and-repeat 2 ops)]
    ((apply comp fs) db)))

(deftest test-base-history
  (let [cfg (assoc-in cfg-template [:store :id] "test-base-history")
        conn (setup-db cfg)
        tx-id0 (:max-tx @conn)]
    (testing "Initial data"
      (is (= #{["Alice" 25] ["Bob" 35]}
             (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] @conn))))

    (testing "historical values"
      (d/transact conn [{:db/id [:name "Alice"] :age 30}])
      (are [x y]
          (= x y)
          #{[30]}
          (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] @conn [:name "Alice"])
          #{[30] [25]}
          (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] (d/history @conn) [:name "Alice"])))
    (testing "historical values after with retraction"
      (let [ ;;tx-id1 (:max-tx @conn)
            _ (d/transact conn [[:db/retractEntity [:name "Alice"]]])
            tx-id2 (:max-tx @conn)]
        (is (thrown-with-msg? Throwable #"Nothing found for entity id"
                              (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] @conn [:name "Alice"])))

        (doseq [db (vary-db-ops @conn d/history)]
          (is (= #{[30] [25]}
                 (d/q '[:find ?a :in $ ?e :where [?e :age ?a]]
                      db
                      [:name "Alice"]))))
        (doseq [db (vary-db-ops @conn d/history #(d/as-of % tx-id0))]
          (is (= #{[25]}
                 (d/q '[:find ?a :in $ ?e :where [?e :age ?a]]
                      db
                      [:name "Alice"]))))
        (doseq [db (vary-db-ops @conn d/history #(d/since % tx-id2))]
          (is (= #{[30]}
                 (d/q '[:find ?a :in $ ?e :where [?e :age ?a]]
                      db
                      [:name "Alice"]))))
        
        (testing "find retracted values"
          (doseq [db (vary-db-ops @conn d/history)]
            (is (= #{["Alice" 25] ["Alice" 30]}
                   (d/q '[:find ?n ?a :where [?r :age ?a _ false] [?r :name ?n _ false]]
                        db)))))
        (testing "find source transaction of retracted values"
          (doseq [db (vary-db-ops @conn d/history)]
            (is (= #{[25 true] [25 false] [30 true] [30 false]}
                   (d/q '[:find ?a ?op
                          :in $ ?e
                          :where
                          [?e :age ?a ?t ?op]
                          [?t :db/txInstant ?d]]
                        db
                        [:name "Alice"])))))))
    (d/release conn)))

(defn replace-commit-id [s]
  (clojure.string/replace s #"#uuid \".+\"" ":REPLACED"))

(deftest test-historical-queries
  (let [cfg (-> cfg-template
                (assoc-in [:store :id] "test-historical-queries")
                (assoc-in [:store :scope] "test.datahike.io"))
        conn (setup-db cfg)]

    (testing "get all values before specific time"
      (let [_ (d/transact conn [{:db/id [:name "Alice"] :age 30}])
            ;; sleep to make sure that transact thread has older timestamp
            _ (sleep 10)
            date (now)
            ;; sleep to make sure that transact thread has newer timestamp
            _ (sleep 10)
            _ (d/transact conn [{:db/id [:name "Alice"] :age 35}])
            history-db (d/history @conn)
            current-db @conn
            current-query '[:find ?a :in $ ?e :where [?e :age ?a]]
            query '[:find ?a
                    :in $ ?e ?fd
                    :where
                    [?e :age ?a ?tx]
                    [?tx :db/txInstant ?t]
                    [(before? ?t ?fd)]]
            query-with-< '[:find ?a
                           :in $ ?e ?fd
                           :where
                           [?e :age ?a ?tx]
                           [?tx :db/txInstant ?t]
                           [(< ?t ?fd)]]]
        (is (= #{[35]}
               (d/q current-query current-db [:name "Alice"])))
        (is (= #{[25] [30]}
               (d/q query history-db [:name "Alice"] date)))
        (is (= #{[25] [30]}
               (d/q query-with-< history-db [:name "Alice"] date)))))
    (testing "print DB"
      (is (= "#datahike/HistoricalDB {:origin #datahike/DB {:store-id [[:mem \"test.datahike.io\" \"test-historical-queries\"] :db] :commit-id :REPLACED :max-tx 536870915 :max-eid 4}}"
             (replace-commit-id (pr-str (d/history @conn))))))
    (d/release conn)))

(deftest test-as-of-db
  (let [cfg (-> cfg-template
                (assoc-in [:store :id] "test-as-of-db")
                (assoc-in [:store :scope] "test.datahike.io"))
        conn (setup-db cfg)
        first-date (now)
        ;; sleep to make sure that transact thread has newer timestamp
        _ (sleep 10)
        tx-id 536870914
        query '[:find ?a :in $ ?e :where [?e :age ?a ?tx]]]
    (testing "get values at specific time"
      (is (= #{[25]}
             (d/q query (d/as-of @conn first-date) [:name "Alice"]))))
    (testing "use transaction ID"
      (is (= #{[25]}
             (d/q query (d/as-of @conn tx-id) [:name "Alice"]))))
    (testing "print DB"
      (let [as-of-str (pr-str (d/as-of @conn tx-id))
            origin-str (pr-str (dbi/-origin (d/as-of @conn tx-id)))]
        (is (= "#datahike/AsOfDB {:origin #datahike/DB {:store-id [[:mem \"test.datahike.io\" \"test-as-of-db\"] :db] :commit-id :REPLACED :max-tx 536870913 :max-eid 4} :time-point 536870914}"
               (replace-commit-id as-of-str)))
        (is (= "#datahike/DB {:store-id [[:mem \"test.datahike.io\" \"test-as-of-db\"] :db] :commit-id :REPLACED :max-tx 536870913 :max-eid 4}"
               (replace-commit-id origin-str)))
        (is (not= as-of-str origin-str))))
    (testing "retraction"
      (let [find-alices-age '[:find ?a :in $ ?n :where [?e :name ?n] [?e :age ?a]]]
        (testing "before"
          (is (= #{[25]}
                 (d/q find-alices-age (d/as-of @conn tx-id) "Alice"))))
        (d/transact conn [[:db/retractEntity [:name "Alice"]]])
        (testing "after"
          (is (= #{}
                 (d/q find-alices-age (d/as-of @conn tx-id) "Alice"))))))
    (d/release conn)))

(deftest test-since-db
  (let [cfg (-> cfg-template
                (assoc-in [:store :id] "test-since-db")
                (assoc-in [:store :scope] "test.datahike.io"))
        conn (setup-db cfg)
        first-date (now)
        ;; sleep to make sure that transact thread has newer timestamp
        _ (sleep 10)
        tx-id 536870914
        query '[:find ?a :where [?e :age ?a]]]
    (testing "empty after first insertion"
      (is (= #{}
             (d/q query (d/since @conn first-date)))))
    (testing "added new value"
      (let [new-age 30
            _ (d/transact conn [{:db/id [:name "Alice"] :age new-age}])]
        (is (= #{[new-age]}
               (d/q query (d/since @conn first-date))))
        (is (= #{[new-age]}
               (d/q query (d/since @conn tx-id))))))
    (testing "print DB"
      (is (= "#datahike/SinceDB {:origin #datahike/DB {:store-id [[:mem \"test.datahike.io\" \"test-since-db\"] :db] :commit-id :REPLACED :max-tx 536870914 :max-eid 4} :time-point 536870914}"
             (replace-commit-id (pr-str (d/since @conn tx-id))))))
    (d/release conn)))

(deftest test-no-history
  (let [initial-tx [{:db/ident :name
                     :db/cardinality :db.cardinality/one
                     :db/valueType :db.type/string
                     :db/unique :db.unique/identity}
                    {:db/ident :age
                     :db/cardinality :db.cardinality/one
                     :db/valueType :db.type/long
                     :db/noHistory true}
                    {:name "Alice" :age 25}
                    {:name "Bob" :age 35}]
        cfg (-> cfg-template
                (assoc-in [:store :id] "test-no-history")
                (assoc :initial-tx initial-tx))
        conn (setup-db cfg)
        query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]]
    (testing "all names and ages are present in history"
      (is (= #{["Alice" 25] ["Bob" 35]}
             (d/q query (d/history @conn)))))
    (d/transact conn [[:db/retractEntity [:name "Alice"]]])
    (testing "no-history attributes are not present in history"
      (is (= #{["Bob" 35]}
             (d/q query (d/history @conn)))))
    (testing "all other attributes are present in history"
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :where [?e :name ?n]] (d/history @conn)))))
    (d/release conn)))

(deftest upsert-history
  (let [cfg {:store {:backend :mem
                     :id "test-upsert-history"}
             :keep-history? true
             :schema-flexibility :read
             :initial-tx schema}
        conn (setup-db cfg)
        query '[:find ?a ?t ?op
                :where
                [?e :name "Alice"]
                [?e :age ?a ?t ?op]]]
    (testing "add history datoms without upsert operation happening"
      (let [datoms (d/datoms (d/history @conn) {:index :aevt :components [:age [:name "Alice"]]})
            xf (map (comp vec seq))]
        (is (= [[(+ const/e0 3) :age 25 (+ const/tx0 1) true]]
               (into [] xf datoms)))))
    (testing "upsert entity"
      (d/transact conn [[:db/add [:name "Alice"] :age 30]])
      (is (= #{[30 (+ const/tx0 2) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]}
             (d/q query (d/history @conn)))))
    (testing "second upsert"
      (d/transact conn [[:db/add [:name "Alice"] :age 35]])
      (is (= #{[35 (+ const/tx0 3) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]}
             (d/q query (d/history @conn)))))
    (testing "re-insert previous value"
      (d/transact conn [[:db/add [:name "Alice"] :age 25]])
      (is (= #{[25 (+ const/tx0 4) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]
               [35 (+ const/tx0 4) false]
               [25 (+ const/tx0 4) true]}
             (d/q query (d/history @conn)))))
    (testing "retract upserted values"
      (d/transact conn [[:db/retract [:name "Alice"] :age 25]])
      (is (= #{}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]
               [35 (+ const/tx0 4) false]
               [25 (+ const/tx0 4) true]
               [25 (+ const/tx0 5) false]}
             (d/q query (d/history @conn)))))
    (testing "historical eavt datoms"
      (is (= #{[1 :db/cardinality :db.cardinality/one 536870913 true]
               [1 :db/ident :name 536870913 true]
               [1 :db/index true 536870913 true]
               [1 :db/unique :db.unique/identity 536870913 true]
               [1 :db/valueType :db.type/string 536870913 true]
               [2 :db/cardinality :db.cardinality/one 536870913 true]
               [2 :db/ident :age 536870913 true]
               [2 :db/valueType :db.type/long 536870913 true]
               [3 :age 25 536870913 true]
               [3 :age 25 536870914 false]
               [3 :age 25 536870916 true]
               [3 :age 25 536870917 false]
               [3 :age 30 536870914 true]
               [3 :age 30 536870915 false]
               [3 :age 35 536870915 true]
               [3 :age 35 536870916 false]
               [3 :name "Alice" 536870913 true]
               [4 :age 35 536870913 true]
               [4 :name "Bob" 536870913 true]}
             (->> (d/datoms (d/history @conn) {:index :eavt :components nil})
                  (map (comp vec seq))
                  (remove (fn [[e _ _ _]]
                            (< const/tx0 e)))
                  set))))
    (testing "historical aevt datoms"
      (is (= #{[3 :age 25 536870913 true]
               [3 :age 25 536870914 false]
               [3 :age 25 536870916 true]
               [3 :age 25 536870917 false]
               [3 :age 30 536870914 true]
               [3 :age 30 536870915 false]
               [3 :age 35 536870915 true]
               [3 :age 35 536870916 false]
               [4 :age 35 536870913 true]
               [3 :name "Alice" 536870913 true]
               [4 :name "Bob" 536870913 true]
               [1 :db/cardinality :db.cardinality/one 536870913 true]
               [2 :db/cardinality :db.cardinality/one 536870913 true]
               [1 :db/ident :name 536870913 true]
               [2 :db/ident :age 536870913 true]
               [1 :db/index true 536870913 true]
               [1 :db/unique :db.unique/identity 536870913 true]
               [1 :db/valueType :db.type/string 536870913 true]
               [2 :db/valueType :db.type/long 536870913 true]}
             (->> (d/datoms (d/history @conn) {:index :aevt :components nil})
                  (map (comp vec seq))
                  (remove (fn [[e _ _ _]]
                            (< const/tx0 e)))
                  set))))
    (testing "historical avet datoms"
      (is (= #{[3 :name "Alice" 536870913 true]
               [4 :name "Bob" 536870913 true]
               [2 :db/ident :age 536870913 true]
               [1 :db/ident :name 536870913 true]}
             (->> (d/datoms (d/history @conn) {:index :avet :components nil})
                  (map (comp vec seq))
                  (remove (fn [[e _ _ _]]
                            (< const/tx0 e)))
                  set))))))

(deftest test-no-duplicates-on-history-search
  (let [schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :sibling
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/ref}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]
        cfg {:store {:backend :mem :id "sandbox"}
             :keep-history? true
             :schema-flexibility :write
             :attribute-refs? false}
        conn (do
               (d/delete-database cfg)
               (d/create-database cfg)
               (d/connect cfg))]

    (d/transact conn schema)
    (d/transact conn [{:name "Alice"
                       :age  25}
                      {:name    "Charlie"
                       :age     45
                       :sibling [[:name "Alice"] [:name "Charlie"]]}])
    (is (= 1 (count (d/datoms (d/history @conn) :eavt [:name "Alice"] :name "Alice"))))
    (is (= 1 (count (filter :added (d/datoms (d/history @conn) :eavt [:name "Alice"] :name "Alice")))))

    (d/release conn)
    (d/delete-database cfg)))
;; => #'datahike.test.time-variance/test-no-duplicates-with-cardinality-many

;; https://github.com/replikativ/datahike/issues/470
(deftest test-history-record-attribute-access
  (let [cfg                                {:store              {:backend :mem}
                                            :keep-history?      true
                                            :schema-flexibility :read
                                            :attribute-refs?    false}
        conn                               (setup-db cfg)
        {{:keys [db/current-tx]} :tempids} (d/transact conn [{:name "Anne"}])
        _                                  (d/transact conn [{:name "Bernard"}])
        db                                 @conn]
    (testing "history db attributes"
      (is (= db (:origin-db (d/history db))))
      (is (= (:eavt db) (-> db d/history :origin-db :eavt))))
    (testing "as-of db attributes"
      (is (= db (:origin-db (d/as-of db current-tx))))
      (is (= current-tx (:time-point (d/as-of db current-tx))))
      (is (= (:eavt db) (-> db (d/as-of current-tx) :origin-db :eavt))))
    (testing "since db attributes"
      (is (= db (:origin-db (d/since db current-tx))))
      (is (= current-tx (:time-point (d/since db current-tx))))
      (is (= (:eavt db) (-> db (d/since current-tx) :origin-db :eavt))))
    (d/release conn)))

(deftest test-filter-current-values-of-same-transaction
  (let [keyword-cfg                                {:store              {:backend :mem}
                                                    :keep-history?      true
                                                    :schema-flexibility :write
                                                    :attribute-refs?    false}
        name-schema {:db/ident       :name
                     :db/cardinality :db.cardinality/one
                     :db/unique      :db.unique/identity
                     :db/valueType   :db.type/string}]
    (testing "cardinality one"
      (let [schema [name-schema
                    {:db/ident       :aka
                     :db/cardinality :db.cardinality/one
                     :db/valueType   :db.type/string}]
            conn (setup-db keyword-cfg)
            _ (d/transact conn schema)
            {:keys [tx-data] :as _tx-report} (d/transact conn [{:name "Michal" :aka "Tupen"}])
            michal (:e (first (filter #(= "Michal" (:v %)) tx-data)))
            {{:keys [db/current-tx]} :tempids} (d/transact conn [[:db/retract michal :aka "Tupen"]
                                                                 [:db/add michal :aka "Devil"]
                                                                 [:db/retract michal :aka "Tupen"]])
            _                                  (d/transact conn [[:db/retract michal :aka "Devil"]])
            as-of-db (d/as-of @conn current-tx)]
        (is (= {:aka "Devil"}
               (d/pull as-of-db [:aka] michal)))
        (is (= nil
               (d/pull @conn [:aka] michal)))
        (d/release conn)))
    (testing "cardinality many"
      (testing "keyword attributes"
        (let [schema [name-schema
                      {:db/ident       :aka
                       :db/cardinality :db.cardinality/many
                       :db/valueType   :db.type/string}]
              conn (setup-db keyword-cfg)
              _ (d/transact conn schema)
              {:keys [tx-data]} (d/transact conn [{:name "Michal" :aka "Tupen"}])
              michal (:e (first (filter #(= "Michal" (:v %)) tx-data)))
              {{:keys [db/current-tx]} :tempids} (d/transact conn [[:db/retract michal :aka "Tupen"]
                                                                   [:db/add michal :aka "Devil"]
                                                                   [:db/retract michal :aka "Tupen"]])
              _                                  (d/transact conn [[:db/retract michal :aka "Devil"]])
              as-of-db (d/as-of @conn current-tx)]
          (is (= {:aka ["Devil"]}
                 (d/pull as-of-db [:aka] michal)))
          (is (= nil
                 (d/pull @conn [:aka] michal)))
          (d/release conn)))

      (testing "reference attributes show all options"
        (let [schema [name-schema
                      {:db/ident       :aka
                       :db/cardinality :db.cardinality/many
                       :db/valueType   :db.type/string}]
              conn (setup-db (assoc keyword-cfg :attribute-refs? true))
              _ (d/transact conn schema)
              {tx-data                 :tx-data
               {:keys [db/current-tx]} :tempids} (d/transact conn [{:name "Michal" :aka ["Tupen" "Devil"]}])
              michal (:e (first (filter #(= "Michal" (:v %)) tx-data)))
              as-of-db (d/as-of @conn current-tx)]
          (is (= {:aka ["Devil" "Tupen"]}
                 (d/pull as-of-db [:aka] michal)))
          (d/release conn))))))

;; https://github.com/replikativ/datahike/issues/572
(deftest as-of-should-fail-on-invalid-time-points
  (let [cfg (assoc-in cfg-template [:store :id] "as-of-invalid-time-points")
        conn (setup-db cfg)]
    (is (thrown-with-msg? Throwable #"Invalid transaction ID. Must be bigger than 536870912."
                          (d/as-of @conn 42)))
    (d/release conn)))
