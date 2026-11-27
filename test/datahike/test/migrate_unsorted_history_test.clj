(ns datahike.test.migrate-unsorted-history-test
  "A history exported with `{:sort? false}` is not in replay order: the final
   schema comes first and one attribute's values follow by value, not by
   transaction. Replayed as written, a later value could be applied before an
   earlier one, and the restore lost a current value while the count still
   matched. The importer sorts such a dump before replaying it."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- fresh [attribute-refs?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true
             :schema-flexibility :write
             :attribute-refs? attribute-refs?}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- dump-dir []
  (str (Files/createTempDirectory "dh-unsorted-history" (make-array FileAttribute 0)) "/dump"))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}])

(defn- populate! [conn]
  (d/transact conn schema)
  ;; back to an earlier value
  (d/transact conn [{:name "a" :score 21}])
  (d/transact conn [{:name "a" :score 24}])
  (d/transact conn [{:name "a" :score 21}])
  ;; replaced by a smaller value
  (d/transact conn [{:name "b" :score 30}])
  (d/transact conn [{:name "b" :score 20}])
  ;; an attribute widened to many after it was used
  (d/transact conn [{:name "a" :tag :x}])
  (d/transact conn [{:db/id [:db/ident :tag] :db/cardinality :db.cardinality/many}])
  (d/transact conn [{:name "a" :tag :y}])
  ;; a value retracted and never set again
  (d/transact conn [[:db/retract [:name "b"] :score 20]])
  ;; replaced by a smaller value, which stays
  (d/transact conn [{:name "c" :score 30}])
  (d/transact conn [{:name "c" :score 20}]))

(defn- current [db]
  {:score (d/q '[:find ?n ?s :where [?e :name ?n] [?e :score ?s]] db)
   :tag   (d/q '[:find ?n ?t :where [?e :name ?n] [?e :tag ?t]] db)})

(defn- history
  "Every assertion and retraction, by the instant of its transaction."
  [db]
  (d/q '[:find ?n ?a ?v ?inst ?op
         :where [?e ?attr ?v ?tx ?op] [?attr :db/ident ?a] [(contains? #{:score :tag} ?a)]
         [?e :name ?n] [?tx :db/txInstant ?inst]]
       (d/history db)))

(defn- history-kw
  "The same without attribute refs, where a datom holds the attribute's ident."
  [db]
  (d/q '[:find ?n ?a ?v ?inst ?op
         :where [?e ?a ?v ?tx ?op] [(contains? #{:score :tag} ?a)]
         [?e :name ?n] [?tx :db/txInstant ?inst]]
       (d/history db)))

(deftest an-unsorted-history-restores-like-a-sorted-one
  (doseq [attribute-refs? [false true]
          ;; one window in memory; a window of three records spills to scratch
          sort-buffer [200000 3]]
    (testing (str "attribute-refs " attribute-refs? ", :sort-buffer " sort-buffer)
      (let [src (fresh attribute-refs?)
            dst (fresh attribute-refs?)
            dir (dump-dir)
            hist (if attribute-refs? history history-kw)]
        (try
          (populate! src)
          (m/export-db @src dir {:history? true :sort? false})
          (let [report (m/import-db dst dir {:sort-buffer sort-buffer})]
            (is (true? (:verified? report)))
            (is (zero? (:max-tx-drift report))))
          (is (= {:score #{["a" 21] ["c" 20]} :tag #{["a" :x] ["a" :y]}} (current @dst)))
          (is (= (current @src) (current @dst)))
          (is (= (hist @src) (hist @dst)))
          (is (= :db.cardinality/many (get-in (d/schema @dst) [:tag :db/cardinality])))
          (finally (d/release src) (d/release dst)))))))

(deftest a-dump-that-does-not-say-its-order-is-checked
  (testing "an unsorted dump written before the manifest recorded :sorted? is
            sorted on the way in"
    (let [src (fresh false)
          dst (fresh false)
          dir (dump-dir)]
      (try
        (populate! src)
        (m/export-db @src dir {:history? true :sort? false})
        (let [f (io/file dir "manifest.edn")
              manifest (edn/read-string {:default tagged-literal} (slurp f))]
          (is (false? (:sorted? manifest)) "an unsorted export says so")
          (spit f (pr-str (dissoc manifest :sorted?))))
        (is (true? (:verified? (m/import-db dst dir))))
        (is (= (current @src) (current @dst)))
        (finally (d/release src) (d/release dst))))))

(deftest a-sorted-export-says-so
  (let [src (fresh false)
        dir (dump-dir)]
    (try
      (populate! src)
      (m/export-db @src dir {:history? true})
      (is (true? (:sorted? (edn/read-string {:default tagged-literal}
                                            (slurp (io/file dir "manifest.edn"))))))
      (finally (d/release src)))))

(deftest schema-comes-before-the-data-of-its-transaction
  (testing "a ref attribute installed and used in one transaction, on entities
            with lower ids than the attribute: sorting by entity alone would
            replay the reference before the attribute exists as a ref, and
            leave it unmapped"
    (doseq [attribute-refs? [false true]]
      (let [src (fresh attribute-refs?)
            dst (fresh attribute-refs?)
            dir (dump-dir)]
        (try
          (d/transact src [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
          (d/transact src [{:name "child"} {:name "parent"}])
          (d/transact src [{:db/ident :parent :db/valueType :db.type/ref
                            :db/cardinality :db.cardinality/one}
                           [:db/add [:name "child"] :parent [:name "parent"]]])
          (m/export-db @src dir {:history? true :sort? false})
          (is (true? (:verified? (m/import-db dst dir {:eids (fn [e] (+ 5000 e)) :batch-size 2}))))
          (is (= #{["child" "parent"]}
                 (d/q '[:find ?c ?p :where [?e :parent ?x] [?e :name ?c] [?x :name ?p]] @dst))
              (str "attribute-refs " attribute-refs?))
          (finally (d/release src) (d/release dst)))))))

(deftest a-malformed-record-still-reaches-validation
  (testing "the sort does not judge records: one it cannot read a key from is
            handed on, and :on-error :collect reports it as before. A window of
            one record spills every record, malformed ones included."
    (let [dst (fresh false)
          t 536870913
          records [[t :db/txInstant #inst "2021-01-01" t true]
                   [1 :db/ident :field t true]
                   [1 :db/valueType :db.type/string t true]
                   [1 :db/cardinality :db.cardinality/one t true]
                   [2 :field "a"]
                   ["bad" :field "c" t true]
                   [3 :field "b" t true]]]
      (try
        (let [report (m/import-source dst (m/records->chunk-src (subvec records 0 4) 10)
                                      {:source-meta {:history? true :sorted? false}
                                       :verify? false})]
          (is (empty? (:errors report))))
        (let [report (m/import-source dst {:chunks [0]
                                           :read (fn [_ _] (subvec records 4))}
                                      {:source-meta {:history? true :sorted? false}
                                       :sort-buffer 1
                                       :on-error :collect :verify? false :merge? true})]
          (is (= [:import/malformed-record :import/malformed-record]
                 (mapv :error (:errors report))))
          (is (= #{["b"]} (d/q '[:find ?v :where [_ :field ?v]] @dst))))
        (finally (d/release dst))))))

(defn- strip-order-mark! [dir]
  (let [f (io/file dir "manifest.edn")]
    (spit f (pr-str (dissoc (edn/read-string {:default tagged-literal} (slurp f)) :sorted?)))))

(deftest an-unmarked-sorted-dump-is-replayed-as-it-was-written
  (testing "a sorted dump from before the order mark is not re-sorted: its
            transactions already arrive in order. The case: a retraction in the
            transaction that sets :db/noHistory, where the data entity has the
            lower id, which main restores correctly from the sorted dump"
    (doseq [attribute-refs? [false true]]
      (let [src (fresh attribute-refs?)
            dst (fresh attribute-refs?)
            dir (dump-dir)]
        (try
          (d/transact src [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
          (d/transact src [{:name "a"}])
          (d/transact src [{:db/ident :score :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
          (d/transact src [{:name "a" :score 7}])
          (d/transact src [[:db/retract [:name "a"] :score 7]
                           {:db/id [:db/ident :score] :db/noHistory true}])
          (m/export-db @src dir {:history? true})
          (strip-order-mark! dir)
          (is (true? (:verified? (m/import-db dst dir))))
          (is (= (count (d/datoms (d/history @src) :eavt))
                 (count (d/datoms (d/history @dst) :eavt))))
          (finally (d/release src) (d/release dst)))))))

(deftest a-rename-and-a-retraction-in-one-transaction
  (testing "the ident timeline names a record by the attribute's name at the
            end of its transaction, so the rename must be replayed before the
            retraction that uses the new name"
    (let [src (fresh true)
          dst (fresh true)
          dir (dump-dir)]
      (try
        (d/transact src [{:db/ident :score :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}])
        (d/transact src [{:db/id -1 :score 7}])
        (let [e (ffirst (d/q '[:find ?e :where [?e :score 7]] @src))]
          (d/transact src [[:db/retract e :score 7]
                           {:db/id [:db/ident :score] :db/ident :rating}]))
        (m/export-db @src dir {:history? true :sort? false})
        (is (true? (:verified? (m/import-db dst dir {:batch-size 2}))))
        (is (empty? (d/q '[:find ?v :where [_ :rating ?v]] @dst)))
        (finally (d/release src) (d/release dst))))))

(deftest a-one-transaction-unsorted-dump-is-sorted
  (testing "everything in one transaction: the unsorted exporter writes the
            schema before the :db/txInstant, which allocated an extra
            transaction id on replay"
    (let [src (fresh false)
          dst (fresh false)
          dir (dump-dir)]
      (try
        (d/transact src [{:db/ident :score :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}
                         {:db/id -1 :score 7}])
        (m/export-db @src dir {:history? true :sort? false})
        (let [report (m/import-db dst dir)]
          (is (true? (:verified? report)))
          (is (zero? (or (:max-tx-drift report) 0)) (str "drift " (:max-tx-drift report))))
        (finally (d/release src) (d/release dst))))))
