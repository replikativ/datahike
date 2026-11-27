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
  (d/transact conn [[:db/retract [:name "b"] :score 20]]))

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
          (is (= {:score #{["a" 21]} :tag #{["a" :x] ["a" :y]}} (current @dst)))
          (is (= (current @src) (current @dst)))
          (is (= (hist @src) (hist @dst)))
          (is (= :db.cardinality/many (get-in (d/schema @dst) [:tag :db/cardinality])))
          (finally (d/release src) (d/release dst)))))))

(deftest a-dump-that-does-not-say-its-order-is-sorted-too
  (testing "dumps written before the manifest recorded :sorted? are taken as
            unsorted"
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
