(ns datahike.test.cardinality-history-test
  "History stays complete across a :db.cardinality/many → one change.

   Card-one assertions enter the temporal index when they are made; card-many
   assertions only when they are retracted. A value asserted while the attribute
   was card-many and overwritten after it became card-one used to lose its
   ASSERTION from history: only the retraction was recorded, so `d/history`
   held a retraction of a fact it never asserted, and an export/import round
   trip of such a database failed verification.

   (`as-of` across such a change is a separate matter: it assembles card-one
   groups by the CURRENT schema, so a view of the card-many era misreads it.
   Not covered here.)"
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.db.interface :as dbi]
            [datahike.test.utils :as utils]))

(defn- cfg [attribute-refs?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true
   :schema-flexibility :write
   :attribute-refs? attribute-refs?})

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- tag-history
  "`[v added]` for every `:tag` datom in history, tx-independent."
  [db]
  (let [h (d/history db)]
    (set (for [dm (d/datoms h :aevt :tag)] [(:v dm) (:added dm)]))))

(defn- unmatched-retractions
  "Retractions in history with no assertion of the same `[e a v]` at or before
   them — impossible in a correct history."
  [db]
  (let [ds (d/datoms (d/history db) :eavt)
        asserted (into #{} (comp (filter :added) (map (juxt :e :a :v))) ds)]
    (into #{} (comp (remove :added)
                    (filter #(> (:tx %) c/tx0))
                    (remove #(asserted [(:e %) (:a %) (:v %)]))
                    (map (juxt :e :a :v)))
          ds)))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}])

(deftest many-to-one-keeps-the-overwritten-assertion
  (doseq [attribute-refs? [false true]
          indexed? [false true]
          same-tx? [false true]]
    (testing (str ":attribute-refs? " attribute-refs? ", :db/index " indexed?
                  ", change and overwrite in one tx " same-tx?)
      (let [conn (utils/setup-db (cfg attribute-refs?))]
        (try
          (d/transact conn (cond-> schema indexed? (assoc-in [1 :db/index] true)))
          (d/transact conn [{:name "a" :tag [:x :y]}])
          (d/transact conn [[:db/retract [:name "a"] :tag :y]])
          (do
            (if same-tx?
              (d/transact conn [{:db/id :tag :db/cardinality :db.cardinality/one}
                                {:name "a" :tag :z}])
              (do (d/transact conn [{:db/id :tag :db/cardinality :db.cardinality/one}])
                  (d/transact conn [{:name "a" :tag :z}])))
            (is (= #{[:x true] [:x false] [:y true] [:y false] [:z true]}
                   (tag-history @conn))
                "the card-many-era assertion of :x survives its card-one overwrite")
            (is (empty? (unmatched-retractions @conn)))
            (is (= #{[:z]} (set (d/q '[:find ?t :where [_ :tag ?t]] @conn))))
            (when indexed?
              (is (= #{:x :y :z} (set (map :v (filter :added (d/datoms (d/history @conn) :avet :tag)))))
                  "temporal AVET holds the assertion too")))
          (finally (teardown conn)))))))

(deftest one-to-many-and-back-is-unaffected
  (testing "a card-one value, the change to many, and back: history unchanged in shape"
    (let [conn (utils/setup-db (cfg false))]
      (try
        (d/transact conn (assoc-in schema [1 :db/cardinality] :db.cardinality/one))
        (d/transact conn [{:name "a" :tag :x}])
        (d/transact conn [{:db/id :tag :db/cardinality :db.cardinality/many}])
        (d/transact conn [[:db/retract [:name "a"] :tag :x] {:name "a" :tag :y}])
        (d/transact conn [{:db/id :tag :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:name "a" :tag :w}])
        (is (= #{[:x true] [:x false] [:y true] [:y false] [:w true]} (tag-history @conn)))
        (is (empty? (unmatched-retractions @conn)))
        (finally (teardown conn))))))
