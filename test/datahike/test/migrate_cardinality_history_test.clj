(ns datahike.test.migrate-cardinality-history-test
  "A cardinality change recorded in history must survive an export/import round
   trip, on both export orderings and with and without `:attribute-refs?`.

   The `:sort? false` exporter used to emit schema datoms in EAVT order, which
   orders one attribute's `:db/cardinality` history by VALUE rather than by
   transaction. A one→many change then replayed as `:many` (t3), `:one` (t1),
   retract `:one` (t3): the attribute ended with no cardinality — card-one — so
   every surviving card-many value was overwritten by the next, verification
   failed on the phantom retractions, and the target lost `:db.cardinality/many`."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.db.interface :as dbi]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

(defn- mem-cfg [attribute-refs?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true
   :schema-flexibility :write
   :attribute-refs? attribute-refs?})

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- history-triples
  "Id-independent fingerprint of the user history: `[attr-ident v op]`. Refs to
   user entities are compared by count rather than value, so only keyword-valued
   attributes are used below."
  [db]
  (let [h (d/history db)]
    (set (for [dm (d/datoms h :eavt) :when (> (:tx dm) c/tx0)]
           [(if (number? (:a dm)) (dbi/-ident-for h (:a dm)) (:a dm))
            (let [v (:v dm)]
              ;; `:db/cardinality` is a ref under `:attribute-refs?`
              (if (and (number? v) (= :db/cardinality (if (number? (:a dm)) (dbi/-ident-for h (:a dm)) (:a dm))))
                (dbi/-ident-for h v)
                v))
            (:added dm)]))))

(defn- current-tags [db]
  (set (d/q '[:find ?n ?t :where [?e :tag ?t] [?e :name ?n]] db)))

(defn- cardinality [db]
  (let [v (:db/cardinality (d/entity db :tag))]
    (if (number? v) (dbi/-ident-for db v) v)))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}])

(def ^:private shapes
  {;; card-one overwrite, then the change, then a second value — the issue's shape
   "overwrite, change, add"
   [schema
    [{:name "a" :tag :x}]
    [{:name "a" :tag :y}]
    [{:db/id :tag :db/cardinality :db.cardinality/many}]
    [{:name "a" :tag :z}]]
   ;; a value asserted BEFORE the change survives it
   "value from the card-one era survives"
   [schema
    [{:name "a" :tag :x}]
    [{:db/id :tag :db/cardinality :db.cardinality/many}]
    [{:name "a" :tag :y}]]
   ;; the change shares its transaction with data on the attribute
   "change and data in one transaction"
   [schema
    [{:name "a" :tag :x}]
    [{:db/id :tag :db/cardinality :db.cardinality/many} {:name "b" :tag :q}]
    [{:name "a" :tag :y}]]})

(defn- round-trip [attribute-refs? sort? txs]
  (let [src (utils/setup-db (mem-cfg attribute-refs?))
        _   (doseq [tx txs] (d/transact src tx))
        dir (str (System/getProperty "java.io.tmpdir") "/dh-card-hist-" (java.util.UUID/randomUUID))
        _   (m/export-db @src dir {:history? true :sort? sort?})
        tgt (utils/setup-db (mem-cfg attribute-refs?))
        rep (try (m/import-db tgt dir {})
                 (catch clojure.lang.ExceptionInfo e {:error (ex-data e)}))]
    (try
      {:report rep
       :src {:history (history-triples @src) :tags (current-tags @src)
             :card (cardinality @src) :count (count (d/datoms (d/history @src) :eavt))}
       :tgt {:history (history-triples @tgt) :tags (current-tags @tgt)
             :card (cardinality @tgt) :count (count (d/datoms (d/history @tgt) :eavt))}}
      (finally (teardown src) (teardown tgt)))))

(deftest one-to-many-change-round-trips
  (doseq [[label txs] shapes
          attribute-refs? [false true]
          sort? [true false]]
    (testing (str label ", :attribute-refs? " attribute-refs? ", :sort? " sort?)
      (let [{:keys [report src tgt]} (round-trip attribute-refs? sort? txs)]
        (is (nil? (:error report)) "import verifies (no count mismatch)")
        (is (:verified? report))
        (is (= :db.cardinality/many (:card src)))
        (is (= :db.cardinality/many (:card tgt)) "target keeps :db.cardinality/many")
        (is (= (:tags src) (:tags tgt)) "every surviving card-many value is present")
        (is (= (:history src) (:history tgt)) "history identical, no phantom retractions")
        (is (= (:count src) (:count tgt)))))))
