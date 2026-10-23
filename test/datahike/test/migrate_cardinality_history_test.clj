(ns datahike.test.migrate-cardinality-history-test
  "A cardinality change recorded in history must survive an export/import round
   trip, on both export orderings and with and without `:attribute-refs?`.

   The `:sort? false` exporter used to emit schema datoms in EAVT order, which
   orders one attribute's `:db/cardinality` history by VALUE rather than by
   transaction. A one→many change then replayed as `:many` (t3), `:one` (t1),
   retract `:one` (t3): the attribute ended with no cardinality — card-one — so
   every surviving card-many value was overwritten by the next, verification
   failed on the phantom retractions, and the target lost `:db.cardinality/many`.

   Replaying all history against the FINAL schema is inherent to `:sort? false`,
   so the schema changes for which that is wrong are refused at export time;
   those are covered at the end."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.db.interface :as dbi]
            [datahike.migrate :as m]
            [datahike.migrate.manifest :as mman]
            [datahike.test.utils :as utils]))

(defn- mem-cfg [attribute-refs?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true
   :schema-flexibility :write
   ;; `:db/index` and `:db/unique` are added to attributes already in use below
   :allow-index-backfill? true
   :attribute-refs? attribute-refs?})

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- history-triples
  "Id-independent fingerprint of the user history: `[attr-ident v op]`, with a
   ref to a user entity replaced by that entity's `:name`."
  [db]
  (let [h (d/history db)
        ident (fn [a] (if (number? a) (dbi/-ident-for h a) a))]
    (set (for [dm (d/datoms h :eavt) :when (> (:tx dm) c/tx0)]
           (let [a (ident (:a dm)) v (:v dm)]
             [a
              (cond
                ;; `:db/cardinality` is a ref under `:attribute-refs?`
                (and (number? v) (= :db/cardinality a)) (dbi/-ident-for h v)
                ;; a user ref: ids are remapped, so name the target instead
                (= :db.type/ref (get-in (d/schema db) [a :db/valueType])) [:ref (:name (d/entity db v))]
                :else v)
              (:added dm)])))))

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

(defn- tmp-dir []
  (str (System/getProperty "java.io.tmpdir") "/dh-card-hist-" (java.util.UUID/randomUUID)))

(defn- round-trip [attribute-refs? sort? txs & [import-opts]]
  (let [src (utils/setup-db (mem-cfg attribute-refs?))
        _   (doseq [tx txs] (d/transact src tx))
        dir (tmp-dir)
        _   (m/export-db @src dir {:history? true :sort? sort?})
        tgt (utils/setup-db (mem-cfg attribute-refs?))
        rep (try (m/import-db tgt dir (or import-opts {}))
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
          sort? [true false]
          batch-size [100000 1]]
    (testing (str label ", :attribute-refs? " attribute-refs? ", :sort? " sort?
                  ", :batch-size " batch-size)
      (let [{:keys [report src tgt]} (round-trip attribute-refs? sort? txs {:batch-size batch-size})]
        (is (nil? (:error report)) "import verifies (no count mismatch)")
        (is (:verified? report))
        (is (= :db.cardinality/many (:card src)))
        (is (= :db.cardinality/many (:card tgt)) "target keeps :db.cardinality/many")
        (is (= (:tags src) (:tags tgt)) "every surviving card-many value is present")
        (is (= (:history src) (:history tgt)) "history identical, no phantom retractions")
        (is (= (:count src) (:count tgt)))))))

;; ---------------------------------------------------------------------------
;; Changes a `:sort? false` export replays faithfully against the final schema

(def ^:private harmless-changes
  {"schema change in the same tx as the data using it"
   [schema
    [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
     {:name "a" :n 1}]
    [{:db/id :tag :db/doc "now documented"} {:name "a" :tag :x}]]
   ":db/doc, :db/index, :db/isComponent changed after data"
   [(conj schema {:db/ident :kid :db/valueType :db.type/ref :db/cardinality :db.cardinality/many})
    [{:name "a" :tag :x} {:name "k"}]
    [{:name "a" :kid [:name "k"]}]
    [{:db/id :tag :db/doc "tags"} {:db/id :tag :db/index true}]
    [{:db/id :kid :db/isComponent true}]
    [{:db/id :tag :db/doc "tags, revised"}]
    [{:name "a" :tag :y}]]})

(deftest harmless-changes-round-trip-unsorted
  (doseq [[label txs] harmless-changes
          attribute-refs? [false true]
          sort? [true false]
          batch-size [100000 1]]
    (testing (str label ", :attribute-refs? " attribute-refs? ", :sort? " sort?
                  ", :batch-size " batch-size)
      (let [{:keys [report src tgt]} (round-trip attribute-refs? sort? txs {:batch-size batch-size})]
        (is (nil? (:error report)))
        (is (:verified? report))
        (is (= (:tags src) (:tags tgt)))
        (is (= (:history src) (:history tgt)))
        (is (= (:count src) (:count tgt)))))))

;; ---------------------------------------------------------------------------
;; Changes it cannot: `export-db` refuses `:sort? false` before writing anything

(defn- rename-in-one-tx! [conn]
  (d/transact conn schema)
  (d/transact conn [{:name "a" :tag :x}])
  (let [aid (:db/id (d/entity @conn :tag))]
    (d/transact conn [[:db/retract aid :db/ident :tag] [:db/add aid :db/ident :label]]))
  (d/transact conn [{:name "b" :label :y}]))

(defn- ident-move-in-one-tx! [conn]
  (d/transact conn [(first schema)
                    {:db/ident :aaa :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
                    {:db/ident :bbb :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:name "a" :aaa :x :bbb :y}])
  (let [a (:db/id (d/entity @conn :aaa)) b (:db/id (d/entity @conn :bbb))]
    (d/transact conn [[:db/retract a :db/ident :aaa] [:db/add a :db/ident :aaa-old]
                      [:db/retract b :db/ident :bbb] [:db/add b :db/ident :aaa]])))

(defn- unique-after-duplicate! [conn]
  (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                    {:db/ident :k :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:db/id -1 :name "e1"} {:db/id -2 :name "e2" :k "dup"}])
  (let [e1 (d/q '[:find ?e . :where [?e :name "e1"]] @conn)
        e2 (d/q '[:find ?e . :where [?e :name "e2"]] @conn)]
    (d/transact conn [[:db/retract e2 :k "dup"]])
    (d/transact conn [[:db/add e1 :k "dup"]])
    (d/transact conn [{:db/id :k :db/unique :db.unique/value}])))

(defn- many-to-one! [conn]
  (d/transact conn (assoc-in schema [1 :db/cardinality] :db.cardinality/many))
  (d/transact conn [{:name "a" :tag [:x :y]}])
  (d/transact conn [[:db/retract [:name "a"] :tag :y]])
  (d/transact conn [{:db/id :tag :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:name "a" :tag :z}]))

(deftest unsorted-export-refuses-schema-history-it-cannot-replay
  (doseq [[label build! refs-only?] [["ident rename inside one tx" rename-in-one-tx! true]
                                     ["ident moved between attributes inside one tx" ident-move-in-one-tx! true]
                                     [":db/unique added after a historical duplicate" unique-after-duplicate! false]
                                     ["cardinality many -> one" many-to-one! false]]
          ;; a keyword-attribute database refuses renaming an attribute in use,
          ;; so those shapes only exist under :attribute-refs?
          attribute-refs? (if refs-only? [true] [false true])]
    (testing (str label ", :attribute-refs? " attribute-refs?)
      (let [src (utils/setup-db (mem-cfg attribute-refs?))]
        (try
          (build! src)
          (let [dir (tmp-dir)
                e (try (m/export-db @src dir {:history? true :sort? false}) nil
                       (catch clojure.lang.ExceptionInfo e e))]
            (is (= :export/unsorted-schema-history (:error (ex-data e))))
            (is (seq (:hazards (ex-data e))))
            (is (not (.exists (java.io.File. (str dir "/manifest.edn"))))
                "refused before anything is written"))
          (is (seq (mman/streaming-replay-hazards @src)))
          (finally (teardown src)))))))

(deftest hazards-are-empty-for-ordinary-histories
  (doseq [[label txs] (merge shapes harmless-changes)
          attribute-refs? [false true]]
    (testing label
      (let [src (utils/setup-db (mem-cfg attribute-refs?))]
        (try
          (doseq [tx txs] (d/transact src tx))
          (is (empty? (mman/streaming-replay-hazards @src)))
          (finally (teardown src)))))))
