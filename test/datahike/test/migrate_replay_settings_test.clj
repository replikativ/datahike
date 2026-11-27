(ns datahike.test.migrate-replay-settings-test
  "A restore replays a dump through the transactor's migration path. That path
   has to arrive at what the source held when an attribute's storage settings
   (`:db/index`, `:db/unique`, `:db/noHistory`) changed after it had data."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.fs :as fs]))

(defn- fresh [attribute-refs?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true
             :schema-flexibility :write
             :attribute-refs? attribute-refs?
             :allow-index-backfill? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- dump-dir [] (fs/join (fs/temp-dir! "dh-replay-settings") "dump"))

(defn- avet [db attr] (mapv :v (d/datoms db {:index :avet :components [attr]})))

(defn- history-size [db] (count (d/datoms (d/history db) :eavt)))

(defn- round-trip
  "Set up a source with `setup`, export it with `export-opts`, import it with
   `import-opts`, and call `(check src-db dst-db report)`."
  [attribute-refs? export-opts import-opts setup check]
  (let [src (fresh attribute-refs?)
        dst (fresh attribute-refs?)
        dir (dump-dir)]
    (try
      (setup src)
      (m/export-db @src dir export-opts)
      (let [report (m/import-db dst dir import-opts)]
        (check @src @dst report))
      (finally (d/release src) (d/release dst)))))

(def ^:private dump-kinds
  [["history" {:history? true}]
   ["current only" {:history? false}]])

(deftest an-index-added-after-the-data-is-filled
  (doseq [attribute-refs? [false true]
          [kind export-opts] dump-kinds
          batch-size [2 5000]]
    (testing (str "attribute-refs " attribute-refs? ", " kind ", batch " batch-size)
      (testing ":db/index"
        (round-trip
         attribute-refs? export-opts {:batch-size batch-size}
         (fn [conn]
           (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
           (d/transact conn [{:score 7} {:score 11}])
           (d/transact conn [{:db/id [:db/ident :score] :db/index true}]))
         (fn [src dst report]
           (is (true? (:verified? report)))
           (is (= [7 11] (avet src :score)) "precondition: the source indexes them")
           (is (= (avet src :score) (avet dst :score)))
           (is (= 1 (count (d/q '[:find ?e :where [?e :score 7]] dst)))))))
      (testing ":db/unique"
        (round-trip
         attribute-refs? export-opts {:batch-size batch-size}
         (fn [conn]
           (d/transact conn [{:db/ident :code :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}])
           (d/transact conn [{:code "a"} {:code "b"}])
           (d/transact conn [{:db/id [:db/ident :code] :db/unique :db.unique/identity}]))
         (fn [src dst report]
           (is (true? (:verified? report)))
           (is (= (avet src :code) (avet dst :code)))
           (is (= "a" (:code (d/pull dst [:code] [:code "a"]))) "a lookup ref resolves")))))))

(defn- history-avet [db attr]
  (set (map (juxt :v :added) (d/datoms (d/history db) {:index :avet :components [attr]}))))

(deftest an-index-removed-after-the-data-is-emptied
  (doseq [attribute-refs? [false true]
          batch-size [2 5000]
          data-first? [false true]]
    (testing (str "attribute-refs " attribute-refs? ", batch " batch-size ", data first " data-first?)
      (round-trip
       attribute-refs? {:history? true} {:batch-size batch-size}
       (fn [conn]
         (when data-first?
           (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
           (d/transact conn [{:name "first"}]))
         (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/many :db/index true}])
         (if data-first?
           (d/transact conn [{:db/id [:name "first"] :score [7 11]}])
           (d/transact conn [{:db/id -1 :score [7 11]}]))
         (let [e (ffirst (d/q '[:find ?e :where [?e :score 7]] @conn))]
           (d/transact conn [[:db/retract e :score 7]
                             {:db/id [:db/ident :score] :db/index false}])))
       (fn [src dst report]
         (is (true? (:verified? report)))
         (is (= [] (avet src :score)) "precondition: the source dropped them")
         (is (= (avet src :score) (avet dst :score)))
         (is (= #{[11]} (d/q '[:find ?v :where [_ :score ?v]] dst)))
         ;; With the data entity below the attribute, a sorted dump replays
         ;; the retraction while the index is still on, as the source did. The
         ;; other way round the schema change comes first and the retraction's
         ;; historical AVET entry is not written (backlog).
         (when data-first?
           (is (= (history-avet src :score) (history-avet dst :score)))))))))

(deftest a-retraction-the-dump-carries-is-kept-in-history
  (testing "`:db/noHistory` changed in the transaction that retracts a value.
            Whether the source kept the retraction depended on the order of
            the two inside that transaction, which the dump does not record;
            the restore keeps what the dump carries. `data-first?` puts the
            data entity below or above the attribute, which decides the order
            a sorted dump replays them in."
    (doseq [attribute-refs? [false true]
            data-first? [true false]
            [label starts-no-history? tx]
            [["retract, then noHistory on" false
              (fn [e] [[:db/retract e :score 7] {:db/id [:db/ident :score] :db/noHistory true}])]
             ["noHistory on, then retract" false
              (fn [e] [{:db/id [:db/ident :score] :db/noHistory true} [:db/retract e :score 7]])]
             ["noHistory off, then retract" true
              (fn [e] [{:db/id [:db/ident :score] :db/noHistory false} [:db/retract e :score 7]])]
             ["retract, then noHistory off" true
              (fn [e] [[:db/retract e :score 7] {:db/id [:db/ident :score] :db/noHistory false}])]]]
      (testing (str "attribute-refs " attribute-refs? ", data first " data-first? ", " label)
        (round-trip
         attribute-refs? {:history? true} {}
         (fn [conn]
           (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/unique :db.unique/identity}])
           (when data-first? (d/transact conn [{:name "a"}]))
           (d/transact conn [(cond-> {:db/ident :score :db/valueType :db.type/long
                                      :db/cardinality :db.cardinality/one}
                               starts-no-history? (assoc :db/noHistory true))])
           (d/transact conn [{:name "a" :score 7}])
           (d/transact conn (tx [:name "a"])))
         (fn [src dst report]
           (is (true? (:verified? report)))
           (is (= (history-size src) (history-size dst)))
           ;; Not for "noHistory on, then retract": there the source's own
           ;; history keeps the assertion and no retraction, so no dump of it
           ;; can say the value is gone. That is the transactor's to fix.
           (when-not (= label "noHistory on, then retract")
             (is (empty? (d/q '[:find ?v :where [_ :score ?v]] dst))))))))))

(deftest uniqueness-enabled-on-duplicates-is-refused
  (testing "values written before an attribute became unique are checked when
            it does, as a live transaction checks them; an :xform that makes
            two of them equal no longer restores two owners of one value"
    (doseq [attribute-refs? [false true]]
      (let [src (fresh attribute-refs?)
            dst (fresh attribute-refs?)
            dir (dump-dir)]
        (try
          (d/transact src [{:db/ident :code :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
          (d/transact src [{:code 7} {:code 11}])
          (d/transact src [{:db/id [:db/ident :code] :db/unique :db.unique/value}])
          (m/export-db @src dir {:history? true})
          (let [r (try (m/import-db dst dir {:xform (map (fn [[e a v t op :as r]]
                                                           (if (and (= :code a) (= 11 v))
                                                             [e a 7 t op]
                                                             r)))})
                       (catch Exception e e))]
            (is (instance? Exception r) (str "got " (pr-str r))))
          (finally (d/release src) (d/release dst)))))))
