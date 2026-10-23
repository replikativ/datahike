(ns datahike.datomic.build-indexes-test
  "`import-from-datomic! {:build-indexes? true}` end to end, against `datomic:mem`.

   The bulk path needs what a Datomic source did not supply until now: an
   ident-keyed `:schema` to tell ref values from numbers, and FINAL entity ids
   before the sort. `partition-mapping` supplies the second without a
   per-entity map. The fixture has the shapes that break each: Datomic's own
   bootstrap entities, a custom partition, enum refs, a ref FROM a transaction
   to an entity, a ref TO a transaction, card-one overwrites, card-many
   retractions and a retracted entity."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.edn :as edn]
            [datomic.api :as dt]
            [datahike.api :as d]
            [datahike.migrate.datomic :as dtm]
            [datahike.constants :as const]))

(def ^:dynamic *dtm* nil)
(def ^:dynamic *facts* nil)

(defn- fixture! [uri]
  (dt/create-database uri)
  (let [c (dt/connect uri)
        tx! (fn [inst data] @(dt/transact c (into [[:db/add "datomic.tx" :db/txInstant inst]] data)))]
    (tx! #inst "2021-01-01"
         [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
           :db/unique :db.unique/identity}
          {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
          {:db/ident :p/pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :p/tag :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
          {:db/ident :p/color :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :audit/by :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :audit/tx :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/id "tenant" :db/ident :part/tenant :db.install/_partition :db.part/db}])
    (tx! #inst "2021-01-02" [{:db/ident :color/red} {:db/ident :color/blue}])
    (let [r (tx! #inst "2021-02-01"
                 [{:db/id "a" :p/name "Ann" :p/age 30 :p/tag ["x" "y"] :p/color :color/red}
                  {:db/id (dt/tempid :part/tenant) :p/name "Tia" :p/age 22 :p/pal "a"}
                  {:db/id "b" :p/name "Bob" :p/age 40}])
          ann (get-in r [:tempids "a"])
          r2 (tx! #inst "2021-03-01" [[:db/add ann :p/age 31]
                                      [:db/add "datomic.tx" :audit/by ann]])
          tx2 (dt/t->tx (dt/basis-t (:db-after r2)))]
      (tx! #inst "2021-04-01" [[:db/retract ann :p/tag "y"]
                               {:p/name "Cid" :audit/tx tx2}])
      (tx! #inst "2021-05-01" [[:db/retractEntity [:p/name "Bob"]]])
      {:conn c :audited-t (dt/tx->t tx2)})))

(use-fixtures :each
  (fn [f]
    (let [uri (str "datomic:mem://dh-bulk-" (rand-int 1000000))
          facts (fixture! uri)]
      (binding [*dtm* (:conn facts) *facts* facts]
        (try (f) (finally (dt/release *dtm*) (dt/delete-database uri)))))))

(defn- fresh []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :schema-flexibility :write
             :index :datahike.index/persistent-set}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- facts
  "What an import must get right, independent of how it numbered transactions."
  [db]
  {:people (d/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] db)
   :tags (d/q '[:find ?n ?t :where [?e :p/name ?n] [?e :p/tag ?t]] db)
   :pals (d/q '[:find ?n ?m :where [?e :p/pal ?p] [?e :p/name ?n] [?p :p/name ?m]] db)
   :colors (d/q '[:find ?n ?c :where [?e :p/color ?x] [?x :db/ident ?c] [?e :p/name ?n]] db)
   :audited-by (d/q '[:find ?n :where [?tx :audit/by ?e] [?e :p/name ?n]] db)
   ;; the ref TO a transaction lands on that transaction's entity
   :audit-tx (d/q '[:find ?n ?t :where [?e :audit/tx ?tx] [?tx :datomic/t ?t] [?e :p/name ?n]] db)
   :history (count (d/datoms (d/history db) :eavt))
   :eids (d/q '[:find ?e ?n :where [?e :p/name ?n]] (d/history db))})

(deftest the-bulk-path-imports-a-datomic-database
  (let [bulk (fresh)
        stream (fresh)
        pm (dtm/partition-mapping *dtm*)]
    (try
      (let [rb (dtm/import-from-datomic! bulk *dtm* {:build-indexes? true})
            rs (dtm/import-from-datomic! stream *dtm* {:eids pm})
            fb (facts @bulk)
            fs (facts @stream)]
        (is (true? (:verified? rb)) "the bulk import verifies against the source count")
        (is (true? (:build-indexes? rb)))
        (is (true? (:verified? rs)))
        (is (= #{["Ann" 31] ["Tia" 22]} (:people fb))
            "values, after the overwrite and the retracted entity")
        (is (= (dissoc fs :eids) (dissoc fb :eids))
            "the same values, refs and history as the streaming import")
        (is (= (:eids fs) (:eids fb)) "and the same entity ids: one mapping, two paths")
        (is (= #{["Cid" (:audited-t *facts*)]} (:audit-tx fb))
            "a ref TO a transaction names that transaction")
        (is (= #{["Ann"]} (:audited-by fb)) "a ref FROM a transaction names its entity")
        (is (every? (fn [d] (<= 0 (:e d))) (d/datoms (d/history @bulk) :eavt))
            "no negative entity: the provenance schema's -1/-2 are mapped")
        (is (< (:max-eid @bulk) const/tx0)))
      (finally (teardown bulk) (teardown stream)))))

(deftest a-ref-to-a-transaction-on-the-streaming-default
  (testing "`:allocate` (the streaming default) used to remap a tx ref as an
            entity id and allocate a phantom for it."
    (let [conn (fresh)]
      (try
        (dtm/import-from-datomic! conn *dtm*)
        (is (= #{["Cid" (:audited-t *facts*)]} (:audit-tx (facts @conn))))
        (finally (teardown conn))))))

(deftest a-partition-mapping-round-trips-through-edn
  (let [pm (dtm/partition-mapping *dtm*)
        params (dtm/mapping-params pm)
        again (dtm/params->mapping (edn/read-string (pr-str params)) *dtm*)
        ids (map :e (take 200 (dt/datoms (dt/history (dt/db *dtm*)) :eavt)))]
    (is (= params (dtm/mapping-params again)))
    (is (= (map #(get pm %) ids) (map #(get again %) ids)) "the same ids, rebuilt")
    (is (< 1 (count (:regions params))) "db, user and the custom partition")
    (let [es (->> (dt/datoms (dt/history (dt/db *dtm*)) :eavt)
                  (map :e) distinct
                  ;; transaction entities are the import's own `:tids`, not the mapping's
                  (remove #(= 3 (bit-shift-right (long %) 42))))]
      (is (apply distinct? (map #(get pm %) es)) "injective over the source's entities"))))

(deftest a-partition-mapping-refuses-what-it-was-not-built-for
  (let [pm (dtm/partition-mapping *dtm*)
        {:keys [regions]} (dtm/mapping-params pm)
        {:keys [max]} (last regions)]
    (is (= :datomic/eid-outside-mapping
           (try (get pm (inc (long max))) nil (catch Exception e (:error (ex-data e)))))
        "past a region's end")
    (is (= :datomic/eid-outside-mapping
           (try (get pm (bit-shift-left 77 42)) nil (catch Exception e (:error (ex-data e)))))
        "an unknown partition")
    (is (nil? (get pm (inc const/tx0))) "datahike tx ids are not its business")
    (is (= :datomic/partition-mapping-exceeds-tx0
           (try (dtm/partition-mapping *dtm* {:base (- const/tx0 10)}) nil
                (catch Exception e (:error (ex-data e))))))))

(deftest a-mapping-that-overlaps-the-target-is-refused
  (let [conn (fresh)]
    (try
      (is (= :datomic/partition-mapping-overlaps-target
             (try (dtm/import-from-datomic! conn *dtm*
                                            {:build-indexes? true
                                             :eids (dtm/partition-mapping *dtm* {:base 0})})
                  nil
                  (catch Exception e (:error (ex-data e))))))
      (finally (teardown conn)))))

;; ---------------------------------------------------------------------------
;; shapes the fixture above does not have, each on its own Datomic database

(defn- with-datomic [setup f]
  (let [uri (str "datomic:mem://dh-bulk-x-" (rand-int 1000000))]
    (dt/create-database uri)
    (let [c (dt/connect uri)]
      (try (f c (setup c))
           (finally (dt/release c) (dt/delete-database uri))))))

(defn- tx! [c inst data]
  @(dt/transact c (into [[:db/add "datomic.tx" :db/txInstant inst]] data)))

(defn- err [f] (try (f) nil (catch Exception e (:error (ex-data e)))))

(deftest a-fact-written-later-about-a-past-transaction
  (testing "`[:db/add old-tx :tx/note \"x\"]` lands on THAT transaction's entity, on
            every path. The bulk path used to abort (partition 3 is not an entity
            partition), and both other paths put the note on a new entity."
    (with-datomic
      (fn [c]
        (tx! c #inst "2021-01-01" [{:db/ident :p/name :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}
                                   {:db/ident :tx/note :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}])
        (let [r (tx! c #inst "2021-01-02" [{:p/name "Ann"}])
              old-tx (dt/t->tx (dt/basis-t (:db-after r)))]
          (tx! c #inst "2021-01-03" [[:db/add old-tx :tx/note "annotated later"]])
          (dt/tx->t old-tx)))
      (fn [c old-t]
        (doseq [opts [{} {:build-indexes? true} {:build-indexes? true :eids :allocate}]]
          (let [conn (fresh)]
            (try
              (let [r (dtm/import-from-datomic! conn c opts)
                    db @conn]
                (is (true? (:verified? r)) (pr-str opts))
                (is (= #{[old-t "annotated later"]}
                       (d/q '[:find ?t ?n :where [?tx :tx/note ?n] [?tx :datomic/t ?t]] db))
                    (str "the note is on the transaction that wrote Ann " (pr-str opts)))
                (is (= #{["Ann" "annotated later"]}
                       (d/q '[:find ?p ?n :where [?e :p/name ?p ?tx] [?tx :tx/note ?n]] db))
                    (pr-str opts))
                (is (< (:max-eid db) const/tx0)))
              (finally (teardown conn)))))))))

(deftest refs-to-ids-with-no-datoms-of-their-own
  (testing "A ref may name an id that never had a datom: never minted, excised, or
            in a partition nothing else uses. It needs a target id too. History
            EAVT does not see it; VAET does."
    (with-datomic
      (fn [c]
        (tx! c #inst "2021-01-01" [{:db/ident :p/name :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}
                                   {:db/ident :p/pal :db/valueType :db.type/ref
                                    :db/cardinality :db.cardinality/one}
                                   {:db/id "lone" :db/ident :part/lone
                                    :db.install/_partition :db.part/db}])
        (let [r (tx! c #inst "2021-01-02" [{:db/id "a" :p/name "Ann"}])
              ann (get-in r [:tempids "a"])
              ghost (+ (long ann) 900000)      ; same partition, never minted
              lone (dt/entid-at (:db-after r) :part/lone 7)]
          (tx! c #inst "2021-01-03" [{:p/name "Bob" :p/pal ghost}
                                     {:p/name "Cid" :p/pal lone}])
          {:ghost ghost :lone lone}))
      (fn [c {:keys [ghost lone]}]
        (let [ext (dtm/partition-extents (dt/db c))]
          (is (<= (first (ext (bit-shift-right ghost 42))) ghost
                  (second (ext (bit-shift-right ghost 42))))
              "extents include ids only VAET knows")
          (is (= [lone lone] (ext (bit-shift-right lone 42)))
              "and partitions only VAET knows"))
        (let [bulk (fresh) stream (fresh)
              pm (dtm/partition-mapping c)]
          (try
            (is (true? (:verified? (dtm/import-from-datomic! bulk c {:build-indexes? true}))))
            (is (true? (:verified? (dtm/import-from-datomic! stream c {:eids pm}))))
            (let [pals (fn [db] (d/q '[:find ?n ?p :where [?e :p/name ?n] [?e :p/pal ?p]] db))
                  pb (pals @bulk)]
              (is (= 2 (count pb)))
              (is (apply distinct? (map second pb)) "two different targets")
              (is (every? #(< 0 (second %) const/tx0) pb))
              (is (= pb (pals @stream)) "the same ids on both paths")
              (is (= #{(get pm ghost) (get pm lone)} (set (map second pb)))))
            (finally (teardown bulk) (teardown stream))))))))

(defn- distinct-source-ids
  "Every non-transaction id in history EAVT (entities) or VAET (ref targets)."
  [db]
  (let [h (dt/history db)
        tx? #(= 3 (bit-shift-right (long %) 42))]
    (into #{} (remove tx?)
          (concat (map :e (dt/datoms h :eavt)) (map :v (dt/datoms h :vaet))))))

(deftest interleaved-partitions-are-packed-exactly
  (testing "Datomic's id counter is shared by every partition and by transactions,
            so a partition's ids spread over everything minted while it was in
            use. Three partitions written alternately each span about three times
            their entity count; a [min max] layout needed ~3x the ids. The rank
            layout needs exactly one per id."
    (with-datomic
      (fn [c]
        (tx! c #inst "2021-01-01" [{:db/ident :p/name :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}
                                   {:db/id "x" :db/ident :part/x :db.install/_partition :db.part/db}
                                   {:db/id "y" :db/ident :part/y :db.install/_partition :db.part/db}])
        (doseq [i (range 300)]
          (tx! c (java.util.Date. (+ 1609545600000 (* 1000 i)))
               [{:db/id (dt/tempid :db.part/user) :p/name (str "u" i)}
                {:db/id (dt/tempid :part/x) :p/name (str "x" i)}
                {:db/id (dt/tempid :part/y) :p/name (str "y" i)}]))
        nil)
      (fn [c _]
        (let [pm (dtm/partition-mapping c)
              {:keys [base reserved end regions]} (dtm/mapping-params pm)
              ids (distinct-source-ids (dt/db c))
              used (- end base reserved)
              user-regions (filter #(< 0 (:partition %)) regions)
              naive (transduce (map #(inc (- (:max %) (:min %)))) + 0 regions)]
          (is (= (count ids) used) "the id space is exactly the distinct id count")
          (is (= 3 (count user-regions)))
          (is (every? #(= 300 (:count %)) user-regions))
          (is (> naive (* 2.5 used)) "what [min max] regions would have needed")
          (is (= (count ids) (count (set (map #(get pm %) ids)))) "injective")
          (is (every? #(< (dec (+ base reserved)) (get pm %) end) ids) "inside the layout")
          (is (pos? (dtm/mapping-bytes pm)) "interleaved partitions are not ranges")
          (let [conn (fresh)]
            (try
              (is (true? (:verified? (dtm/import-from-datomic! conn c {:build-indexes? true}))))
              (is (= 900 (count (d/q '[:find ?e :where [?e :p/name]] @conn))))
              (is (< (:max-eid @conn) end))
              (finally (teardown conn)))))))))

(deftest a-stale-saved-mapping-is-refused-before-anything-is-written
  (let [params (dtm/mapping-params (dtm/partition-mapping *dtm*))
        saved-t (:basis-t params)]
    (tx! *dtm* #inst "2021-06-01" [{:p/name "Dee" :p/age 50}])
    (let [conn (fresh)
          pm (dtm/params->mapping (edn/read-string (pr-str params)) *dtm*)]
      (try
        (is (= :datomic/partition-mapping-stale
               (err #(dtm/import-from-datomic! conn *dtm* {:build-indexes? true :eids pm}))))
        (is (= 0 (count (d/q '[:find ?e :where [?e :p/name]] @conn))) "nothing written")
        (testing "importing as of the mapping's basis is what it describes"
          (let [r (dtm/import-from-datomic! conn *dtm* {:build-indexes? true :eids pm
                                                        :basis-t saved-t})]
            (is (true? (:verified? r)))
            (is (= #{["Ann" 31] ["Tia" 22]}
                   (d/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] @conn))
                "Dee came later")))
        (finally (teardown conn))))
    (testing "a mapping rebuilt against another database does not match"
      (with-datomic
        (fn [c] (tx! c #inst "2021-01-01" [{:db/ident :q/x :db/valueType :db.type/long
                                            :db/cardinality :db.cardinality/one}])
          nil)
        (fn [c _]
          (is (contains? #{:datomic/partition-mapping-mismatch :datomic/basis-t-ahead}
                         (err #(dtm/params->mapping params c)))))))))

(deftest a-merge-into-a-populated-target-with-a-mapping-above-it
  (let [conn (fresh)]
    (try
      (d/transact conn [{:db/ident :local/x :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:local/x 1} {:local/x 2}])
      (let [floor (inc (:max-eid @conn))
            pm (dtm/partition-mapping *dtm* {:base floor})
            r (dtm/import-from-datomic! conn *dtm* {:eids pm :merge? true})
            db @conn]
        (is (true? (:verified? r)))
        (is (= #{[1] [2]} (d/q '[:find ?x :where [_ :local/x ?x]] db)) "local data intact")
        (is (= #{["Ann" 31] ["Tia" 22]} (d/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] db)))
        (is (every? (fn [[e]] (<= floor e)) (d/q '[:find ?e :where [?e :p/name]] (d/history db)))
            "imported entities sit above what was there"))
      (finally (teardown conn)))))

(deftest an-explicit-nil-schema-is-refused-on-the-bulk-path
  (let [conn (fresh)]
    (try
      (is (= :datomic/schema-nil
             (err #(dtm/import-from-datomic! conn *dtm* {:build-indexes? true :schema nil}))))
      (finally (teardown conn)))))

(deftest attribute-schema-carries-tuple-declarations
  (testing "so a tuple's ref elements can be found by an importer that remaps them"
    (with-datomic
      (fn [c]
        (tx! c #inst "2021-01-01" [{:db/ident :t/a :db/valueType :db.type/ref
                                    :db/cardinality :db.cardinality/one}
                                   {:db/ident :t/b :db/valueType :db.type/long
                                    :db/cardinality :db.cardinality/one}
                                   {:db/ident :t/ab :db/valueType :db.type/tuple
                                    :db/tupleAttrs [:t/a :t/b] :db/cardinality :db.cardinality/one}
                                   {:db/ident :t/het :db/valueType :db.type/tuple
                                    :db/tupleTypes [:db.type/ref :db.type/long]
                                    :db/cardinality :db.cardinality/one}])
        nil)
      (fn [c _]
        (let [s (dtm/attribute-schema (dt/db c))]
          (is (= [:t/a :t/b] (get-in s [:t/ab :db/tupleAttrs])))
          (is (= [:db.type/ref :db.type/long] (get-in s [:t/het :db/tupleTypes])))
          (is (= :db.type/ref (get-in s [:t/a :db/valueType]))))))))

(deftest region-encodings-rank-exactly
  (let [rnd (java.util.Random. 7)
        check (fn [ids]
                (let [ids (vec (distinct (sort ids)))
                      r (dtm/build-region ids)
                      rank #(.rank ^datahike.migrate.datomic.IRank (:region r) (long %))
                      s (set ids)]
                  (is (= (range (count ids)) (map rank ids)) (str (:kind r)))
                  ;; misses: every gap neighbour, and either side of the set
                  (is (every? #(= -1 (rank %))
                              (remove s (mapcat (fn [x] [(- x 2) (dec x) (inc x) (+ x 2)]) ids))))
                  (:kind r)))]
    (is (= :range (check (range 100 1000))))
    (is (= :bitmap (check (filter (fn [_] (< (.nextDouble rnd) 0.5)) (range 5000 200000)))))
    (is (= :table (check (filter (fn [_] (< (.nextDouble rnd) 0.01)) (range 5000 2000000)))))
    (is (= :table (check (concat (range 0 64) [(+ 64 (bit-shift-left 1 40))]))))
    (is (= :range (check [5])))))

(deftest a-mapping-that-does-not-cover-the-source-is-refused-before-the-scan
  (testing "bounds are checked by index seeks before the count scan and before
            anything is written"
    (let [{:keys [base reserved regions] :as params} (dtm/mapping-params (dtm/partition-mapping *dtm*))
          first-id (+ base reserved)
          narrow (assoc params
                        :regions (vec (map-indexed (fn [i {:keys [partition min]}]
                                                     {:partition partition :kind :range
                                                      :min min :max min :count 1
                                                      :start (+ first-id i) :bytes 0 :digest 0})
                                                   regions))
                        :end (+ first-id (count regions)))
          pm (dtm/params->mapping narrow)
          conn (fresh)]
      (try
        (is (= :datomic/eid-outside-mapping
               (err #(dtm/import-from-datomic! conn *dtm* {:build-indexes? true :eids pm}))))
        (is (= 0 (count (d/q '[:find ?e :where [?e :p/name]] @conn))))
        (finally (teardown conn))))))

(deftest invalid-mapping-params-are-refused
  (let [{:keys [regions] :as params} (dtm/mapping-params (dtm/partition-mapping *dtm*))]
    (is (= :datomic/partition-mapping-invalid
           (err #(dtm/params->mapping (assoc params :regions (vec (reverse regions))) *dtm*)))
        "out of order")
    (is (= :datomic/partition-mapping-invalid
           (err #(dtm/params->mapping (assoc-in params [:regions 1 :start]
                                                (get-in params [:regions 0 :start]))
                                      *dtm*)))
        "overlapping")))
