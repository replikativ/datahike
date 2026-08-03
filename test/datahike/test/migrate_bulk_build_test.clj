(ns datahike.test.migrate-bulk-build-test
  "What a bulk build owes the transact path, tree by tree.

   `migrate-bulk-test` covers the index-layer primitive (`init-index-sorted`
   against `init-index`). This namespace covers the layer above it: turning a
   dump's HISTORY records back into the six trees a transacted database has.

   That split is not obvious and getting it wrong is silent. The temporal trees
   are not \"every record\": `with-datom`'s assert branch touches only the
   current trees, so a live card-many datom never reaches temporal, and
   `keep-history?` excludes `:db/noHistory` attributes outright. The history VIEW
   looks right anyway, because `dbu/distinct-datoms` unions temporal with the
   current datoms of exactly those two classes — so a database with wrong
   temporal trees answers `d/history` correctly and differs only in the trees.

   The oracle is therefore the source database's own trees, read directly, not
   anything derived through the query API."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.db.utils :as dbu]
            [datahike.migrate.bulk :as bulk]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.history :as mh]
            [datahike.migrate.manifest :as mman]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(def ^:private base-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :note :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/noHistory true}])

(defn- build-adversarial-db!
  "Every shape that makes the current/temporal split non-obvious.

   Mirrors `migrate-fidelity-test`'s generator and adds one case it does not
   have: a card-many value RETRACTED AND RE-ASSERTED. That shape is the only
   one that separates two plausible readings of the rule (see
   `only-the-final-record-of-a-run-is-dropped`), so without it a wrong rule
   passes."
  [conn]
  (d/transact conn (vec (take 4 base-schema)))
  (d/transact conn [{:db/id -1 :name "a" :score 1 :tag :x}
                    {:db/id -2 :name "b" :score 2 :tag :y}
                    {:db/id -3 :name "c" :score 3}])
  (d/transact conn [{:db/id [:name "a"] :score 10}])
  (d/transact conn [{:db/id [:name "a"] :score 100}])
  (d/transact conn [{:db/id [:name "b"] :tag :z}])
  (d/transact conn [[:db/retract [:name "b"] :tag :y]])
  (d/transact conn [[:db/retract [:name "c"] :score 3]])
  (d/transact conn [{:db/id [:name "c"] :score 3}])
  (d/transact conn [{:db/id [:name "a"] :pal [:name "c"]}])
  (d/transact conn [[:db/retractEntity [:name "c"]]])
  (d/transact conn [(nth base-schema 4)])
  (d/transact conn [{:db/id [:name "a"] :note "no history kept for this"}])
  (d/transact conn [{:db/id [:name "a"] :note "second value"}])
  ;; the separating shape
  (d/transact conn [[:db/retract [:name "a"] :tag :x]])
  (d/transact conn [{:db/id [:name "a"] :tag :x}])
  conn)

(defn- adversarial-conn []
  (build-adversarial-db!
   (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                    :keep-history? true :schema-flexibility :write})))

(defn- tup
  "Datom -> `[e a v t op]`, the record shape the dump uses."
  [dm]
  [(nth dm 0) (nth dm 1) (nth dm 2) (dd/datom-tx dm) (dd/datom-added dm)])

(defn- sort-eavt [records]
  (sort-by (fn [r] [(nth r 0) (str (nth r 1)) (str (nth r 2)) (nth r 3)]) records))

(defn- excluded-attr-pred
  "The attributes whose LIVE datom never reaches the temporal trees."
  [db]
  (let [rschema (:rschema db)
        no-history (set (:db/noHistory rschema))
        multival (set (:db.cardinality/many rschema))]
    (fn [a] (or (contains? no-history a) (contains? multival a)))))

(defn- diff [expected actual]
  {:missing (sort (set/difference expected actual))
   :spurious (sort (set/difference actual expected))})

;; ---------------------------------------------------------------------------

(deftest the-dump-records-partition-into-exactly-the-two-trees
  (testing "the rule a bulk build rests on, checked against the database that
            produced the records: folding the dump's history by `[e a v]` runs
            reconstructs BOTH trees exactly — not approximately, and not only in
            what the query API can see."
    (let [conn (adversarial-conn)
          db @conn
          records (vec (mman/export-records db {:history? true}))
          sorted (sort-eavt records)
          actual-current (set (map tup (seq (:eavt db))))
          actual-temporal (set (map tup (seq (:temporal-eavt db))))
          built-current (set (map vec (mh/current-from-eavt-sorted sorted)))
          built-temporal (set (map vec (mh/temporal-from-eavt-sorted
                                        (excluded-attr-pred db) sorted)))]
      (testing "precondition: the generator produced all three populations"
        (is (pos? (count records)))
        (is (pos? (count actual-current)))
        (is (pos? (count actual-temporal))))

      (testing "current"
        (is (= {:missing () :spurious ()} (diff actual-current built-current))))

      (testing "temporal"
        (is (= {:missing () :spurious ()} (diff actual-temporal built-temporal))))

      (testing "and the temporal tree is genuinely SMALLER than the record set —
                otherwise 'temporal = every record' would pass the check above
                and this whole namespace would be vacuous"
        (is (< (count actual-temporal) (count records))
            (str "temporal " (count actual-temporal) " vs records " (count records))))
      (teardown conn))))

(deftest only-the-final-record-of-a-run-is-dropped
  (testing "the rule drops the last record of an `[e a v]` run when it is a live
            assertion on an excluded attribute — NOT every assertion in that run.

            The difference is reachable and was measured: a card-many value
            retracted and then re-asserted has an earlier assertion that datahike
            DOES keep in temporal, because the retraction put it there. The
            weaker reading loses it. This pins the distinction directly, so a
            future simplification back to the wrong rule fails here rather than
            silently shrinking a temporal tree."
    (let [conn (adversarial-conn)
          db @conn
          records (vec (mman/export-records db {:history? true}))
          sorted (sort-eavt records)
          excluded? (excluded-attr-pred db)
          built (set (map vec (mh/temporal-from-eavt-sorted excluded? sorted)))
          ;; the WRONG reading, spelled out so the test states what it excludes
          live (set (map (fn [r] [(nth r 0) (nth r 1) (nth r 2)])
                         (mh/current-from-eavt-sorted sorted)))
          wrong (set (remove (fn [r]
                               (and (nth r 4)
                                    (contains? live [(nth r 0) (nth r 1) (nth r 2)])
                                    (excluded? (nth r 1))))
                             (map vec records)))
          actual-temporal (set (map tup (seq (:temporal-eavt db))))]
      (is (= {:missing () :spurious ()} (diff actual-temporal built))
          "the run-final rule matches the database")
      (is (not= wrong actual-temporal)
          "precondition: the generator actually contains the separating shape —
           if this passes vacuously the retract/re-assert case has been lost")
      (is (= 1 (count (set/difference actual-temporal wrong)))
          "and it separates them by exactly the re-asserted card-many datom")
      (teardown conn))))

(deftest no-history-attributes-are-absent-from-temporal-entirely
  (testing "`keep-history?` is `(and (-keep-history? db) (not (no-history? db a)))`,
            so a `:db/noHistory` attribute never reaches the temporal trees at
            all — not even its superseded values.

            Asserted on the TREE rather than through `d/history`, which unions
            the current datom back in and so cannot distinguish a correct
            temporal tree from one carrying noHistory records."
    (let [conn (adversarial-conn)
          db @conn
          temporal-attrs (set (map #(nth % 1) (seq (:temporal-eavt db))))]
      (is (contains? (set (:db/noHistory (:rschema db))) :note)
          "precondition: :note is a noHistory attribute")
      (is (not (contains? temporal-attrs :note))
          ":note must not appear in temporal-eavt")
      (is (contains? (set (map #(nth % 1) (seq (:eavt db)))) :note)
          "but it IS in the current tree — the asymmetry this rule is about")
      (teardown conn))))

(deftest live-card-many-datoms-are-absent-from-temporal
  (testing "a plain card-many assertion goes through `with-datom`'s added branch,
            which touches only the current indexes. So a live card-many datom is
            in `eavt` and not in `temporal-eavt` — the second of the two classes
            `distinct-datoms` unions back."
    (let [conn (adversarial-conn)
          db @conn
          current (set (map tup (seq (:eavt db))))
          temporal (set (map tup (seq (:temporal-eavt db))))
          live-tags (filter (fn [r] (= :tag (nth r 1))) current)]
      (is (pos? (count live-tags)) "precondition: there are live :tag datoms")
      (doseq [t live-tags]
        (is (not (contains? temporal t))
            (str "live card-many datom must not be in temporal: " (pr-str t))))
      (testing "while a RETRACTED card-many value is in temporal, with its marker"
        (let [tag-temporal (filter (fn [r] (= :tag (nth r 1))) temporal)]
          (is (pos? (count tag-temporal))
              "precondition: some :tag history reached temporal")
          (is (some (fn [r] (false? (nth r 4))) tag-temporal)
              "including at least one retraction marker")))
      (teardown conn))))

(deftest the-folds-hold-for-every-index-family
  (testing "`sort-family!` orders by each family's own comparator, so the folds
            see `[a e v t]` and `[a v e t]` as well as `[e a v t]`. They are
            still correct because all three keys are permutations of `[e a v]`
            followed by `t` — every `[e a v]` run stays adjacent and t-ascending.

            Load-bearing and previously undocumented, so it is checked rather
            than asserted in a comment."
    (let [conn (adversarial-conn)
          db @conn
          records (vec (mman/export-records db {:history? true}))
          excluded? (excluded-attr-pred db)
          reference (set (map vec (mh/temporal-from-eavt-sorted
                                   excluded? (sort-eavt records))))
          ref-current (set (map vec (mh/current-from-eavt-sorted (sort-eavt records))))]
      (doseq [[family key-fn]
              {:aevt (fn [r] [(str (nth r 1)) (nth r 0) (str (nth r 2)) (nth r 3)])
               :avet (fn [r] [(str (nth r 1)) (str (nth r 2)) (nth r 0) (nth r 3)])}]
        (testing (name family)
          (let [sorted (sort-by key-fn records)]
            (is (= reference (set (map vec (mh/temporal-from-eavt-sorted excluded? sorted))))
                "temporal fold agrees across families")
            (is (= ref-current (set (map vec (mh/current-from-eavt-sorted sorted))))
                "currentness fold agrees across families"))))
      (teardown conn))))

;; ---------------------------------------------------------------------------
;; the whole component, against the database it has to reproduce

(deftest build-family-reproduces-the-source-trees
  (testing "`sort-family!` + `build-family!` on a dump's records produce, for
            every family, trees whose CONTENTS equal the source database's.

            This is the component test `bulk.clj` never had. It is what makes the
            two defects above reachable from outside: the temporal comparison
            fails if the noHistory/card-many rule is wrong, in any family."
    (let [conn (adversarial-conn)
          db @conn
          rschema (:rschema db)
          ;; exactly how db.cljc:969-973 derives it — same map, same key
          index-config {:indexed (:db/index rschema)}
          store (:store db)
          records (vec (mman/export-records db {:history? true}))
          tmp (fs/temp-dir! "dh-bulk-build-test")]
      (try
        (doseq [[family current-key temporal-key]
                [[:eavt :eavt :temporal-eavt]
                 [:aevt :aevt :temporal-aevt]
                 [:avet :avet :temporal-avet]]]
          (testing (name family)
            (let [sorted-file (bulk/sort-family! records family 1000000 tmp)
                  {:keys [current temporal]}
                  (bulk/build-family! store :datahike.index/persistent-set
                                      family sorted-file index-config rschema)]
              (is (= {:missing () :spurious ()}
                     (diff (set (map tup (seq (get db current-key))))
                           (set (map tup (seq current)))))
                  "current tree")
              (is (= {:missing () :spurious ()}
                     (diff (set (map tup (seq (get db temporal-key))))
                           (set (map tup (seq temporal)))))
                  "temporal tree"))))
        (finally
          (doseq [n (or (fs/list-names tmp) [])] (fs/delete! (fs/join tmp n)))
          (fs/delete! tmp)
          (teardown conn))))))

;; ---------------------------------------------------------------------------

(deftest a-record-with-t-zero-decodes-as-a-retraction
  (testing "`dd/datom`'s 5-arity encodes `added` in the SIGN of tx, so t=0 has no
            sign and comes back retracted. Nothing in the import produces t=0 —
            `export-records` keeps only tx > tx0 — but a non-dump source feeding
            the same seam could, and it would silently invert the operation.

            Pinned so the constraint is visible to whoever writes the CSV reader."
    (let [d0 (dd/datom 1 :a "v" 0 true)]
      (is (false? (dd/datom-added d0))
          "t=0 cannot represent an assertion — a source must not emit it"))))
