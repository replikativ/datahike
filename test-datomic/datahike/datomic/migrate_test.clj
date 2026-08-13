(ns datahike.datomic.migrate-test
  "Datomic Pro <-> datahike, over the record seam.

   Lives on its own source path, `test-datomic`, reached only by the `:datomic`
   alias and the `:datomic` kaocha tier — `bb test datomic`, or the
   `datomic-test` CI job.

   The separate PATH is the point, not fastidiousness. Requiring this namespace
   without `com.datomic/peer` is a hard load failure, not a skip, and the
   `:integration` tier selects by path (`test/datahike/integration_test`), so
   sitting there would have broken the existing integration-test job outright.
   The `:migrate` tier would have claimed it too: its regex is
   `^datahike\\.test\\.(migrate-|…)`, which any `datahike.test.migrate-*` name
   matches. A path no other tier names is the only placement that is safe by
   construction rather than by remembering.

   `datomic:mem://` needs no license key, no transactor and no container, so this
   is an integration test only in the sense that it drives a second database.

   What is asserted here is what the ns docstring of `datahike.migrate.datomic`
   claims, in the same order: values and history survive, ids do not, and the
   correspondence between the two is queryable afterwards."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datomic.api :as dt]
            [datahike.api :as d]
            [datahike.migrate.datomic :as dtm]
            [datahike.constants :as const]))

(def ^:dynamic *dtm* nil)
(def ^:dynamic *dh* nil)
(def ^:dynamic *dh-cfg* nil)

(defn- datomic-fixture
  "A Datomic database with the shapes that make a migration non-trivial: a
   cardinality-one overwrite, a retraction, a ref between entities, a
   cardinality-many attribute, and historical `:db/txInstant`s."
  [uri]
  (dt/create-database uri)
  (let [c (dt/connect uri)]
    @(dt/transact c [[:db/add "datomic.tx" :db/txInstant #inst "2021-01-01"]
                     {:db/ident :p/name :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one
                      :db/unique :db.unique/identity}
                     {:db/ident :p/age :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one}
                     {:db/ident :p/pal :db/valueType :db.type/ref
                      :db/cardinality :db.cardinality/many}
                     {:db/ident :p/tag :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/many}])
    (let [r @(dt/transact c [[:db/add "datomic.tx" :db/txInstant #inst "2021-02-01"]
                             {:db/id "a" :p/name "Ann" :p/age 30 :p/pal "b"
                              :p/tag ["x" "y"]}
                             {:db/id "b" :p/name "Bob" :p/age 40}])
          ann (get-in r [:tempids "a"])]
      @(dt/transact c [[:db/add "datomic.tx" :db/txInstant #inst "2021-03-01"]
                       [:db/add ann :p/age 31]])              ; card-one overwrite
      @(dt/transact c [[:db/add "datomic.tx" :db/txInstant #inst "2021-04-01"]
                       [:db/retract ann :p/tag "y"]])         ; card-many retraction
      c)))

(use-fixtures
  :each
  (fn [f]
    (let [uri (str "datomic:mem://dh-mig-" (rand-int 1000000))
          cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :write}]
      (d/create-database cfg)
      (binding [*dtm* (datomic-fixture uri)
                *dh* (d/connect cfg)
                *dh-cfg* cfg]
        (try (f)
             (finally
               (d/release *dh*) (d/delete-database cfg)
               (dt/release *dtm*) (dt/delete-database uri)))))))

;; ---------------------------------------------------------------------------
;; Datomic -> datahike

(deftest values-and-history-survive-the-import
  (let [rep (dtm/import-from-datomic! *dh* *dtm*)]
    (is (true? (:verified? rep))
        "the record count the source declared is the count that landed")
    (testing "current values"
      (is (= #{["Ann" 31] ["Bob" 40]}
             (into #{} (d/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] @*dh*)))
          "including the cardinality-one overwrite, 30 -> 31"))
    (testing "the ref between two entities"
      (is (= #{["Ann" "Bob"]}
             (into #{} (d/q '[:find ?a ?b :where [?x :p/pal ?y] [?x :p/name ?a] [?y :p/name ?b]]
                            @*dh*)))))
    (testing "cardinality-many, after one value was retracted"
      (is (= #{"x"} (into #{} (map first)
                          (d/q '[:find ?t :where [?e :p/name "Ann"] [?e :p/tag ?t]] @*dh*)))))
    (testing "history carries the retraction and the superseded value"
      (is (= #{[30 true] [30 false] [31 true]}
             (into #{} (d/q '[:find ?v ?op :where [?e :p/name "Ann"] [?e :p/age ?v _ ?op]]
                            (d/history @*dh*))))))
    (testing "the SOURCE's transaction times, not the import's"
      (is (= [#inst "2021-01-01" #inst "2021-02-01" #inst "2021-03-01" #inst "2021-04-01"]
             (sort (map first (d/q '[:find ?i :where [_ :db/txInstant ?i]] @*dh*))))))))

(deftest provenance-makes-the-id-remap-queryable
  (testing "entity and transaction ids cannot survive — Datomic's exceed datahike's
            emax/txmax by four orders of magnitude — so the correspondence is
            recorded as data instead"
    (dtm/import-from-datomic! *dh* *dtm*)
    (let [prov (into {} (d/q '[:find ?dt ?tx :where [?tx :datomic/t ?dt]] @*dh*))
          src-ts (mapv :t (dt/tx-range (dt/log *dtm*) nil nil))]
      (is (= (count src-ts) (count prov))
          "one provenance datom per source transaction")
      (is (= (set src-ts) (set (keys prov)))
          "and they name the source's own t values")
      (is (every? #(<= const/tx0 % const/txmax) (vals prov))
          "while the datahike side is in datahike's transaction space")
      (testing "so a source t answers with the transaction it became"
        (let [t (first src-ts)]
          (is (= #{[(get prov t)]}
                 (into #{} (d/q '[:find ?tx :in $ ?t :where [?tx :datomic/t ?t]] @*dh* t))))))
      (testing "the Datomic tx entity id is recorded too"
        (is (= (count src-ts)
               (count (d/q '[:find ?tx ?e :where [?tx :datomic/tx-eid ?e]] @*dh*))))))))

(deftest provenance-can-be-declined
  (dtm/import-from-datomic! *dh* *dtm* {:provenance? false})
  (is (empty? (d/q '[:find ?tx :where [?tx :datomic/t _]] @*dh*))
      "no provenance datoms")
  (is (= 2 (count (d/q '[:find ?e :where [?e :p/name _]] @*dh*)))
      "and the data still arrives"))

(deftest eids-preserve-is-refused-rather-than-silently-remapped
  (testing "datahike does not range-check an incoming eid, it reallocates — so
            honouring :preserve would look like it worked. Refused instead."
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cannot be honoured"
                          (dtm/import-from-datomic! *dh* *dtm* {:eids :preserve})))))

(deftest a-window-splits-the-log-without-splitting-a-transaction
  (testing "descriptors are t RANGES, so the log is never held whole; a chunk is
            whole transactions by construction"
    (let [src (dtm/source *dtm* {:window 1})]
      (let [per (mapv (fn [c] (count (distinct (map #(nth % 3) ((:read src) c {})))))
                      (:chunks src))]
        (is (every? #(<= % 1) per)
            (str "no chunk holds more than one transaction, got " per))
        (is (= 4 (reduce + per))
            "and between them the chunks cover every source transaction")
        (is (some zero? per)
            "some windows are EMPTY: Datomic's t space has gaps (1000,1001,1004,1005),
             and descriptors are t RANGES, so a window may cover no transaction. That
             is the price of descriptors that cost O(1) instead of holding every t.")))))

;; ---------------------------------------------------------------------------
;; datahike -> Datomic

(deftest export-round-trips-back-into-a-fresh-datomic
  (testing "out through the sink and back in reproduces the values. A FRESH
            Datomic database, because :db/txInstant must be >= the basis and a
            fresh basis is 1970."
    (dtm/import-from-datomic! *dh* *dtm*)
    (let [uri2 (str "datomic:mem://dh-mig-back-" (rand-int 1000000))]
      (dt/create-database uri2)
      (let [c2 (dt/connect uri2)]
        (try
          (let [res (dtm/export-to-datomic! @*dh* c2)]
            (is (pos? (:transactions res)) "something was written")
            (let [db2 (dt/db c2)]
              (is (= #{["Ann" 31] ["Bob" 40]}
                     (into #{} (dt/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] db2)))
                  "current values survive the return trip")
              (let [instants (set (map first (dt/q '[:find ?i :where [_ :db/txInstant ?i]] db2)))]
                (is (every? instants [#inst "2021-01-01" #inst "2021-02-01"
                                      #inst "2021-03-01" #inst "2021-04-01"])
                    "every original transaction time survives the return trip")
                (is (contains? instants #inst "2020-12-31T23:59:59.999-00:00")
                    "plus one EXTRA transaction at (first instant - 1ms): Datomic
                     requires an attribute installed before its first use, and
                     the FIRST transaction both installs the provenance schema
                     and is stamped with it — datahike's doing, not the source's,
                     since Datomic could not have produced such a transaction.
                     The schema half needs a time of its own or it would be
                     stamped `now` and push the basis past every historical
                     instant after it. Documented in the ns; the schema's install
                     time is approximate, its content and ordering are exact."))))
          (finally (dt/release c2) (dt/delete-database uri2)))))))

;; ---------------------------------------------------------------------------
;; flexibility: an occupied target, and caller-controlled eids

(deftest import-into-an-existing-datahike-database
  (testing "`:merge? true` lifts the empty-target refusal. Append-only: the
            index-build path is not involved and `transact-entities-directly`
            does NOT resolve :db.unique/identity, so this ADDS entities rather
            than upserting onto matching ones."
    (d/transact *dh* [{:db/ident :local/note :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact *dh* [{:local/note "pre-existing"}])
    (let [before (count (d/datoms @*dh* :eavt))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (dtm/import-from-datomic! *dh* *dtm*))
          "without :merge? a non-empty target is refused")
      (let [rep (dtm/import-from-datomic! *dh* *dtm* {:merge? true})]
        (is (pos? (:datom-count rep)))
        (is (> (count (d/datoms @*dh* :eavt)) before)
            "the Datomic data was added")
        (is (= #{"pre-existing"}
               (into #{} (map first) (d/q '[:find ?n :where [_ :local/note ?n]] @*dh*)))
            "and the pre-existing data is untouched")
        (is (= #{"Ann" "Bob"}
               (into #{} (map first) (d/q '[:find ?n :where [_ :p/name ?n]] @*dh*))))))))

(deftest eids-can-be-remapped-by-the-caller
  (testing ":eids takes a map or a function, so a caller who knows how the two id
            spaces relate can say so. Only :preserve is refused, because Datomic's
            ids do not fit."
    (testing "a function: offset every source eid into a chosen band"
      (let [rep (dtm/import-from-datomic! *dh* *dtm* {:eids (fn [e] (+ 100000 (mod e 1000)))})]
        (is (pos? (:datom-count rep)))
        (is (= #{"Ann" "Bob"}
               (into #{} (map first) (d/q '[:find ?n :where [_ :p/name ?n]] @*dh*)))
            "the data still lands and refs still resolve")))))

(deftest a-window-bounds-what-a-read-holds
  (testing "descriptors are t ranges and :read returns ONE window, so neither the
            descriptor list nor a read holds the log. The whole-log scan that
            `log-t-range` used to do — binding the seq and then walking it to the
            end — is gone; `basis-t` answers in O(1)."
    (let [src (dtm/source *dtm* {:window 2})
          all (mapcat (fn [c] ((:read src) c {})) (:chunks src))]
      (is (every? map? (:chunks src)) "descriptors are metadata, not records")
      (is (every? #(= #{:from :to} (set (keys %))) (:chunks src))
          "and carry only a t range")
      (is (= 4 (count (distinct (map #(nth % 3) all))))
          "the windows between them still cover every transaction"))))

;; ---------------------------------------------------------------------------
;; memory

(deftest a-read-returns-a-realized-bounded-collection
  (testing "The seam requires `:read` to return \"a realized, bounded
            collection\": the importer `mapv`s it immediately, so laziness buys
            nothing, and a lazy read holding a cursor across chunks is a leak.

            This replaced a WeakReference test that asserted chunk results were
            unreachable afterwards. That test passed against the BUGGY
            `log-t-range` as well as the fixed one — it could not fail, so it
            asserted nothing. The property below can: make `:read` lazy and it
            goes red."
    (let [src (dtm/source *dtm* {:window 2})
          rs  ((:read src) (first (:chunks src)) {})]
      (is (vector? rs)
          "a realized vector, not a lazy seq holding a Datomic cursor")
      (testing "and re-entrant — verify and the index build read chunks twice"
        (is (= rs ((:read src) (first (:chunks src)) {}))
            "the same descriptor yields the same records on a second read")))))

(deftest the-log-is-not-scanned-to-find-its-bounds
  (testing "`log-t-range` used to bind `(tx-range log nil nil)` and call `last` on
            it: a full scan AND a retained head. `basis-t` answers in O(1). Probe
            it by counting how many log entries are realized while building the
            source — building a source must not touch the log beyond its first
            entry."
    (let [realized (atom 0)
          orig     dt/tx-range]
      (with-redefs [dt/tx-range (fn [& args]
                                  (map (fn [tx] (swap! realized inc) tx)
                                       (apply orig args)))]
        (let [src (dtm/source *dtm* {:window 100})]
          (is (<= @realized 1)
              (str "building a source realized " @realized " log entries; it should "
                   "read at most the first, and take the upper bound from basis-t"))
          (is (seq (:chunks src)) "and it still produced chunks"))))))

(deftest one-huge-transaction-imports-without-being-split
  (testing "The scale run covered many small transactions. The opposite shape —
            few but enormous — is what broke `tx-aligned-chunks` before, because a
            chunk grows to the next change of `t` and a single transaction cannot
            be split at all. A Datomic window of 1 holding 5000 datoms must still
            arrive whole."
    (let [uri (str "datomic:mem://dh-fat-" (rand-int 1000000))]
      (dt/create-database uri)
      (let [c (dt/connect uri)]
        (try
          @(dt/transact c [{:db/ident :k :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
          @(dt/transact c (vec (for [i (range 5000)] {:db/id (str "e" i) :k i})))
          (let [cfg {:store {:backend :memory :id (random-uuid)}
                     :keep-history? true :schema-flexibility :write}]
            (d/create-database cfg)
            (let [conn (d/connect cfg)]
              (try
                (let [src (dtm/source c {:window 1})
                      big (apply max (map (fn [ch] (count ((:read src) ch {}))) (:chunks src)))
                      rep (dtm/import-from-datomic! conn c {:window 1})]
                  (is (>= big 5000)
                      (str "one chunk carries the whole fat transaction, got " big))
                  (is (true? (:verified? rep)))
                  (is (= 5000 (count (d/q '[:find ?e :where [?e :k _]] @conn)))
                      "and every entity landed"))
                (finally (d/release conn) (d/delete-database cfg)))))
          (finally (dt/release c) (dt/delete-database uri)))))))

;; ---------------------------------------------------------------------------
;; differential: Datomic -> datahike -> Datomic, compared against the original

(defn- shape
  "A Datomic database's transaction log reduced to what a migration CAN preserve.

   Entity ids, transaction ids and tx entity ids are all remapped by a round trip
   (see the ns docstring on `datahike.migrate.datomic`), so comparing them would
   only re-measure that. What is left is the shape: per transaction, in order, the
   multiset of `[attribute value op]` — with ref values dropped, since a ref's
   value IS an entity id and cannot survive either.

   `:datomic/t` and `:datomic/tx-eid` are dropped as well: they are provenance
   ADDED by the import, so the round-tripped database has them and the original
   never did."
  [conn]
  (let [db     (dt/db conn)
        refs   (into #{} (map first)
                     (dt/q '[:find ?i :where [?a :db/valueType :db.type/ref] [?a :db/ident ?i]] db))
        idents (into {} (dt/q '[:find ?e ?i :where [?e :db/ident ?i]] db))
        ;; provenance the import ADDS: both the values and the :db/ident datoms
        ;; that define them. Dropping only the values left the schema behind and
        ;; showed up as a phantom delta.
        prov   #{:datomic/t :datomic/tx-eid}
        drop?  (fn [a v] (or (prov a) (= a :db.install/attribute)
                             (and (= a :db/ident) (prov v))))]
    (->> (dt/tx-range (dt/log conn) nil nil)
         (map (fn [{:keys [data]}]
                (frequencies
                 (keep (fn [^datomic.Datom dm]
                         (let [a (idents (.a dm))]
                           (when-not (or (nil? a) (drop? a (.v dm)) (refs a))
                             [a (.v dm) (.added dm)])))
                       data))))
         (remove empty?)
         vec)))

(deftest datomic-round-trips-to-a-shape-identical-datomic
  (testing "Datomic -> datahike -> Datomic, compared against the ORIGINAL Datomic.

            Not id-identical, and cannot be: ids are remapped in both directions.
            But everything a migration claims to preserve — the transactions, in
            order, and what each asserted or retracted — should come back."
    (dtm/import-from-datomic! *dh* *dtm*)
    (let [uri2 (str "datomic:mem://dh-rt-" (rand-int 1000000))]
      (dt/create-database uri2)
      (let [c2 (dt/connect uri2)]
        (try
          (dtm/export-to-datomic! @*dh* c2)
          (let [a (shape *dtm*) b (shape c2)]
            ;; MEASURED: 4 source transactions come back as 5. Not a defect and not
            ;; noise — it is the schema/data split, the one structural difference a
            ;; Datomic round trip introduces. Datomic will not use an attribute in
            ;; the transaction that installs it, while datahike will, and the
            ;; import's FIRST transaction does exactly that with the provenance
            ;; schema. Asserted exactly, so that a SECOND source of extra
            ;; transactions would still fail this.
            ;;
            ;; This fixture cannot tell "one split" from "one split PER schema
            ;; transaction", because all of its schema is in the first
            ;; transaction — and an earlier sink did the latter. See
            ;; `only-the-provenance-transaction-splits-however-much-schema-there-is`,
            ;; which adds a second schema transaction for that reason.
            (is (= (inc (count a)) (count b))
                (str "exactly one extra transaction, from the schema/data split: "
                     (count a) " -> " (count b)))
            (is (= (apply merge-with + a)
                   ;; `dissoc`, not `(update … dec)`: the key is ABSENT from the
                   ;; original, and a count of 0 is not the same map as no key.
                   (dissoc (apply merge-with + b)
                           ;; the split transaction's own instant, source minus 1ms
                           [:db/txInstant #inst "2020-12-31T23:59:59.999-00:00" true]))
                "and across the whole log, every assertion and retraction is the
                 same one for one. Diagnosed in the REPL: the ONLY residue is the
                 split transaction's synthetic instant. Nothing is lost, dropped
                 or altered — the split moves datoms between transactions and
                 adds a timestamp for the half it created.")
            (testing "current values agree through the query api too"
              (is (= (into #{} (dt/q '[:find ?n ?g :where [?e :p/name ?n] [?e :p/age ?g]] (dt/db *dtm*)))
                     (into #{} (dt/q '[:find ?n ?g :where [?e :p/name ?n] [?e :p/age ?g]] (dt/db c2)))))
              (is (= (into #{} (dt/q '[:find ?n ?t :where [?e :p/name ?n] [?e :p/tag ?t]] (dt/db *dtm*)))
                     (into #{} (dt/q '[:find ?n ?t :where [?e :p/name ?n] [?e :p/tag ?t]] (dt/db c2)))))))
          (finally (dt/release c2) (dt/delete-database uri2)))))))


(deftest datahike-round-trips-through-datomic-and-back
  (testing "The other direction: a datahike database out to Datomic and back into
            a fresh datahike. This is the one a user doing a there-and-back
            migration actually performs, and it exercises the sink and the source
            against each other rather than each against Datomic alone."
    (let [src-cfg {:store {:backend :memory :id (random-uuid)}
                   :keep-history? true :schema-flexibility :write}
          tgt-cfg {:store {:backend :memory :id (random-uuid)}
                   :keep-history? true :schema-flexibility :write}
          uri (str "datomic:mem://dh-rt2-" (rand-int 1000000))]
      (d/create-database src-cfg) (d/create-database tgt-cfg) (dt/create-database uri)
      (let [src (d/connect src-cfg) tgt (d/connect tgt-cfg) c (dt/connect uri)]
        (try
          (d/transact src [{:db/ident :n :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            ;; unique, because the overwrite below addresses the
                            ;; entity by lookup ref
                            :db/unique :db.unique/identity}
                           {:db/ident :age :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
          (d/transact src [{:db/id -1 :n "Ann" :age 30} {:db/id -2 :n "Bob" :age 40}])
          (d/transact src [{:db/id [:n "Ann"] :age 31}])
          (dtm/export-to-datomic! @src c)
          (dtm/import-from-datomic! tgt c)
          (let [q '[:find ?n ?a :where [?e :n ?n] [?e :age ?a]]]
            (is (= (into #{} (d/q q @src)) (into #{} (d/q q @tgt)))
                "current values survive datahike -> Datomic -> datahike"))
          (is (= (into #{} (d/q '[:find ?v ?op :where [?e :n "Ann"] [?e :age ?v _ ?op]]
                                (d/history @src)))
                 (into #{} (d/q '[:find ?v ?op :where [?e :n "Ann"] [?e :age ?v _ ?op]]
                                (d/history @tgt))))
              "and so does the history of the overwritten value")
          (finally
            (d/release src) (d/delete-database src-cfg)
            (d/release tgt) (d/delete-database tgt-cfg)
            (dt/release c) (dt/delete-database uri)))))))

(deftest only-the-provenance-transaction-splits-however-much-schema-there-is
  (testing "The sink splits a transaction that INSTALLS an attribute and USES it,
            because Datomic refuses that and datahike allows it. Exactly one
            transaction in a Datomic round trip is of that shape, and it is
            datahike's own doing rather than the source's: `source` emits the
            provenance schema with the log's FIRST transaction and stamps
            `:datomic/t` on that same transaction.

            A Datomic SOURCE cannot produce such a transaction — Datomic would
            have refused it — so every OTHER schema transaction must come back
            whole. It used to split all of them, because the predicate was the
            mere presence of schema datoms rather than a use of what they
            install. Measured before the fix: four source transactions came back
            as six.

            The fixture above puts all of its schema in the first transaction,
            so it cannot tell the two apart; this one adds a SECOND schema
            transaction, which is the whole point."
    (let [uri (str "datomic:mem://dh-split-" (rand-int 1000000))]
      (dt/create-database uri)
      (let [src (dt/connect uri)]
        (try
          @(dt/transact src [{:db/ident :s/name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             [:db/add "datomic.tx" :db/txInstant #inst "2021-01-01"]])
          @(dt/transact src [{:s/name "Ann"}
                             [:db/add "datomic.tx" :db/txInstant #inst "2021-02-01"]])
          ;; The second schema transaction. Installs `:s/age`, uses nothing it
          ;; installs — its only other datoms are `:db/txInstant` and the
          ;; provenance the import adds, all defined earlier.
          @(dt/transact src [{:db/ident :s/age :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}
                             [:db/add "datomic.tx" :db/txInstant #inst "2021-03-01"]])
          @(dt/transact src [{:s/name "Ann" :s/age 31}
                             [:db/add "datomic.tx" :db/txInstant #inst "2021-04-01"]])

          (dtm/import-from-datomic! *dh* src)
          (let [uri2 (str "datomic:mem://dh-split-t-" (rand-int 1000000))]
            (dt/create-database uri2)
            (let [tgt (dt/connect uri2)]
              (try
                (dtm/export-to-datomic! @*dh* tgt)
                (let [n-src (count (shape src))
                      n-tgt (count (shape tgt))]
                  (is (= (inc n-src) n-tgt)
                      (str "exactly ONE extra transaction — the provenance split — "
                           "however many schema transactions the source had: "
                           n-src " -> " n-tgt))
                  (testing "and every original transaction time is still there"
                    (let [instants (set (map first (dt/q '[:find ?i :where [_ :db/txInstant ?i]]
                                                         (dt/db tgt))))]
                      (is (every? instants [#inst "2021-01-01" #inst "2021-02-01"
                                            #inst "2021-03-01" #inst "2021-04-01"]))
                      (testing "with the one split stamped a millisecond before the first"
                        (is (contains? instants #inst "2020-12-31T23:59:59.999-00:00")))
                      (testing "and NO split before the second schema transaction"
                        (is (not (contains? instants #inst "2021-02-28T23:59:59.999-00:00"))
                            "a gratuitous split would land here")))))
                (finally (dt/release tgt) (dt/delete-database uri2)))))
          (finally (dt/release src) (dt/delete-database uri)))))))
