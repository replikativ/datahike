(ns datahike.test.migrate-init-import-test
  "`import-db {:build-indexes? true}` end to end, against the database the DEFAULT import
   produces.

   That is the oracle, and it is not the same as the source database.
   `migrate-fidelity-test`'s docstring states it: a bulk build \"must produce a
   DB record equal to the one the transact path produces\". Comparing against
   the SOURCE instead would fail for a reason that has nothing to do with bulk —
   a `:keep-history? false` dump carries only the datoms that survived, so its
   transactions renumber densely on import whichever path reads it, and its
   fully-retracted entities are simply absent. Measured: both paths report
   `:max-tx-drift -7` on this fixture — negative because the dump names fewer
   transactions than its source had, and equal because the two paths now agree
   on numbering (the streaming path's old per-call `max-tx` bump, which made it
   -6, is gone).

   `migrate-init-build-test` covers the trees. This covers everything a DB
   record owes beyond them — `:hash`, `:schema`, `:rschema`, `:max-eid`,
   `:max-tx`, the ident maps — plus the refusals, which are the other half of
   the feature: a bulk import that quietly degraded to the streaming path would
   turn a configuration mistake into a mystifying performance report."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.schema :as ds]
            [datahike.test.utils :as utils]
            [datahike.migrate.ids :as ids]
            [datahike.constants :as const]
            [konserve.store :as ks]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- cfg
  "Pinned to persistent-set: bulk is refused for anything else, and this suite
   runs a second time under `:clj-hht` with `*default-index*` rebound."
  [history?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :index :datahike.index/persistent-set
   :keep-history? history? :schema-flexibility :write})

(def ^:private base-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :note :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/noHistory true}])

(defn- build-adversarial-db!
  "The same generator `migrate-fidelity-test` and `migrate-init-build-test` use:
   card-one overwrite, card-many add/retract/re-assert, retract-then-reassert,
   a fully retracted entity, schema added mid-history, and a `:db/noHistory`
   attribute overwritten. Each of those is a way `:hash` or the temporal split
   goes wrong, and a uniform random database exercises none of them."
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
  (d/transact conn [[:db/retract [:name "a"] :tag :x]])
  (d/transact conn [{:db/id [:name "a"] :tag :x}])
  conn)

;; ---------------------------------------------------------------------------
;; the comparison, kept identical to `migrate-fidelity-test`'s

(def ^:private index-fields
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(def ^:private derived-fields
  ;; :op-count is excluded for the reason `migrate-fidelity-test` records: it is
  ;; inert for persistent-set, and hitchhiker-tree — the only index that reads
  ;; it — is refused by `build-indexes-refusal`.
  ;; :max-tx is excluded because it differs BY DESIGN and is asserted separately.
  [:hash :schema :rschema :max-eid :system-entities :ident-ref-map :ref-ident-map
   :secondary-indices])

(defn- compare-records [a b]
  (reduce (fn [acc k]
            (let [x (get a k) y (get b k)]
              (if (contains? (set index-fields) k)
                (if (= (vec x) (vec y)) acc (assoc acc k [(count x) (count y)]))
                (if (= x y) acc (assoc acc k [x y])))))
          {}
          (concat index-fields derived-fields)))

(defn- tmp-path [tag]
  (str (System/getProperty "java.io.tmpdir") "/dh-bulkimp-" tag "-"
       (java.util.UUID/randomUUID)))

(defn- fresh [history?] (utils/setup-db (cfg history?)))

;; ---------------------------------------------------------------------------

(deftest bulk-and-transact-produce-the-same-database
  (testing "every index and every derived field, both history modes, and across
            a chunk boundary.

            Chunk size 5 matters independently: the bulk path normalises the
            dump to one file before sorting, so a chunk boundary that fell
            between an entity's datoms would show up here and nowhere else.

            `:eids :allocate` explicitly, because that is what makes this
            apples-to-apples: allocation is what the transact path does, so it
            is the mode in which the two must agree id-for-id. The default here
            is `:preserve`, which deliberately differs on a `:history? false`
            dump — see `preserve-keeps-the-source-ids`."
    (doseq [history? [true false]
            chunk    [1000 5]]
      (testing (str "keep-history? " history? ", chunk-size " chunk)
        (let [src (build-adversarial-db! (fresh history?))
              path (tmp-path (str history? "-" chunk))
              _ (m/export-db @src path {:history? history? :chunk-size chunk})
              tx-tgt (fresh history?)
              tx-rep (m/import-db tx-tgt path {})
              bk-tgt (fresh history?)
              bk-rep (m/import-db bk-tgt path {:build-indexes? true :eids :allocate})
              diffs (compare-records @tx-tgt @bk-tgt)]
          (is (empty? diffs)
              (str "fields differ: " (pr-str (keys diffs)) " -> " (pr-str diffs)))
          (is (true? (:build-indexes? bk-rep)) "the report says which path ran")
          (is (true? (:verified? bk-rep)) "and verification ran and passed")
          (is (= (:datom-count tx-rep) (:datom-count bk-rep)))
          (is (= (:tx-count tx-rep) (:tx-count bk-rep)))
          (testing ":max-tx now AGREES between the two paths.

                    It used to differ by one: the streaming import ended via
                    `transact-entities-directly`, which bumped max-tx once per
                    call, while the index build does not transact at all and
                    landed on the number the dump names. That per-call bump was
                    a bug — it made the streaming result depend on
                    `:batch-size` — and with it gone both paths land on the
                    dump's own max-tx. The two import paths producing the same
                    database is the property this whole test exists for, so
                    there is nothing left to exempt."
            (is (= (long (:max-tx @tx-tgt)) (long (:max-tx @bk-tgt)))))
          (teardown src) (teardown tx-tgt) (teardown bk-tgt))))))

(deftest bulk-reproduces-the-source-database-exactly
  (testing "with history kept, a bulk restore equals its SOURCE — not merely the
            transact import of the same dump.

            Stronger than the test above and only available in this mode: a
            history dump carries (current ∪ temporal), which is everything, so
            nothing has to be reconstructed or renumbered. `:max-tx` matches too,
            which the streaming path cannot manage."
    (let [src (build-adversarial-db! (fresh true))
          path (tmp-path "src")
          _ (m/export-db @src path {:history? true})
          tgt (fresh true)
          rep (m/import-db tgt path {:build-indexes? true})
          diffs (compare-records @src @tgt)]
      (is (empty? diffs)
          (str "fields differ: " (pr-str (keys diffs)) " -> " (pr-str diffs)))
      (is (= (:max-tx @src) (:max-tx @tgt)) ":max-tx too")
      (is (zero? (:max-tx-drift rep)) "and the report says there is no drift")
      (testing "as-of at every transaction, which final-state equality cannot see"
        (let [txs (sort (distinct (map :tx (d/datoms @src :eavt))))
              triples (fn [db] (set (map (juxt :e :a :v :added) (d/datoms db :eavt))))]
          (is (seq txs) "precondition: there are transactions to replay")
          (doseq [t txs]
            (is (= (triples (d/as-of @src t)) (triples (d/as-of @tgt t)))
                (str "as-of " t " differs")))))
      (teardown src) (teardown tgt))))

(deftest preserve-keeps-the-source-ids-and-holds-no-id-map
  (testing "`:eids :preserve` — the default for this path — is what makes a big
            restore fit in a bounded heap.

            `:allocate` builds a map with one entry per source entity and per
            source transaction, which `estimate-import-memory` calls the
            dominant term; on a large database it is the term that decides
            whether the import completes. An empty target has nothing for a
            source id to collide with, so `:preserve` needs no map at all — and
            skips the whole pre-pass, which is one of the two reads of the dump.

            The observable difference is a `:history? false` dump, where the
            surviving datoms come from scattered transactions: `:allocate`
            renumbers them densely (as the transact path does), `:preserve`
            leaves them where the source had them."
    (let [src (build-adversarial-db! (fresh false))
          path (tmp-path "preserve")
          _ (m/export-db @src path {:history? false})
          alloc (fresh false)
          a-rep (m/import-db alloc path {:build-indexes? true :eids :allocate})
          pres (fresh false)
          p-rep (m/import-db pres path {:build-indexes? true})]
      (is (zero? (:id-map-size p-rep)) ":preserve holds no id map")
      (is (pos? (:id-map-size a-rep)) ":allocate does, which is the cost it pays")
      (is (= (:max-tx @src) (:max-tx @pres))
          ":preserve lands on the source's own max-tx")
      (is (zero? (:max-tx-drift p-rep)))
      (is (< (long (:max-tx @alloc)) (long (:max-tx @src)))
          ":allocate compacts the surviving transactions, as the transact path does")
      (testing "and both report the same transaction count, from different sources
                — `:allocate` from the id map's :tids, `:preserve` by counting
                :db/txInstant assertions, which needs no set"
        (is (= (:tx-count a-rep) (:tx-count p-rep))))
      (testing "the DATA is the same either way — only the numbering differs"
        (let [triples (fn [db] (set (map (juxt :a :v) (d/datoms db :eavt))))]
          (is (= (triples @alloc) (triples @pres)))
          (is (= (triples @src) (triples @pres)))))
      (teardown src) (teardown alloc) (teardown pres))))

(deftest no-history-builds-no-temporal-trees
  (testing "`:keep-history? false` must leave the temporal fields nil, not hold
            empty trees.

            `writing/db->stored` omits the temporal keys entirely in this mode
            rather than storing empty ones, so building those trees would write
            index nodes that nothing will ever reference — garbage from the
            moment the commit lands. `build-family!` builds both trees by
            default, so this is the assertion that the bulk path asks it not to.

            `contains?` cannot express this: `DB` is a record and always
            contains its declared fields. The value is the claim."
    (let [src (build-adversarial-db! (fresh false))
          path (tmp-path "nohist")
          _ (m/export-db @src path {:history? false})
          tx-tgt (fresh false)
          _ (m/import-db tx-tgt path {})
          bk-tgt (fresh false)
          _ (m/import-db bk-tgt path {:build-indexes? true})]
      (doseq [k [:temporal-eavt :temporal-aevt :temporal-avet]]
        (is (nil? (get @bk-tgt k)) (str k " must be nil"))
        (is (= (get @tx-tgt k) (get @bk-tgt k))
            (str k " must match the streaming import")))
      (teardown src) (teardown tx-tgt) (teardown bk-tgt))))

(deftest a-bulk-built-database-survives-a-reconnect
  (testing "the trees have to be DURABLE, not merely in the connection's db value.

            Every other test here reads `@conn` in process, so a bulk build that
            never flushed its nodes — or flushed them under the wrong keys —
            would pass all of them. This releases the connection and reconnects
            from the store, which reads the six trees back through
            `stored->db`.

            The schema is the sharpest part of this: it is a STORED artifact
            under `schema-meta-key`, not something recomputed from the datoms at
            load time, so a bulk build that got `:schema` right in memory and
            failed to commit it would answer every query correctly until the
            process restarted."
    (let [src (build-adversarial-db! (fresh true))
          path (tmp-path "reconnect")
          _ (m/export-db @src path {:history? true})
          cfg' (cfg true)
          _ (d/create-database cfg')
          tgt (d/connect cfg')
          _ (m/import-db tgt path {:build-indexes? true})
          before @tgt
          _ (d/release tgt)
          reconnected (d/connect cfg')
          diffs (compare-records before @reconnected)]
      (is (empty? diffs)
          (str "fields differ after reconnect: " (pr-str (keys diffs)) " -> " (pr-str diffs)))
      (is (= (:max-tx before) (:max-tx @reconnected)))
      (is (= (:schema @src) (:schema @reconnected))
          "the schema came back from the store, not from a lucky in-memory value")
      (is (= (set (map (juxt :e :a :v :added) (d/datoms (d/history @src) :eavt)))
             (set (map (juxt :e :a :v :added) (d/datoms (d/history @reconnected) :eavt))))
          "and the full history reads back")
      (teardown src) (teardown reconnected))))

(deftest a-store-target-bulk-imports-identically
  (testing "the dump medium is not the bulk builder's business.

            It consumes the same `{:chunks .. :read ..}` seam the streaming
            importer does, so a konserve store — which stands in for S3 here —
            has to give the same result as a directory."
    (let [src (build-adversarial-db! (fresh true))
          store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                                 {:sync? true})
          target {:store store :prefix "bulk"}
          _ (m/export-db @src target {:history? true :chunk-size 5})
          tx-tgt (fresh true)
          _ (m/import-db tx-tgt target {})
          bk-tgt (fresh true)
          rep (m/import-db bk-tgt target {:build-indexes? true})
          diffs (compare-records @tx-tgt @bk-tgt)]
      (is (empty? diffs) (str "fields differ: " (pr-str (keys diffs))))
      (is (true? (:build-indexes? rep)))
      (teardown src) (teardown tx-tgt) (teardown bk-tgt))))

(deftest an-xform-runs-once-per-pass-and-agrees-with-the-streaming-path
  (testing "the bulk path reads the dump TWICE — once to build the id mapping,
            once to normalise — and instantiates a fresh transducer for each.

            That is sound only because `:xform` is documented as pure: both
            passes see the same input in the same order, so both instances
            produce the same stream. A stateful transducer is the case that
            would expose it if they did not, so `(take 25)` is asserted here and
            not only a stateless `remove`."
    (let [src (build-adversarial-db! (fresh true))
          path (tmp-path "xform")
          _ (m/export-db @src path {:history? true :chunk-size 5})
          drop-tags (remove (fn [record] (= :tag (nth record 1))))
          tx-tgt (fresh true)
          tx-rep (m/import-db tx-tgt path {:xform drop-tags})
          bk-tgt (fresh true)
          rep (m/import-db bk-tgt path {:xform drop-tags :build-indexes? true})
          diffs (compare-records @tx-tgt @bk-tgt)]
      (is (empty? diffs) (str "fields differ: " (pr-str (keys diffs))))
      (is (true? (:transformed? rep)))
      (is (empty? (filter #(= :tag (:a %)) (d/datoms (d/history @bk-tgt) :eavt)))
          "the transducer actually dropped something")
      (testing "verification still runs, with the dump's count adjusted by what
                the transducer removed — the same rule the streaming path uses.
                Left ON here on purpose: `:verify? false` would hide the whole
                question, and a bulk path that could not verify a transformed
                import would be verification theatre."
        (is (true? (:verified? rep)))
        (is (pos? (:dropped rep)) "and the drop is reported, not hidden")
        (is (= (:dropped tx-rep) (:dropped rep))
            "both paths count the same net removal"))
      (testing "a STATEFUL transducer spans the stream, not a chunk and not a pass"
        (let [tgt (fresh true)
              r (m/import-db tgt path {:xform (take 25) :build-indexes? true})]
          (is (= 25 (:datom-count r))
              "25 from the stream — not 25 per chunk, and not a different 25 per pass")
          (is (true? (:verified? r)) "and a truncating transducer still verifies")
          (teardown tgt)))
      (teardown src) (teardown tx-tgt) (teardown bk-tgt))))

;; ---------------------------------------------------------------------------
;; refusals

(deftest bulk-is-refused-with-a-reason-not-silently-downgraded
  (testing "`:build-indexes? true` that cannot be honoured THROWS.

            Falling back to the streaming path would be the friendlier-looking
            choice and the worse one: the import would succeed, take as long as
            it always did, and give the operator nothing to act on."
    (let [src (build-adversarial-db! (fresh true))
          path (tmp-path "refuse")
          _ (m/export-db @src path {:history? true})
          refusal (fn [opts]
                    (let [tgt (fresh true)
                          r (try (m/import-db tgt path (assoc opts :build-indexes? true))
                                 (catch Exception e e))
                          r (if (satisfies? clojure.core.async.impl.protocols/ReadPort r)
                              (async/<!! r) r)]
                      (teardown tgt)
                      (when (instance? Throwable r) (ex-message r))))]
      (is (re-find #"cannot merge" (str (refusal {:merge? true}))))
      (is (re-find #":eids" (str (refusal {:eids :offset})))
          ":offset is about fitting a dump around existing data, which is the
           case this path refuses outright")
      (is (nil? (refusal {:eids :preserve})) ":preserve is the DEFAULT here")
      (is (nil? (refusal {:eids :allocate})) "and :allocate is still available")
      (testing "`:sync? false` is NOT one of them any more, and this is the JVM's
                only coverage of the async arm.

                It used to be refused with \"the sort and the tree build are
                blocking\", which described an implementation rather than a
                property the builder cannot reproduce. `refusal` returns nil only
                when the import RAN and returned normally, so this asserts an
                actual index-build import driven through a `go` block and taken
                off a channel — the same source that Node runs, on the runtime
                where a failure is legible."
        (is (nil? (refusal {:sync? false}))))
      (teardown src))))

(deftest build-indexes-refusal-names-every-blocking-reason-at-once
  (testing "`build-indexes-refusal` is the predicate, and it reports ALL the reasons in
            one message rather than one per run.

            Called directly here on doctored inputs, because two of the
            conditions — a hitchhiker-tree target and `:attribute-refs? true` —
            are awkward to reach through `import-db` and are properties of the
            config, which is all this reads."
    (let [conn (fn [config] (atom {:config config}))
          pss {:index :datahike.index/persistent-set}
          ;; a manifest good enough to be accepted: the schema has to be
          ;; non-empty, because the id pre-pass reads it to find ref attributes
          man {:schema {:name {:db/valueType :db.type/string}}}]
      (is (nil? (m/build-indexes-refusal (conn pss) man {:sync? true}))
          "the ordinary case is not refused")
      (is (re-find #"persistent-set"
                   (m/build-indexes-refusal (conn {:index :datahike.index/hitchhiker-tree})
                                            man {:sync? true})))
      (is (re-find #"attribute-refs"
                   (m/build-indexes-refusal (conn (assoc pss :attribute-refs? true))
                                            man {:sync? true})))
      (is (re-find #"secondary"
                   (m/build-indexes-refusal (conn pss)
                                            {:schema {:by-name {:db.secondary/type :stratum}}}
                                            {:sync? true}))
          "a secondary index in the DUMP's schema, which the build does not construct")
      (is (re-find #"schema" (m/build-indexes-refusal (conn pss) {} {:sync? true}))
          "a manifest with no :schema KEY: ref values could not be remapped")
      (is (nil? (m/build-indexes-refusal (conn pss) {:schema {}} {:sync? true}))
          "an EMPTY schema is fine — a :schema-flexibility :read database
           declares no attributes and can hold no refs either")
      (testing "and several at once come back together"
        (let [why (m/build-indexes-refusal (conn (assoc pss :attribute-refs? true))
                                           man {:sync? true :merge? true})]
          (is (re-find #"merge" why))
          (is (re-find #"attribute-refs" why)))))))

;; ---------------------------------------------------------------------------
;; export-side :xform

(deftest export-xform-splits-a-database-per-tenant
  (testing "the motivating case for an export-side transform: one database that
            already holds every tenant, factored into per-tenant dumps.

            Multi-tenancy is usually worth adopting only once usage patterns are
            clear — which is AFTER a single database holds everyone — so the
            migration out of that shape is an export filter rather than a
            bespoke tool.

            Two things such a filter must get right, and the test pins both:
            SCHEMA DATOMS MUST BE KEPT (a dump of data with no schema imports
            into a database that declares nothing), and refs leaving the retained
            set dangle, which `:check-refs?` reports rather than hides."
    (let [src (utils/setup-db (cfg true))]
      (d/transact src [{:db/ident :tenant :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :note :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}])
      (d/transact src [{:db/id -1 :tenant "acme" :note "a1"}
                       {:db/id -2 :tenant "acme" :note "a2"}
                       {:db/id -3 :tenant "zenith" :note "z1"}])
      (let [;; entity ids belonging to one tenant, resolved before exporting
            acme (set (map first (d/q '[:find ?e :where [?e :tenant "acme"]] @src)))
            schema-datom? (fn [r] (or (ds/schema-attr? (nth r 1))
                                      (ds/entity-spec-attr? (nth r 1))))
            only-acme (filter (fn [r] (or (schema-datom? r)
                                          (contains? acme (nth r 0))
                                          ;; tx entities carry the transactions
                                          (= (nth r 0) (nth r 3)))))
            path (tmp-path "tenant-acme")
            _ (m/export-transformed @src path only-acme {:history? true})
            tgt (fresh true)
            rep (m/import-db tgt path {:check-refs? true})]
        (is (= #{"acme"} (set (map first (d/q '[:find ?t :where [_ :tenant ?t]] @tgt))))
            "only the retained tenant's data is in the restored database")
        (is (= #{"a1" "a2"} (set (map first (d/q '[:find ?n :where [_ :note ?n]] @tgt))))
            "and all of it")
        (is (seq (d/q '[:find ?e :where [?e :db/ident :tenant]] @tgt))
            "the schema came along — without it the target declares nothing")
        (is (true? (:verified? rep))
            "the count check holds: the dump describes what the filter produced")
        (teardown src) (teardown tgt)))))

;; ---------------------------------------------------------------------------
;; a COMPUTED `:eids` mapping

(defn- packer
  "A mapping with a handful of entries rather than one per entity — the shape a
   Datomic source needs. Source ids are astronomically large (~1.76e13) and dense
   within a partition, so a base per partition and an offset within it names every
   id in constant space.

   TOTAL — identity below `lo` — and that is the contract, not a convenience. The
   bulk path has no allocator: it needs FINAL ids before it sorts, which is the
   whole reason a caller supplies a mapping, so an id the mapping does not name
   can only pass through unchanged. The streaming path allocates instead. The two
   therefore agree only on a mapping that names everything, which
   `a-partial-mapping-is-the-callers-contract` pins directly."
  [lo base]
  (reify clojure.lang.ILookup
    (valAt [_ k] (if (and (number? k) (>= (long k) lo)) (+ base (- (long k) lo)) k))
    (valAt [this k nf] (let [v (.valAt this k)] (if (nil? v) nf v)))))

(defn- partial-packer
  "The same thing with a hole in it: nil for anything below `lo`, so the schema
   entities this fixture's records declare are unnamed."
  [lo base]
  (reify clojure.lang.ILookup
    (valAt [_ k] (when (and (number? k) (>= (long k) lo)) (+ base (- (long k) lo))))
    (valAt [this k nf] (or (.valAt this k) nf))))

(def ^:private dtm-lo 13194139534312)

(defn- dtm-records
  "Datomic-shaped: enormous entity ids, a ref between two of them, and a tx
   entity — the three things a mapping has to get right."
  []
  (let [t1 (+ const/tx0 1) t2 (+ const/tx0 2)]
    [[t1 :db/txInstant #inst "2021-01-01" t1 true]
     [100 :db/ident :name t1 true]
     [100 :db/valueType :db.type/string t1 true]
     [100 :db/cardinality :db.cardinality/one t1 true]
     [101 :db/ident :pal t1 true]
     [101 :db/valueType :db.type/ref t1 true]
     [101 :db/cardinality :db.cardinality/one t1 true]
     [t2 :db/txInstant #inst "2021-02-01" t2 true]
     [dtm-lo :name "a" t2 true]
     [(+ dtm-lo 1) :name "b" t2 true]
     [dtm-lo :pal (+ dtm-lo 1) t2 true]]))

(def ^:private dtm-schema
  {:name {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   :pal  {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}})

(defn- import-with
  "Import the Datomic-shaped records under `opts`; return the fields a mapping
   can move, or the failure's ex-data."
  [opts]
  (let [conn (fresh true)]
    (try
      (m/import-source conn (m/records->chunk-src (dtm-records))
                       (merge {:sync? true :verify? false :schema dtm-schema} opts))
      {:max-eid (:max-eid @conn)
       :max-tx  (:max-tx @conn)
       :hash    (:hash @conn)
       :names   (into #{} (map first) (d/q '[:find ?n :where [?e :name ?n]] @conn))
       :eids    (vec (sort (map first (d/q '[:find ?e :where [?e :name _]] @conn))))
       :ref     (first (d/q '[:find ?a ?b :where [?x :pal ?y] [?x :name ?a] [?y :name ?b]]
                            @conn))}
      (catch Exception e (or (ex-data e) {:threw (ex-message e)}))
      (finally (teardown conn)))))

(deftest a-computed-mapping-agrees-with-a-materialised-one
  (testing "`apply-mapping` reads ids with `get … not-found`, so an ILookup is
            already its whole contract. What was missing was everything AROUND
            it: the option schema rejected one, the refusal excluded it, and two
            fields read `:next-eid`/`(count …)` off a map a computed mapping does
            not have."
    (let [materialised (merge {dtm-lo 1000 (+ dtm-lo 1) 1001}
                              ;; total over this fixture's ids, so the two shapes
                              ;; are the SAME mapping and may be compared at all
                              {100 100 101 101})
          computed     (packer dtm-lo 1000)
          a (import-with {:eids materialised :build-indexes? true})
          b (import-with {:eids computed :build-indexes? true})]
      (is (= [1000 1001] (:eids a)) "the caller's ids are the ids that land")
      (is (= a b)
          "field for field — :hash, :max-eid, :max-tx, the ref and the values")
      (is (= ["a" "b"] (:ref b)) "including a ref VALUE remapped through the same mapping"))))

(deftest both-import-paths-agree-on-the-same-mapping
  (testing "The two paths used to disagree about what a mapping IS — the
            streaming one CALLED a function, the bulk one `get`-ed it, and
            `(get some-fn k nf)` is `nf`. So a mapping that worked on one
            silently mapped NOTHING on the other, with no error anywhere.
            `ids/lookup-id` is now the single owner, and this is the test that
            can tell.

            It earns its keep: keying `remember-eid` on `map?` alone — correct
            for a computed mapping, wrong for `nil` — silently stopped
            `:allocate` memoising, and the streaming path drifted to :max-eid 9
            where the bulk path said 4. Nothing else in the suite noticed."
    (doseq [[label eids] [["ILookup"   (packer dtm-lo 1000)]
                          ["fn"        (fn [e] (if (and (number? e) (>= (long e) dtm-lo))
                                                 (+ 1000 (- (long e) dtm-lo))
                                                 e))]
                          [":allocate" :allocate]]]
      (let [streaming (import-with {:eids eids})
            bulk      (import-with {:eids eids :build-indexes? true})]
        (is (= streaming bulk)
            (str label ": the streaming and bulk paths must produce the same database"))))))

(deftest an-allocated-mapping-and-the-running-fold-agree
  (testing "`run-index-build` used to take `:max-eid` from `(dec (:next-eid mapping))`
            whenever a mapping existed, and from the pass-2 running maxima
            otherwise. It now always uses the fold, because the fold is exact for
            EVERY mapping — computed over records that have already been remapped
            — while `:next-eid` exists only on what `build-mapping` returns, so a
            computed mapping made `(long nil)` throw.

            The two agreed, and this pins that so the simplification cannot
            silently become a behaviour change."
    (let [recs (dtm-records)
          mapping (ids/build-mapping
                   {:schema dtm-schema :system-entities #{} :max-eid const/e0 :max-tx const/tx0}
                   (fn [rf init] (reduce rf init recs)))
          imported (import-with {:eids :allocate :build-indexes? true})]
      (is (= (dec (long (:next-eid mapping))) (:max-eid imported))
          ":max-eid — the allocator's own answer and the fold's")
      (is (= (dec (long (:next-tx mapping))) (:max-tx imported))
          ":max-tx likewise"))))

(deftest a-partial-mapping-is-the-callers-contract
  (testing "A mapping that does not name an id leaves the two paths with different
            jobs, and neither is wrong: the streaming path ALLOCATES a fresh id,
            because it allocates anyway; the bulk path passes the id through
            UNCHANGED, because it has no allocator — it needs final ids before it
            sorts, which is the entire reason a caller supplies a mapping.

            So a caller mapping must be TOTAL for the two to agree. Pinned rather
            than fixed, because the alternative is giving the bulk path an
            allocator and losing the property that makes it a bulk path.

            What is NOT allowed, and used to happen silently, is the partial case
            CORRUPTING the streaming side: a computed mapping memoised nowhere, so
            every datom of an unnamed entity got its own fresh id. Measured on
            unpatched main with a partial function — schema eids `(1 4)` with one
            attribute's three datoms scattered across them, `:pal` therefore never
            a ref, and its ref value left as the untranslated source id."
    (let [streaming (import-with {:eids (partial-packer dtm-lo 1000)})
          bulk      (import-with {:eids (partial-packer dtm-lo 1000) :build-indexes? true})
          as-a-map  (import-with {:eids {dtm-lo 1000 (+ dtm-lo 1) 1001}})]
      (testing "the mapped ids land on both, and the ref value is remapped on both"
        (is (= [1000 1001] (:eids streaming)))
        (is (= [1000 1001] (:eids bulk)))
        (is (= ["a" "b"] (:ref streaming)) "NOT a dangling ref")
        (is (= ["a" "b"] (:ref bulk))))
      (testing "a partial computed mapping behaves exactly like a partial map"
        (is (= as-a-map streaming)
            "an unnamed id is allocated, once, and remembered — not re-allocated per datom"))
      (testing "and the bulk path differs from both, by passing unnamed ids through"
        (is (not= streaming bulk))))))

(deftest a-mapping-may-not-put-an-entity-at-or-above-tx0
  (testing "`tx0` PARTITIONS the id space, and every consumer downstream reads it
            that way — the running `:max-eid` counts anything at or above it as
            0. So a mapping that lands an entity there imports SUCCESSFULLY and
            publishes a `:max-eid` a later transact will allocate straight
            through, on top of live data.

            Sizing against `emax` (2,147,483,647) rather than `tx0` (536,870,912)
            is the easy way to get there — it looks like four times the room — and
            is what the report this came from actually hit."
    (let [r (import-with {:eids (packer dtm-lo 600000000) :build-indexes? true})]
      (is (= :import/eid-above-tx0 (:error r))
          (str "expected a refusal, got " (pr-str r)))
      (is (>= (long (:entity-id r)) (long const/tx0))))))
