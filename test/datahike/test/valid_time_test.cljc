(ns datahike.test.valid-time-test
  "Tests for the bitemporal valid-time tx-meta attributes that graduated
   into the system schema:

   - `:db.valid/from` and `:db.valid/to` are pre-installed system attrs.
   - Consumers attach them to the writing tx via the `:tx-meta` map-arg
     form — `(d/transact conn {:tx-data [...] :tx-meta {:db.valid/from
     #inst ... :db.valid/to #inst ...}})`. The legacy inline-tempid form
     `{:db/id \"datomic.tx\" :db.valid/from ...}` also works (both shapes
     normalise to the same internal `{:tx-data :tx-meta}` representation
     at `api/impl.cljc:29-41`), but the map-arg form is idiomatic — see
     `schema.cljc:71-74`: \":db.valid/from and :db.valid/to graduate from
     userland tx-meta into datahike system schema.\"
   - Both attrs are `:db/index true` so they materialise into the
     temporal AVET index for range seeks by the query planner.

   This file accompanies the system-schema graduation in commit 1 of
   the `feature/bitemporal-v1` branch. The planner-recognised rule
   rewrites (`valid-at`, `valid-between`, …) ship in commit 3 — this
   file's tests cover the storage layer only."
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.api :as d]
            [datahike.schema :as s]
            [datahike.constants :as c]))

(defn- fresh-conn
  ([] (fresh-conn {}))
  ([extra-cfg]
   (let [cfg (merge {:store {:backend :memory :id (random-uuid)}
                     :schema-flexibility :write
                     :keep-history? true}
                    extra-cfg)]
     (d/create-database cfg)
     (d/connect cfg))))

(deftest valid-time-attrs-are-system-schema
  (testing "both attrs appear in `system-schema`"
    (is (some #(= :db.valid/from (:db/ident %)) c/system-schema))
    (is (some #(= :db.valid/to   (:db/ident %)) c/system-schema)))
  (testing "schema recognises them as meta-attributes"
    (is (s/meta-attr? :db.valid/from))
    (is (s/meta-attr? :db.valid/to)))
  (testing "no user-side schema install is needed — fresh DB accepts vt tx-meta"
    ;; System attrs are recognised by the transactor via the implicit
    ;; schema (same path `:db/txInstant` uses). They don't need a
    ;; `[:db/add ... :db/ident ...]` install — they're built in.
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :pos/x
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (is (some? (d/transact conn
                             {:tx-data [{:pos/x 1}]
                              :tx-meta {:db.valid/from #inst "2024-01-01"
                                        :db.valid/to   #inst "2024-07-01"}}))))))

(deftest valid-time-tx-meta-lands-on-the-tx-entity
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :pos/x
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (let [report (d/transact conn
                             {:tx-data [{:pos/x 42}]
                              :tx-meta {:db.valid/from #inst "2024-01-01"
                                        :db.valid/to   #inst "2024-07-01"}})
          tx    (get-in report [:tempids :db/current-tx])
          db    (d/db conn)
          pulled (d/pull db '[*] tx)]
      (testing "the writing tx resolves via `:db/current-tx` and the tx-meta attrs land"
        (is (= #inst "2024-01-01" (:db.valid/from pulled)))
        (is (= #inst "2024-07-01" (:db.valid/to   pulled))))
      (testing "the user datom is on its own entity, not the tx"
        (is (= 42 (-> (d/q '[:find ?x . :where [_ :pos/x ?x]] db)))))
      (testing "tx-data contains exactly the meta datoms + the user datom + :db/txInstant"
        (is (= 4 (count (:tx-data report))))))))

(deftest valid-time-attrs-are-indexed-and-queryable-via-history
  ;; The tx-entity isn't a "currently asserted" entity (no eid in user
  ;; space), so its datoms live in the temporal index. This is the same
  ;; place auditors look for `:db/txInstant`-keyed reads. Range queries
  ;; on `:db.valid/from` therefore run against `(d/history db)`, and
  ;; should run fast — the planner-rule rewrite (commit 3) leans on this.
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :pos/x
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (dotimes [i 5]
      (d/transact conn
                  {:tx-data [{:pos/x i}]
                   :tx-meta {:db.valid/from (java.util.Date.
                                             (+ 1700000000000 (* i 86400000)))}}))
    (let [db   (d/db conn)
          hist (d/history db)
          all-vts (d/q '[:find [?vf ...]
                         :where [_ :db.valid/from ?vf]]
                       hist)]
      (testing "all 5 vt-bearing txes are recoverable via history"
        (is (= 5 (count all-vts))))
      (testing "range query: find txes with vt-from in [t1, t2)"
        (let [t1 #inst "2023-11-16T00:00:00.000-00:00"
              t2 #inst "2023-11-18T00:00:00.000-00:00"
              n (d/q '[:find (count ?tx) .
                       :in $ ?from ?to
                       :where
                       [?tx :db.valid/from ?vf]
                       [(.compareTo ^java.util.Date ?vf ?from) ?cf]
                       [(<= 0 ?cf)]
                       [(.compareTo ^java.util.Date ?vf ?to) ?ct]
                       [(< ?ct 0)]]
                     hist t1 t2)]
          (is (= 2 n)))))))

(deftest valid-to-is-optional
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :pos/x
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (let [report (d/transact conn
                             {:tx-data [{:pos/x 1}]
                              :tx-meta {:db.valid/from #inst "2024-01-01"}})
          tx    (get-in report [:tempids :db/current-tx])
          pulled (d/pull (d/db conn) '[*] tx)]
      (testing "vt-from alone is sufficient; vt-to may be omitted"
        (is (= #inst "2024-01-01" (:db.valid/from pulled)))
        (is (nil? (:db.valid/to pulled)))))))

(deftest system-attrs-show-up-as-indexed
  ;; Regression: `:db.valid/from` / `:db.valid/to` must appear in
  ;; rschema's `:db/index` set so `(dbu/indexing? db ...)` returns
  ;; true and the planner's AVET pushdown for predicates pivoting on
  ;; these attrs actually fires. Without it, `[(<= ?vf ?at)]` after
  ;; `[?tx :db.valid/from ?vf]` silently returned 0 results (the
  ;; planner computed pushdown bounds but the seek path treated the
  ;; attr as non-indexed).
  ;;
  ;; Note: `:db/txInstant` is deliberately NOT in this set in
  ;; non-attribute-refs mode — adding it would land every tx's
  ;; `:db/txInstant` datom in AVET, a semantic change that breaks
  ;; existing tests + the existing `:db/txInstant`-by-tx lookup
  ;; path. See `non-ref-implicit-schema` in constants.cljc.
  (let [conn (fresh-conn)
        db   (d/db conn)
        rs   (datahike.db.interface/-rschema db)]
    (testing ":db.valid/from and :db.valid/to are recognised as indexed"
      (is (contains? (:db/index rs) :db.valid/from))
      (is (contains? (:db/index rs) :db.valid/to)))))

(deftest built-in-rules-work-without-in-percent
  ;; `valid-at` / `valid-between` / `valid-during` / `period-overlaps?`
  ;; should be invokable in `:where` without the user declaring `%` in
  ;; `:in`. The auto-inject inside `normalize-q-input` adds `%` and
  ;; the built-in rule defs transparently.
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :emp/salary
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:emp/name "Bob" :emp/salary 100000}])
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-07-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 120000}]
                      :tx-meta {:db.valid/from #inst "2024-07-01"}})
    (let [hist (d/history (d/db conn))]
      ;; NOTE: `d/history` returns BOTH added and retracted datoms. A
      ;; cardinality-one upsert (like the salary updates here) produces
      ;; one retraction + one assertion per tx. The valid-at filter
      ;; runs PER-TX (it filters the tx's vt-window, not per-datom
      ;; add/retract polarity), so a tx whose vt-window includes
      ;; ?at returns ALL its salary datoms — both the retracted and
      ;; the newly-asserted value. Use the 5-tuple `[e a v t added?]`
      ;; pattern to filter to additions only.
      (testing "valid-at — Bob's salary as of mid-April-2024"
        (let [r (d/q '[:find ?s :in $ ?at :where
                       (valid-at ?tx ?at)
                       [?e :emp/salary ?s ?tx true]]
                     hist #inst "2024-04-15")]
          (is (= #{[110000]} r))))
      (testing "valid-at — Bob's salary as of mid-August-2024"
        (let [r (d/q '[:find ?s :in $ ?at :where
                       (valid-at ?tx ?at)
                       [?e :emp/salary ?s ?tx true]]
                     hist #inst "2024-08-15")]
          (is (= #{[120000]} r))))
      (testing "valid-between — salaries with vt-window intersecting Q2 2024"
        (let [r (d/q '[:find ?s :in $ ?from ?to :where
                       (valid-between ?tx ?from ?to)
                       [?e :emp/salary ?s ?tx true]]
                     hist #inst "2024-04-01" #inst "2024-07-01")]
          (is (= #{[110000]} r))))
      (testing "user-supplied % overrides built-ins on name collision"
        ;; Define a no-op `valid-at` that matches every tx; built-in
        ;; should be shadowed and the result includes all salaries.
        (let [user-rules '[[(valid-at ?tx ?at) [?tx :db/txInstant _]]]
              r (d/q '[:find ?s :in $ % ?at :where
                       (valid-at ?tx ?at)
                       [?e :emp/salary ?s ?tx true]]
                     hist user-rules #inst "2024-04-15")]
          (is (= #{[100000] [110000] [120000]} r))))
      (testing "native < <= > >= work on Dates inside predicates"
        ;; Regression for the planner-pushdown-on-system-attrs bug.
        (let [r (d/q '[:find ?s :in $ ?at :where
                       [?tx :db.valid/from ?vf]
                       [(<= ?vf ?at)]
                       [?e :emp/salary ?s ?tx true]]
                     hist #inst "2024-08-15")]
          (is (= #{[110000] [120000]} r)))))))
