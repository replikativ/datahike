(ns datahike.test.valid-time-test
  "Tests for the bitemporal valid-time tx-meta attributes that graduated
   into the system schema:

   - `:db.valid/from` and `:db.valid/to` are pre-installed system attrs.
   - Consumers can attach them to the tx entity via the standard
     `{:db/id \"datomic.tx\" :db.valid/from #inst ... :db.valid/to #inst ...}`
     tx-meta map — no schema declaration required.
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
      (is (some? (d/transact conn [{:db/id "datomic.tx"
                                    :db.valid/from #inst "2024-01-01"
                                    :db.valid/to   #inst "2024-07-01"}
                                   {:pos/x 1}]))))))

(deftest valid-time-tx-meta-lands-on-the-tx-entity
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :pos/x
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (let [report (d/transact conn
                              [{:db/id "datomic.tx"
                                :db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-07-01"}
                               {:pos/x 42}])
          tx    (get-in report [:tempids "datomic.tx"])
          db    (d/db conn)
          pulled (d/pull db '[*] tx)]
      (testing "the tempid `datomic.tx` resolves and the tx-meta attrs land"
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
      (d/transact conn [{:db/id "datomic.tx"
                         :db.valid/from (java.util.Date.
                                         (+ 1700000000000 (* i 86400000)))}
                        {:pos/x i}]))
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
                              [{:db/id "datomic.tx"
                                :db.valid/from #inst "2024-01-01"}
                               {:pos/x 1}])
          tx    (get-in report [:tempids "datomic.tx"])
          pulled (d/pull (d/db conn) '[*] tx)]
      (testing "vt-from alone is sufficient; vt-to may be omitted"
        (is (= #inst "2024-01-01" (:db.valid/from pulled)))
        (is (nil? (:db.valid/to pulled)))))))
