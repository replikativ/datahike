(ns datahike.test.back-correction-test
  "Regression for `d/valid-at` back-correction semantics — the
   supersession contract under two overlapping vt-windowed assertions
   for the same (e, a). When tx-2's vt-window covers `at` AND has a
   greater tx-id than tx-1, tx-2's assertion supersedes tx-1's — the
   bitemporal supersession at the read layer (see
   `api/impl.cljc:269-309` for the design rationale). This test pins
   the four cases that distinguish supersession-correct from
   single-axis-naive: current-view, pre-correction history,
   post-correction history, and the supersession winner."
  (:require [clojure.test :as t :refer [is deftest testing]]
            [datahike.api :as d]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(deftest valid-at-back-correction-supersession
  ;; Scenario:
  ;;   tx-1 (vt=[Jan-01, ∞)): assert Bob.salary = 100k
  ;;   tx-2 (vt=[Apr-01, ∞)): assert Bob.salary = 90k  (back-correction)
  ;;
  ;; bitemporal supersession at the read layer:
  ;;   query at vt=Feb-15 → 100k (pre-correction era, tx-2 excluded by vt)
  ;;   query at vt=May-15 →  90k (post-correction era, tx-2 supersedes)
  ;;
  ;; The supersession-correct `d/valid-at` implementation must produce
  ;; the polygon shape on both branches.
  (let [conn (fresh-conn)
        _ (d/transact conn [{:db/ident :emp/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}
                            {:db/ident :emp/salary
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one
                             :db/index true}])
        _ (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                            :tx-meta {:db.valid/from #inst "2024-01-01"}})
        _ (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 90000}]
                            :tx-meta {:db.valid/from #inst "2024-04-01"}})
        db   @conn
        hist (d/history db)]
    (testing "current-time only sees the latest assertion (90k)"
      (is (= #{[90000]}
             (d/q '[:find ?s :where [_ :emp/salary ?s]] db))))
    (testing "current-time + valid-at Feb-15 → empty (tx-2's vt excludes Feb-15; tx-1's 100k is retracted in current view)"
      (is (= #{}
             (d/q '[:find ?s :where [_ :emp/salary ?s]]
                  (d/valid-at db #inst "2024-02-15")))))
    (testing "history + valid-at Feb-15 → returns {100k}"
      (is (= #{[100000]}
             (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]]
                  (d/valid-at hist #inst "2024-02-15"))))
      "tx-1 vt=[Jan, ∞) covers Feb-15 → 100k admitted; tx-2 vt=[Apr, ∞) excluded")
    (testing "history + valid-at May-15 → {90k} via supersession"
      ;; Both tx-1's vt=[Jan, ∞) and tx-2's vt=[Apr, ∞) cover May-15.
      ;; Supersession picks the latest tx for each (e, a, v):
      ;;   - (bob, :emp/salary, 100k) → winner = tx-2 (retraction). Reject the assertion datom.
      ;;   - (bob, :emp/salary,  90k) → winner = tx-2 (assertion). Admit.
      (let [result (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]]
                        (d/valid-at hist #inst "2024-05-15"))]
        (is (= #{[90000]} result)
            "supersession-aware predicate picks tx-2's 90k over tx-1's 100k")))))
