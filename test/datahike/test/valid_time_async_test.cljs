(ns datahike.test.valid-time-async-test
  "Valid-time filters on the async engine: purifiable predicate layers
   (:overlap/:during always; :at over non-historical stacks) are rebuilt as
   PURE predicates from two batch-read AVET ranges at query entry
   (execute/prepare-vt-wrappers-step), so vt queries stream cold. valid-at
   over history stays a documented sync island (pinned to fault cleanly),
   as do opaque d/filter predicates."
  (:require [cljs.test :refer [is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [datahike.api :as d]
            [datahike.query :as q]
            [datahike.test.async :refer-macros [deftest-async]]
            [datahike.test.async-mode :as am]))

(defn- run-cold [query cold]
  (let [ch (a/promise-chan)]
    ((d/q {:query query :args [cold] :sync? false})
     (fn [v] (a/put! ch {:result v}))
     (fn [e] (a/put! ch (if (= :storage/sync-read-unavailable (:error (ex-data e)))
                          {:fault e}
                          {:error e}))))
    ch))

(deftest-async valid-time-async-cold
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read :keep-history? true}
        _ (<! (d/create-database cfg))
        conn (<! (d/connect cfg {:sync? false}))
        d2020 #inst "2020-01-01"
        d2021 #inst "2021-01-01"
        d2022 #inst "2022-01-01"
        d2023 #inst "2023-01-01"
        _ (<! (d/transact! conn {:tx-data [{:db/id 1 :vt-name "early"}]
                                 :tx-meta {:db.valid/from d2020 :db.valid/to d2021}}))
        _ (<! (d/transact! conn {:tx-data [{:db/id 2 :vt-name "late"}]
                                 :tx-meta {:db.valid/from d2022 :db.valid/to d2023}}))
        _ (<! (d/transact! conn [{:db/id 3 :vt-name "timeless"}]))
        db @conn
        handle (am/flush-db! db)
        query '[:find ?n :where [?e :vt-name ?n]]
        sync-q (fn [wrapped] (binding [q/*query-result-cache?* false]
                               (d/q query wrapped)))]
    (testing "valid-at (current view) cold ≡ sync"
      (doseq [at [#inst "2020-06-01" #inst "2022-06-01" #inst "2024-01-01"]]
        (let [sync-r (sync-q (d/valid-at db at))
              cold (d/valid-at (am/cold-db db handle) at)
              {:keys [result error fault]} (<! (run-cold query cold))]
          (is (and (nil? error) (nil? fault))
              (str "valid-at " at ": " (some-> (or error fault) ex-message)))
          (is (= sync-r result) (str "valid-at " at " cold ≡ sync")))))
    (testing "valid-between cold ≡ sync"
      (let [sync-r (sync-q (d/valid-between db d2020 d2022))
            cold (d/valid-between (am/cold-db db handle) d2020 d2022)
            {:keys [result error fault]} (<! (run-cold query cold))]
        (is (and (nil? error) (nil? fault))
            (str "valid-between: " (some-> (or error fault) ex-message)))
        (is (= sync-r result) "valid-between cold ≡ sync")))
    (testing "valid-during cold ≡ sync"
      (let [sync-r (sync-q (d/valid-during db d2020 d2023))
            cold (d/valid-during (am/cold-db db handle) d2020 d2023)
            {:keys [result error fault]} (<! (run-cold query cold))]
        (is (and (nil? error) (nil? fault))
            (str "valid-during: " (some-> (or error fault) ex-message)))
        (is (= sync-r result) "valid-during cold ≡ sync")))
    (testing "valid-between over history cold ≡ sync (purifiable regardless)"
      (let [sync-r (sync-q (d/valid-between (d/history db) d2020 d2022))
            cold (d/valid-between (d/history (am/cold-db db handle)) d2020 d2022)
            {:keys [result error fault]} (<! (run-cold query cold))]
        (is (and (nil? error) (nil? fault))
            (str "between∘history: " (some-> (or error fault) ex-message)))
        (is (= sync-r result) "valid-between over history cold ≡ sync")))
    (testing "valid-at over HISTORY is the documented phase-2 sync island: clean fault"
      (let [cold (d/valid-at (d/history (am/cold-db db handle)) d2020)
            {:keys [fault result error]} (<! (run-cold query cold))]
        (is (nil? error) (str "unexpected error: " (some-> error ex-message)))
        (when result
          ;; if it ever completes, it must agree — that would mean phase 2 landed
          (is (= (sync-q (d/valid-at (d/history db) d2020)) result)))
        (when-not result
          (is (some? fault) "valid-at over history must fault cleanly, not error"))))
    (testing "opaque d/filter predicates are untouched: clean fault on cold"
      (let [cold (d/filter (am/cold-db db handle)
                           (fn [fdb datom] (odd? (:e datom))))
            sync-r (sync-q (d/filter db (fn [fdb datom] (odd? (:e datom)))))
            {:keys [result fault error]} (<! (run-cold query cold))]
        (is (nil? error) (str "unexpected error: " (some-> error ex-message)))
        ;; a pure (non-db-reading) opaque pred actually WORKS via the fused
        ;; fpred path — assert agreement when it completes, clean fault else
        (when result (is (= sync-r result)))
        (when-not result (is (some? fault)))))))
