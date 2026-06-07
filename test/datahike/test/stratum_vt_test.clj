(ns datahike.test.stratum-vt-test
  "Tests for the stratum secondary-index adapter's `:valid-time` (SCD2) mode.

   When the index config declares `:valid-time true`, the adapter:

   - Materialises `:_valid_from` / `:_valid_to` columns on every row,
     populated from the writing tx's `:db.valid/from` / `:db.valid/to`
     tx-meta (falling back to `:db/txInstant` for non-vt-bearing txes).
   - On entity update, the previous open row's `:_valid_to` is closed
     to the new tx's vt-from, and a new row is appended carrying the
     merged-with-previous attribute values plus the new vt-window.
   - `IValidTimeAware/-search-at-vt` translates `valid-at` / window-
     overlap into stratum WHERE predicates on the two vt columns.

   No Thread/sleep is needed: `instantiate-secondary` auto-detects
   whether AEVT has any datoms for the indexed attrs at registration
   time. When the index is registered on an empty (or empty-for-
   these-attrs) DB, status is set to `:ready` directly and no async
   `build-secondary-index!` dispatch fires — eliminating the race
   between the async backfill and subsequent user writes."
  (:require [clojure.test :as t :refer [is deftest testing]]
            [datahike.api :as d]
            [datahike.index.secondary :as sec]
            [datahike.index.secondary.stratum]
            [datahike.versioning :as dv]
            [stratum.api :as st]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- index-dataset [conn idx-ident]
  (.-dataset ^datahike.index.secondary.stratum.StratumIndex
   (-> (d/db conn) :secondary-indices idx-ident)))

(defn- vt-rows [conn idx-ident]
  (vec (st/q {:from (index-dataset conn idx-ident)
              :select [:eid :_valid_from :_valid_to :name :salary]})))

(defn- register-vt-index! [conn]
  (d/transact conn [{:db/ident :emp/name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one
                     :db/unique :db.unique/identity}
                    {:db/ident :emp/salary
                     :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :idx/employees
                     :db.secondary/type :stratum
                     :db.secondary/attrs [:emp/name :emp/salary]
                     :db.secondary/config {:valid-time true}
                     :db.secondary/status :ready}]))

;; ============================================================================
;; Dataset shape — vt config wires through

(deftest vt-mode-flag-creates-vt-columns
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (let [ds (index-dataset conn :idx/employees)]
      (testing "vt cols exist on the empty initial dataset"
        (is (contains? (set (keys (st/columns ds))) :_valid_from))
        (is (contains? (set (keys (st/columns ds))) :_valid_to)))
      (testing "metadata round-trips the valid-axis config"
        ;; Stratum's bitemporal config carries both axes by default —
        ;; we assert the valid axis specifically.
        (is (= {:from-col :_valid_from :to-col :_valid_to :unit :micros}
               (get-in (:metadata ds) [:bitemporal :valid]))))
      (testing "system-time axis is present for SCD2 audit symmetry"
        (is (= {:from-col :_system_from :to-col :_system_to :unit :micros}
               (get-in (:metadata ds) [:bitemporal :system])))))))

;; ============================================================================
;; SCD2 layout — close-on-upsert

(deftest scd2-upsert-closes-old-row-and-opens-new
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-07-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db.valid/from #inst "2024-07-01"}})
    (let [rows (vt-rows conn :idx/employees)]
      (testing "two rows: the closed tx1 row + the open tx2 row"
        (is (= 2 (count rows))))
      (testing "tx1's row is closed at tx2's :db.valid/from"
        (let [tx1 (first (filter #(= 100000 (:salary %)) rows))]
          (is (= 1704067200000000 (:_valid_from tx1))) ;; 2024-01-01
          (is (= 1719792000000000 (:_valid_to   tx1))) ;; 2024-07-01
          (is (= "Bob" (:name tx1)))))
      (testing "tx2's row carries the new salary + open vt-to (MAX_VALUE)"
        (let [tx2 (first (filter #(= 110000 (:salary %)) rows))]
          (is (= 1719792000000000 (:_valid_from tx2))) ;; 2024-07-01
          (is (= Long/MAX_VALUE    (:_valid_to   tx2)))
          (is (= "Bob" (:name tx2))))))))

;; ============================================================================
;; IValidTimeAware — search-at-vt

(deftest search-at-vt-returns-correct-as-of-eid
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-07-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db.valid/from #inst "2024-07-01"}})
    (let [idx (-> (d/db conn) :secondary-indices :idx/employees)]
      (testing "vt-aware?: the StratumIndex implements IValidTimeAware"
        (is (sec/vt-aware? idx)))
      (testing "valid-at #inst 2024-04-15 → only tx1's row matches"
        (let [bs (sec/-search-at-vt idx
                                    {:where [[:= :salary 100000]]}
                                    nil
                                    #inst "2024-04-15")]
          (is (not (.isEmpty bs)))))
      (testing "valid-at #inst 2024-09-15 → only tx2's row matches"
        (let [bs (sec/-search-at-vt idx
                                    {:where [[:= :salary 110000]]}
                                    nil
                                    #inst "2024-09-15")]
          (is (not (.isEmpty bs))))))))

;; ============================================================================
;; vt-mode off — adapter still works (regression / parity check)

(deftest non-vt-config-skips-vt-columns
  (let [conn (fresh-conn)
        _ (d/transact conn [{:db/ident :emp/salary
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :idx/employees-plain
                             :db.secondary/type :stratum
                             :db.secondary/attrs [:emp/salary]
                             :db.secondary/config {}
                             :db.secondary/status :ready}])
        ds (index-dataset conn :idx/employees-plain)]
    (testing "no :bitemporal in metadata → no vt-config exposed"
      (is (nil? (:bitemporal (:metadata ds)))))
    (testing "no _valid_from / _valid_to columns"
      (is (not (contains? (set (keys (st/columns ds))) :_valid_from)))
      (is (not (contains? (set (keys (st/columns ds))) :_valid_to))))))

;; ============================================================================
;; Versioning — release/reconnect + branch round-trip preserve SCD2 layout
;;
;; The adapter implements IVersionedSecondaryIndex (-sec-flush calls
;; `stratum.dataset/sync!`, -sec-restore calls `stratum.dataset/load`).
;; Stratum commit 1 (feature/valid-time) made `:metadata {:valid-time
;; ...}` round-trip through sync!/load; these tests confirm the
;; integration end-to-end through the datahike write/read path.

(defn- file-cfg []
  {:store {:backend :file
           :id (java.util.UUID/randomUUID)
           :path (str "/tmp/datahike-stratum-vt-test-" (random-uuid))}
   :keep-history? true
   :schema-flexibility :write})

(deftest vt-mode-survives-release-and-reconnect
  (let [cfg (file-cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (register-vt-index! conn)
      (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                        :tx-meta {:db.valid/from #inst "2024-01-01"
                                  :db.valid/to   #inst "2024-07-01"}})
      (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                        :tx-meta {:db.valid/from #inst "2024-07-01"}})
      (let [pre-rows (vt-rows conn :idx/employees)]
        (testing "two rows present before release"
          (is (= 2 (count pre-rows))))
        (d/release conn)
        (let [conn2 (d/connect cfg)]
          (try
            (let [ds (index-dataset conn2 :idx/employees)
                  post-rows (vt-rows conn2 :idx/employees)]
              (testing "vt metadata round-trips through konserve"
                (is (= {:from-col :_valid_from :to-col :_valid_to :unit :micros}
                       (get-in (:metadata ds) [:bitemporal :valid]))))
              (testing "vt columns are tagged :micros on restore"
                (is (= :micros (:temporal-unit (get (st/columns ds) :_valid_from))))
                (is (= :micros (:temporal-unit (get (st/columns ds) :_valid_to)))))
              (testing "SCD2 row set is identical after reconnect"
                (is (= (set pre-rows) (set post-rows)))))
            (finally
              (d/release conn2)))))
      (finally
        (d/delete-database cfg)))))

;; ============================================================================
;; d/valid-at marker + search-with-vt routing

(deftest valid-at-marker-routes-to-search-at-vt
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to   #inst "2024-07-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db.valid/from #inst "2024-07-01"}})
    (let [db (d/db conn)
          idx (-> db :secondary-indices :idx/employees)]
      (testing "valid-at marker lands on the db's metadata"
        (let [marked (d/valid-at db #inst "2024-04-15")]
          (is (= #inst "2024-04-15"
                 (:datahike/valid-at (meta marked))))))
      (testing "valid-at nil clears the marker"
        (let [marked (d/valid-at db #inst "2024-04-15")
              cleared (d/valid-at marked nil)]
          (is (nil? (:datahike/valid-at (meta cleared))))))
      (testing "search-with-vt routes through -search-at-vt when marker is set + index is vt-aware"
        (let [marked (d/valid-at db #inst "2024-04-15")
              bs (sec/search-with-vt marked idx
                                     {:where [[:= :salary 100000]]}
                                     nil)]
          (is (not (.isEmpty bs)))))
      (testing "search-with-vt without marker → plain -search (no vt filter applied)"
        (let [bs-no-marker (sec/search-with-vt db idx
                                               {:where [[:= :salary 100000]]}
                                               nil)
              bs-marker (sec/search-with-vt
                         (d/valid-at db #inst "2024-09-15")  ;; after the closed window
                         idx
                         {:where [[:= :salary 100000]]}
                         nil)]
          ;; Both queries ask for salary=100000. Without marker → returns
          ;; entity 4 (the row with salary 100k exists in the dataset).
          ;; With marker at 2024-09-15 → that row's window is [2024-01-01,
          ;; 2024-07-01), which doesn't contain 2024-09-15 → no match.
          (is (not (.isEmpty bs-no-marker)) "without marker, plain search finds salary=100k")
          (is (.isEmpty bs-marker) "with valid-at after the window, vt-pushdown filters the row out"))))))

(deftest vt-mode-survives-branch
  (let [cfg (file-cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (register-vt-index! conn)
      (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                        :tx-meta {:db.valid/from #inst "2024-01-01"
                                  :db.valid/to   #inst "2024-07-01"}})
      (let [main-rows (vt-rows conn :idx/employees)]
        (testing "main has one row"
          (is (= 1 (count main-rows))))
        (dv/branch! conn :db :feature)
        (let [feat-conn (d/connect (assoc cfg :branch :feature))]
          (try
            (testing "feature branch inherits vt-mode metadata"
              (let [ds (index-dataset feat-conn :idx/employees)]
                (is (= {:from-col :_valid_from :to-col :_valid_to :unit :micros}
                       (get-in (:metadata ds) [:bitemporal :valid])))))
            (testing "feature branch sees the same SCD2 rows"
              (is (= (set main-rows)
                     (set (vt-rows feat-conn :idx/employees)))))
            (testing "writing to feature branch keeps main unchanged"
              (d/transact feat-conn
                          {:tx-data [{:emp/name "Bob" :emp/salary 200000}]
                           :tx-meta {:db.valid/from #inst "2024-07-01"}})
              (is (= 2 (count (vt-rows feat-conn :idx/employees))))
              (is (= 1 (count (vt-rows conn :idx/employees)))
                  "main branch should still show only the original row"))
            (finally
              (d/release feat-conn)))))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

;; ============================================================================
;; System-time symmetry on SCD2 surgery (DH-5 / Phase E)
;;
;; When the vt-mode adapter closes an old row's `_valid_to`, it must
;; also close that row's `_system_to` to the current tx's instant, so
;; a `FOR SYSTEM_TIME AS OF <pre-correction>` query still sees the
;; row as "open at the time the DB knew it." Without this, backdated
;; corrections silently rewrite past system-time views — the very
;; bug stratum's P0-1 fixed at the dataset layer.
;; ============================================================================

(deftest scd2-closes-system-to-on-old-row
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (d/transact conn {:tx-meta {:db/txInstant #inst "2024-06-01T00:00:00Z"
                                :db.valid/from #inst "2024-01-01"}
                      :tx-data [{:emp/name "Bob" :emp/salary 100000}]})
    (d/transact conn {:tx-meta {:db/txInstant #inst "2024-08-01T00:00:00Z"
                                :db.valid/from #inst "2024-07-01"}
                      :tx-data [{:emp/name "Bob" :emp/salary 110000}]})
    (let [ds (index-dataset conn :idx/employees)
          rows (vec (st/q {:from ds
                           :select [:eid :salary :_valid_from :_valid_to
                                    :_system_from :_system_to]}))]
      (testing "two rows present after the SCD2 update"
        (is (= 2 (count rows))))
      (let [closed (first (filter #(not= Long/MAX_VALUE (:_valid_to %)) rows))
            new-row (first (filter #(= Long/MAX_VALUE (:_valid_to %)) rows))]
        (testing "closed row's _system_to advanced to second tx's instant"
          (is (some? closed))
          (is (= (* 1000 (.getTime #inst "2024-08-01T00:00:00Z"))
                 (:_system_to closed))
              "closed row's _system_to should equal the correcting tx's txInstant"))
        (testing "new row's _system_from is the correcting tx's instant"
          (is (some? new-row))
          (is (= (* 1000 (.getTime #inst "2024-08-01T00:00:00Z"))
                 (:_system_from new-row))
              "new row's _system_from should equal the tx's txInstant"))
        (testing "new row's _system_to is open (MAX_VALUE)"
          (is (= Long/MAX_VALUE (:_system_to new-row))))))))
