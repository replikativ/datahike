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
            [datahike.migrate.fs :as fs]
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
              :select [:eid
                       :_valid_from :_valid_to
                       :_system_from :_system_to
                       :name :salary]})))

(defn- error-data [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- at-request [at]
  {:system {:mode :current}
   :valid {:mode :at :at at}})

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
                                    (at-request #inst "2024-04-15"))]
          (is (not (.isEmpty bs)))))
      (testing "valid-at #inst 2024-09-15 → only tx2's row matches"
        (let [bs (sec/-search-at-vt idx
                                    {:where [[:= :salary 110000]]}
                                    nil
                                    (at-request #inst "2024-09-15"))]
          (is (not (.isEmpty bs))))))))

(deftest point-valid-time-order-and-candidate-page-filter-before-limit
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from #inst "2024-01-01"
                                :db.valid/to #inst "2024-07-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db.valid/from #inst "2024-07-01"}})
    (let [db (d/valid-at (d/db conn) #inst "2024-04-15")
          idx (-> (d/db conn) :secondary-indices :idx/employees)
          query-spec {:attribute :emp/salary
                      :direction :asc
                      :where []}
          ordered (sec/slice-ordered-with-vt
                   db idx query-spec nil :emp/salary :asc 1)
          page (sec/candidate-page
                db :idx/employees idx query-spec nil {:limit 1})]
      (is (= [100000] (mapv :value ordered)))
      (is (= [100000] (mapv :value (:candidates page))))
      (is (:exhausted? page)))))

(deftest secondary-value-keeps-a-finite-current-valid-window
  (let [conn (fresh-conn)]
    (register-vt-index! conn)
    (let [eid (get-in
               (d/transact conn
                           {:tx-data [{:db/id -1
                                       :emp/name "Bob"
                                       :emp/salary 100000}]
                            :tx-meta {:db.valid/from #inst "2024-01-01"
                                      :db.valid/to #inst "2024-07-01"}})
               [:tempids -1])
          idx (-> (d/db conn) :secondary-indices :idx/employees)]
      (is (= 100000 (sec/-sec-value idx :emp/salary eid))))))

(deftest populated-valid-time-build-refuses-invented-history
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/salary
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:emp/salary 100000}])
    (let [failure (error-data
                   #(sec/create-index :stratum
                                      {:attrs #{:emp/salary}
                                       :valid-time true}
                                      (d/db conn)))]
      (is (= :secondary/stratum-valid-time-backfill-required
             (:type failure)))
      (is (= 1 (:row-count failure))))))

(deftest schema-registration-refuses-current-value-vt-backfill
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/salary
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:emp/salary 100000}])
    (let [failure (error-data
                   #(d/transact
                     conn
                     [{:db/ident :idx/employees
                       :db.secondary/type :stratum
                       :db.secondary/attrs [:emp/salary]
                       :db.secondary/config {:valid-time true}}]))]
      (is (= :secondary/transaction-history-backfill-required
             (:type failure)))
      (is (nil? (get-in (d/schema (d/db conn))
                        [:idx/employees :db.secondary/status])))
      (is (nil? (get-in (d/db conn)
                        [:secondary-indices :idx/employees]))
          "the failed transaction publishes neither :building nor an index"))))

(deftest secondary-system-axis-uses-the-primary-transaction-instants
  (let [conn (fresh-conn)
        first-instant #inst "2030-01-01T00:00:00.100Z"
        second-instant #inst "2030-01-01T00:00:00.101Z"]
    (register-vt-index! conn)
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db/txInstant first-instant
                                :db.valid/from #inst "2024-01-01"}})
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                      :tx-meta {:db/txInstant second-instant
                                :db.valid/from #inst "2024-07-01"}})
    (let [rows (vt-rows conn :idx/employees)
          old-row (first (filter #(= 100000 (:salary %)) rows))
          new-row (first (filter #(= 110000 (:salary %)) rows))]
      (is (= 1893456000100000 (:_system_from old-row)))
      (is (= 1893456000101000 (:_system_to old-row)))
      (is (= 1893456000101000 (:_system_from new-row)))
      (is (= Long/MAX_VALUE (:_system_to new-row))))))

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
    (testing "an ordinary Stratum generation declines native valid-time search"
      (is (not (sec/vt-aware?
                (-> (d/db conn) :secondary-indices :idx/employees-plain)))))
    (testing "no :bitemporal in metadata → no vt-config exposed"
      (is (nil? (:bitemporal (:metadata ds)))))
    (testing "no _valid_from / _valid_to columns"
      (is (not (contains? (set (keys (st/columns ds))) :_valid_from)))
      (is (not (contains? (set (keys (st/columns ds))) :_valid_to))))))

;; ============================================================================
;; Versioning — release/reconnect + branch round-trip preserve SCD2 layout
;;
;; The adapter implements the immutable durable-generation lifecycle
;; (`-sec-prepare` seals, `-sec-restore` opens the exact generation).
;; Stratum commit 1 (feature/valid-time) made `:metadata {:valid-time
;; ...}` round-trip through seal/open; these tests confirm the
;; integration end-to-end through the datahike write/read path.

(defn- file-cfg []
  {:store {:backend :file
           :id (java.util.UUID/randomUUID)
           :path (fs/temp-store-path! "datahike-stratum-vt-test-")}
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
      (testing "an unqualified query declines the audit representation"
        (let [plain-error (error-data
                           #(sec/search-with-vt db idx
                                                {:where [[:= :salary 100000]]}
                                                nil))
              bs-marker (sec/search-with-vt
                         (d/valid-at db #inst "2024-09-15")  ;; after the closed window
                         idx
                         {:where [[:= :salary 100000]]}
                         nil)]
          (is (= :secondary/stratum-temporal-view-unsupported
                 (:type plain-error)))
          (is (= :primary-current-row-not-represented
                 (:reason plain-error)))
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
    (let [idx (-> (d/db conn) :secondary-indices :idx/employees)
          ds (index-dataset conn :idx/employees)
          rows (vec (st/q {:from ds
                           :select [:eid :salary :_valid_from :_valid_to
                                    :_system_from :_system_to]}))]
      ;; THREE rows, and each answers a different question. This is a
      ;; CORRECTION — tx2 declares no `:db.valid/to`, so it revises a belief
      ;; that was recorded as open-ended — and a correction must not rewrite
      ;; what the database believed BEFORE it arrived.
      ;;
      ;; The superseded row therefore keeps its original valid window and only
      ;; its `_system_to` closes; the corrected history is a NEW row. Closing
      ;; both windows on the original, as this used to, leaves no row saying
      ;; "on 2024-07-01 we believed 100000 was valid indefinitely" — so
      ;; `FOR SYSTEM_TIME AS OF 2024-07-01` would answer with knowledge that
      ;; did not exist until 2024-08-01, which is the one thing a system axis
      ;; exists to prevent.
      (let [tx2-instant (* 1000 (.getTime #inst "2024-08-01T00:00:00Z"))
            sys-open?   #(= Long/MAX_VALUE (:_system_to %))
            preserved   (first (remove sys-open? rows))
            current     (first (filter #(and (sys-open? %)
                                             (= Long/MAX_VALUE (:_valid_to %)))
                                       rows))
            corrected   (first (filter #(and (sys-open? %)
                                             (not= Long/MAX_VALUE (:_valid_to %)))
                                       rows))]
        (testing "three rows after the SCD2 correction"
          (is (= 3 (count rows))))
        (testing "the pre-correction belief is PRESERVED — system-closed at the
                  correcting instant, valid window untouched"
          (is (some? preserved))
          (is (= tx2-instant (:_system_to preserved))
              "superseded row's _system_to should equal the correcting tx's txInstant")
          (is (= Long/MAX_VALUE (:_valid_to preserved))
              "and its valid window is NOT rewritten — that is what makes
               FOR SYSTEM_TIME AS OF <before the correction> answerable")
          (is (= 100000 (:salary preserved))))
        (testing "corrected history: the old salary, now bounded, believed since tx2"
          (is (some? corrected))
          (is (= 100000 (:salary corrected)))
          (is (= tx2-instant (:_system_from corrected))))
        (testing "current: the new salary, open on both axes"
          (is (some? current))
          (is (= 110000 (:salary current)))
          (is (= tx2-instant (:_system_from current)))
          (is (= Long/MAX_VALUE (:_system_to current)))))
      (testing "native valid-at selects the current system belief before matching"
        (is (.isEmpty
             (sec/search-with-vt
              (d/valid-at (d/db conn) #inst "2024-09-01")
              idx {:where [[:= :salary 100000]]} nil))
            "the preserved pre-correction row is system-closed")
        (is (not (.isEmpty
                  (sec/search-with-vt
                   (d/valid-at (d/db conn) #inst "2024-09-01")
                   idx {:where [[:= :salary 110000]]} nil))))))))
