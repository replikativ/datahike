(ns datahike.test.fusion-test
  "Index-root fusion (EXPERIMENTAL, opt-in :fuse-index-roots?): inline each
   index's root node into the db-record and skip its separate konserve write.
   Covers: per-commit object-write reduction, cold-reopen exactness (incl.
   history), GC over fused records, crypto-hash audit, and connect-time
   adoption of the stored flag."
  (:require [datahike.api :as d]
            [datahike.audit :as audit]
            [datahike.gc :as gc]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]
            [clojure.test :refer [deftest is testing]]))

(defn- mk-cfg [label extra]
  (merge {:store {:backend :file
                  :path (str (System/getProperty "java.io.tmpdir") "/dh-fusion-test-" label)
                  :id (java.util.UUID/randomUUID)}
          :schema-flexibility :read :keep-history? true}
         extra))

(defn- count-keys [store] (count (k/keys store {:sync? true})))

(defn- build!
  "300 entities bulk, then 20 small update commits; returns net new objects for
   the 20 commits."
  [conn]
  (d/transact conn (vec (for [i (range 300)] {:db/id (inc i) :n i})))
  (let [before (count-keys (:store @conn))]
    (dotimes [i 20] (d/transact conn [{:db/id (inc i) :n (+ 1000 i)}]))
    (- (count-keys (:store @conn)) before)))

(defn- check-data [db]
  (and (= 300 (d/q '[:find (count ?e) . :where [?e :n _]] db))
       (= 20  (d/q '[:find (count ?e) . :where [?e :n ?v] [(>= ?v 1000)]] db))
       (= 320 (d/q '[:find (count ?v) . :with ?e :where [?e :n ?v]] (d/history db)))))

(deftest fusion-write-reduction-and-roundtrip
  (testing "fusion writes fewer objects per commit and cold-reopens exactly"
    (let [run (fn [extra]
                (let [cfg (mk-cfg (str "wr" (hash extra)) extra)]
                  (when (d/database-exists? cfg) (d/delete-database cfg))
                  (d/create-database cfg)
                  (let [conn (d/connect cfg)
                        growth (build! conn)]
                    (d/release conn)
                    (let [conn (d/connect cfg)
                          ok?  (check-data @conn)]
                      (d/release conn)
                      (d/delete-database cfg)
                      [growth ok?]))))
          [g-off ok-off] (run {})
          [g-on  ok-on]  (run {:fuse-index-roots? true})]
      (is ok-off "unfused store reopens exactly")
      (is ok-on "fused store reopens exactly (incl. history)")
      (is (< g-on g-off)
          (str "fusion writes fewer objects (" g-on " vs " g-off ")"))
      ;; at the default branching-factor every index is a single leaf root, so
      ;; fusion inlines the whole index: net growth = the commit-id record only
      (is (= 20 g-on) "single-leaf indexes fully inline: 1 object (the cid) per commit"))))

(deftest fusion-gc-and-crypto-audit
  (testing "GC walks fused records; crypto deep audit verifies the inlined root"
    (let [cfg (mk-cfg "gc-crypto" {:fuse-index-roots? true :crypto-hash? true
                                   :index-config {:diff-buf-size 64 :branching-factor 16}})]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (build! conn)
        (d/release conn))
      (let [conn (d/connect cfg)]
        (is (= :ok (:status (audit/verify-chain @conn nil {:deep? true})))
            "cold deep verify over fused+diff-buf store")
        ;; full-range GC: sweeps commits older than now except branch heads;
        ;; must walk the fused records (seeded roots) without raising
        (<?? S (gc/gc-storage! @conn (java.util.Date.)))
        (d/release conn))
      (let [conn (d/connect cfg)]
        (is (check-data @conn) "data exact after GC + cold reopen")
        ;; full-range GC sweeps old commit records (heads retained), so the
        ;; chain walk is legitimately :incomplete — the bytes-level deep
        ;; verification of the surviving head is what fusion must preserve
        (let [res (audit/verify-chain @conn nil {:deep? true})]
          (is (= :ok (get-in res [:deep :status]))
              "head deep verify still :ok after GC"))
        (d/release conn))
      (d/delete-database cfg))))

(deftest fusion-connect-adoption
  (testing ":fuse-index-roots? is adopted from the stored config on reconnect"
    (let [cfg (mk-cfg "adopt" {:fuse-index-roots? true})]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/id 1 :n 1}])
        (d/release conn))
      (let [conn (d/connect (dissoc cfg :fuse-index-roots?))]
        (is (true? (:fuse-index-roots? (:config @conn))) "stored flag adopted")
        (d/transact conn [{:db/id 2 :n 2}])
        (is (= 2 (d/q '[:find (count ?e) . :where [?e :n _]] @conn)))
        (d/release conn))
      (d/delete-database cfg))))
