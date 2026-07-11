(ns datahike.test.commit-graph-test
  "Commit graph opt-out (:commit-graph? false): every commit normally persists
   an immutable record under its cid — the provenance chain consumed by audit,
   ancestry walks, branch!-from-cid and dh://…?commit= references. Stores that
   need none of these skip the record: commits write only the branch head, so
   the dominant per-commit garbage object disappears and buffered fused
   commits become a single PUT."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.gc :refer [gc-storage!]]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

(defn- base-cfg [id]
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir") "/dh-commit-graph-" id)
           :id id}
   :schema-flexibility :write
   :keep-history? false
   :commit-graph? false})

(def schema-tx
  [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(deftest no-commit-records-persisted
  (testing "cids are stamped in meta but never written as store objects"
    (let [cfg (base-cfg #uuid "c0313000-0000-0000-0000-000000000001")]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn schema-tx)
        (dotimes [i 20] (d/transact conn [{:id (long (mod i 5)) :score (long i)}]))
        (let [db @conn
              store (:store db)
              cid (get-in db [:meta :datahike/commit-id])
              parent (first (get-in db [:meta :datahike/parents]))]
          (is (uuid? cid) "cid identity is still computed and stamped")
          (is (uuid? parent) "one-step lineage is still stamped")
          (is (nil? (k/get store cid nil {:sync? true}))
              "no record persisted under the head cid")
          (is (nil? (k/get store parent nil {:sync? true}))
              "no record persisted under the parent cid")
          (is (= cid (get-in (k/get store :db nil {:sync? true})
                             [:meta :datahike/commit-id]))
              "branch head carries the cid"))
        (is (= 5 (d/q '[:find (count ?e) . :where [?e :id _]] @conn)))
        (d/release conn))
      (d/delete-database cfg))))

(deftest gc-tolerates-absent-commit-records
  (testing "gc-storage! walks lineage that ends at absent records without NPE"
    (let [cfg (base-cfg #uuid "c0313000-0000-0000-0000-000000000002")]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn schema-tx)
        (dotimes [i 30] (d/transact conn [{:id (long i) :score (long i)}]))
        (let [swept (<?? S (gc-storage! @conn (java.util.Date.)))]
          (is (set? swept)))
        ;; a second pass converges with nothing left to sweep but freed leftovers
        (<?? S (gc-storage! @conn (java.util.Date.)))
        (is (= 30 (d/q '[:find (count ?e) . :where [?e :id _]] @conn))
            "data intact after gc")
        (d/release conn))
      ;; reopen after gc: stored->db from the branch head alone
      (let [conn (d/connect (dissoc cfg :commit-graph?))]
        (is (= 30 (d/q '[:find (count ?e) . :where [?e :id _]] @conn)))
        (d/release conn))
      (d/delete-database cfg))))

(deftest branching-from-branch-works-from-cid-errors
  (let [cfg (base-cfg #uuid "c0313000-0000-0000-0000-000000000003")]
    (when (d/database-exists? cfg) (d/delete-database cfg))
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn schema-tx)
      (d/transact conn [{:id 1 :score 1}])
      (testing "branch! from a branch keyword works without commit records"
        (v/branch! conn :db :fork)
        (let [fork-conn (d/connect (assoc cfg :branch :fork))]
          (is (= 1 (d/q '[:find (count ?e) . :where [?e :id _]] @fork-conn)))
          (d/transact fork-conn [{:id 2 :score 2}])
          (is (= 2 (d/q '[:find (count ?e) . :where [?e :id _]] @fork-conn)))
          (is (= 1 (d/q '[:find (count ?e) . :where [?e :id _]] @conn))
              "main branch unaffected by fork writes")
          (d/release fork-conn)))
      (testing "branch! from a cid fails with a self-explaining error"
        (let [cid (get-in @conn [:meta :datahike/commit-id])
              e (try (v/branch! conn cid :from-cid) nil
                     (catch Exception e e))]
          (is (some? e))
          (is (re-find #"commit-graph\? false" (.getMessage e)))
          (is (false? (:commit-graph? (ex-data e))))))
      (d/release conn))
    (d/delete-database cfg)))

(deftest connect-adopts-and-guards-the-flag
  (let [cfg (base-cfg #uuid "c0313000-0000-0000-0000-000000000004")]
    (when (d/database-exists? cfg) (d/delete-database cfg))
    (d/create-database cfg)
    (testing "reconnect without the flag adopts it from the store"
      (let [conn (d/connect (dissoc cfg :commit-graph?))]
        (is (false? (:commit-graph? (:config @conn))))
        (d/transact conn schema-tx)
        (d/transact conn [{:id 1 :score 1}])
        (is (nil? (k/get (:store @conn) (get-in @conn [:meta :datahike/commit-id]) nil {:sync? true}))
            "adopted flag governs commits")
        (d/release conn)))
    (testing "explicitly conflicting value raises"
      (is (thrown-with-msg? Exception #"Create-time-fixed"
                            (d/connect (assoc cfg :commit-graph? true)))))
    (d/delete-database cfg)))

(deftest crypto-hash-combination-rejected
  (testing "crypto-hash auditing requires the commit chain"
    (let [cfg (assoc (base-cfg #uuid "c0313000-0000-0000-0000-000000000005")
                     :crypto-hash? true)]
      (d/delete-database cfg)
      (is (thrown-with-msg? Exception #"cannot be combined"
                            (d/create-database cfg)))
      ;; validation fires after backing-store creation (pre-existing behavior
      ;; for all invalid configs) — clean the empty store dir up
      (d/delete-database cfg))))
