(ns datahike.test.head-cache-test
  "The writer must not re-read its own branch head from storage on every
   commit: under the single-writer invariant the head commit-id is already in
   memory ([:meta :datahike/commit-id], stamped by the previous commit! or by
   stored->db at connect). On S3-class backends that read was 3 sequential
   requests (~150 ms at 40 ms RTT) — the dominant cost of a fused commit."
  (:require [datahike.api :as d]
            [konserve.core :as k]
            [clojure.test :refer [deftest is testing]]))

(deftest no-branch-head-read-per-commit
  (testing "steady-state commits do zero k/get reads of the branch-head key"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-head-cache")
                       :id #uuid "6ead0000-0000-0000-0000-000000000001"}
               :schema-flexibility :write :keep-history? false}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:id 1 :score 0}])   ; warm: writer meta now carries a cid
        (let [head-reads (atom 0)
              orig k/get]
          (with-redefs [k/get (fn [store key & args]
                                (when (= key (:branch (:config @conn)))
                                  (swap! head-reads inc))
                                (apply orig store key args))]
            (dotimes [i 10] (d/transact conn [{:id 1 :score (long (inc i))}])))
          (is (zero? @head-reads)
              (str "commits re-read the branch head " @head-reads " times")))
        ;; parent chain stays correct without the read
        (let [db @conn
              store (:store db)
              head (k/get store :db nil {:sync? true})
              parent (first (get-in head [:meta :datahike/parents]))
              parent-rec (k/get store parent nil {:sync? true})]
          (is (some? parent-rec) "parent cid recorded from memory resolves in the commit graph")
          (is (= (get-in parent-rec [:meta :datahike/commit-id]) parent)
              "parent record's own cid matches the recorded parent"))
        (is (= 10 (d/q '[:find ?s . :where [?e :id 1] [?e :score ?s]] @conn))
            "data correct after cached-parent commits")
        (d/release conn))
      (d/delete-database cfg))))
