(ns datahike.test.writer-alternating-test
  "Two processes writing the same database ONE AFTER THE OTHER.

   Datahike's `:self` writer assumes all writers for a database live in one JVM:
   it keeps the branch head in memory ([:meta :datahike/commit-id]) and never
   re-reads it, so a warm writer is blind to anything another process committed.
   AWS Lambda breaks that premise — each execution environment is a separate JVM
   that believes it is the only writer, and Lambda keeps several warm and routes
   to them alternately. Each then commits from its own stale head and silently
   clobbers the other's transactions.

   `:writer {:backend :self :streaming? false}` re-reads the branch head from
   storage before every transaction, which makes alternating processes safe. It
   does NOT make CONCURRENT processes safe — that needs head fencing (#878)."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.connections :as conns]
            [datahike.writer :as w]
            [konserve.core :as k]))

;; A second `d/connect` with the same config normally returns the SAME cached
;; Connection out of the in-JVM registry — one writer, no race, nothing to test.
;; Binding the registry away gives a genuinely independent Connection with its
;; own LocalWriter and its own in-memory head, which is what a second Lambda
;; execution environment is. `d/connect` runs synchronously on this thread, so a
;; thread-local binding is enough.
(defn- connect-as-separate-process [cfg]
  (let [registry (atom {})]
    (binding [conns/*connections* registry]
      {:conn (d/connect cfg) :registry registry})))

(defn- release-separate-process [{:keys [conn registry]}]
  (binding [conns/*connections* registry]
    (d/release conn)))

;; The file backend is deliberate: two connections must not share in-memory
;; state behind our back. (The memory backend does share the konserve store
;; between connections of the same store id, which is fine — it is the durable
;; medium — but file is the unambiguous stand-in for S3 here.)
(defn- cfg [tag streaming?]
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir") "/dh-alternating-" tag)
           :id #uuid "a17e2a71-0000-0000-0000-000000000001"}
   :schema-flexibility :read
   :keep-history? false
   :writer (cond-> {:backend :self}
             (some? streaming?) (assoc :streaming? streaming?))})

(defn- fresh-db! [cfg]
  (when (d/database-exists? cfg) (d/delete-database cfg))
  (d/create-database cfg))

(defn- commit-chain-length
  "Number of commits reachable from the head through :datahike/parents. Detects
   a truncated lineage even when the datom count happens to look right."
  [conn]
  (let [db    @conn
        store (:store db)]
    (loop [cid (get-in db [:meta :datahike/commit-id]) n 0 seen #{}]
      (if (or (nil? cid) (seen cid))
        n
        (if-let [rec (k/get store cid nil {:sync? true})]
          (recur (first (get-in rec [:meta :datahike/parents])) (inc n) (conj seen cid))
          n)))))

(defn- alternate!
  "Commit `n` transactions alternately through two independent writers, then
   report what survived as seen by a third, freshly connected process."
  [tag streaming? n]
  (let [c (cfg tag streaming?)]
    (fresh-db! c)
    (let [a (connect-as-separate-process c)
          b (connect-as-separate-process c)]
      (try
        (dotimes [i n]
          (d/transact (:conn (if (even? i) a b)) [{:db/id -1 :name (str "e" i)}]))
        (finally
          (release-separate-process a)
          (release-separate-process b))))
    (let [observer (connect-as-separate-process c)]
      (try
        {:datoms (count (d/datoms @(:conn observer) :eavt))
         :commits (commit-chain-length (:conn observer))}
        (finally (release-separate-process observer))))))

(deftest default-writer-loses-alternating-updates
  (testing "the bug: with the default (:streaming? true) each warm writer commits
            from its own stale head, so half the transactions vanish"
    (let [n 10
          {:keys [datoms commits]} (alternate! "default" nil n)]
      (is (< datoms n)
          (str "expected the default writer to LOSE updates, but all " n
               " survived — if this starts passing, the head-cid threading in "
               "datahike.writer's commit loop changed and this test's premise "
               "needs revisiting"))
      ;; strict alternation: each writer only ever sees its own lineage, so
      ;; every second commit is overwritten
      (is (= (quot n 2) datoms)
          "each writer keeps exactly its own half")
      (is (< commits (inc n))
          "the commit lineage is truncated too, not just the datoms"))))

(deftest non-streaming-writer-survives-alternating-processes
  (testing ":streaming? false re-reads the head, so nothing is lost"
    (let [n 10
          {:keys [datoms commits]} (alternate! "nonstreaming" false n)]
      (is (= n datoms) "every alternating transaction survived")
      (is (= (inc n) commits)
          "and every commit is reachable from the head (create + n commits)"))))

(deftest non-streaming-costs-one-head-read-per-commit
  (testing "the price of :streaming? false is exactly one branch-head read per
            commit — one GET on an object store, not three"
    (doseq [[streaming? expected] [[true 0] [false 10]]]
      (let [c (cfg (str "headreads-" streaming?) streaming?)]
        (fresh-db! c)
        (let [{:keys [conn] :as p} (connect-as-separate-process c)]
          (try
            ;; warm: after the first commit a streaming writer holds a head cid
            (d/transact conn [{:db/id -1 :name "warm"}])
            (let [reads (atom 0)
                  orig  k/get]
              (with-redefs [k/get (fn [store key & args]
                                    (when (= key (:branch (:config @(:wrapped-atom conn))))
                                      (swap! reads inc))
                                    (apply orig store key args))]
                (dotimes [i 10] (d/transact conn [{:db/id -1 :name (str i)}])))
              (is (= expected @reads)
                  (str "streaming? " streaming? ": expected " expected
                       " branch-head reads for 10 commits, got " @reads)))
            (finally (release-separate-process p))))))))

(deftest streaming-flag-is-plumbed-from-the-writer-config
  (testing ":streaming? reaches the LocalWriter and defaults to true"
    (doseq [[streaming? expected] [[nil true] [true true] [false false]]]
      (let [c (cfg (str "plumb-" streaming?) streaming?)]
        (fresh-db! c)
        (let [{:keys [conn] :as p} (connect-as-separate-process c)]
          (try
            (is (= expected (w/streaming? (:writer @(:wrapped-atom conn))))
                (str ":writer " (:writer c)))
            (finally (release-separate-process p))))))))

(deftest streaming-is-a-connect-time-choice
  (testing "an existing database created with the default writer can be connected
            to with :streaming? false — the flag comes from the CONNECT config,
            which is what a serverless deployment can actually set"
    (let [created   (cfg "connect-time" nil)
          connected (assoc created :writer {:backend :self :streaming? false})]
      (fresh-db! created)
      (let [{:keys [conn] :as p} (connect-as-separate-process connected)]
        (try
          (is (false? (w/streaming? (:writer @(:wrapped-atom conn)))))
          (d/transact conn [{:db/id -1 :name "x"}])
          (is (= 1 (count (d/datoms @conn :eavt))))
          (finally (release-separate-process p)))))))

(deftest non-streaming-writer-survives-a-rejected-transaction
  (testing "a transaction that fails before it reaches the commit queue must not
            leave the loop waiting for a commit that never happens"
    (let [c (assoc (cfg "rejected" false) :schema-flexibility :write)]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (d/transact conn [{:db/ident       :name
                             :db/valueType   :db.type/string
                             :db/cardinality :db.cardinality/one}])
          (is (thrown? Exception (d/transact conn [{:unknown-attribute "boom"}])))
          (d/transact conn [{:name "after"}])
          (is (= ["after"] (map :v (d/datoms @conn :aevt :name))))
          (finally (release-separate-process p)))))))

(deftest non-streaming-writer-handles-concurrent-callers-in-one-process
  (testing "the head re-read serialises transactions inside a writer without
            deadlocking or dropping any of them"
    (let [c (cfg "concurrent" false)
          n 20]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (->> (range n)
               (mapv (fn [i] (future (d/transact conn [{:db/id -1 :name (str i)}]))))
               (run! deref))
          (is (= n (count (d/datoms @conn :eavt))))
          (finally (release-separate-process p)))))))
