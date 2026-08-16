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
            [datahike.config :as dc]
            [datahike.connections :as conns]
            [datahike.connector :as dcon]
            [datahike.db.transaction :as dbtx]
            [datahike.index.secondary :as sec]
            [datahike.index.secondary.stratum]
            [datahike.writer :as w]
            [datahike.writing :as dw]
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

;; ---------------------------------------------------------------------------
;; Secondary indices. Stratum is the test target because it is konserve-backed
;; and copy-on-write: its key-map is a content-addressed dataset commit-id, so
;; it moves with every flush and restores from the store — which is what makes
;; it, unlike scriptum's Lucene directory, multi-process safe.

(defn- stratum-cfg [tag]
  (assoc (cfg tag false) :schema-flexibility :write))

(defn- with-stratum-index! [conn]
  (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:db/ident          :idx/strat
                     :db.secondary/type :stratum
                     :db.secondary/attrs [:p/name]}]))

(defn- strat-rows [db]
  (when-let [idx (get-in db [:secondary-indices :idx/strat])]
    (sec/-estimate idx {})))

(defn- head-record [db]
  (k/get (:store db) (:branch (:config db)) nil {:sync? true}))

(defn- head-strat-key [db]
  (get-in (head-record db) [:secondary-index-keys :idx/strat]))

(deftest secondary-index-survives-alternating-processes
  (testing "a secondary index is named by the commit, so the head re-read has to
            pick up the OTHER process's index writes with it — keeping the
            writer's own already-open instance loses half the rows"
    (let [c (stratum-cfg "sec-alternating")
          n 6]
      (fresh-db! c)
      (let [a (connect-as-separate-process c)
            b (connect-as-separate-process c)
            last-key (atom nil)]
        (try
          (with-stratum-index! (:conn a))
          (dotimes [i n]
            (d/transact (:conn (if (even? i) a b)) [{:p/name (str "p" i)}])
            (reset! last-key (head-strat-key @(:wrapped-atom (:conn a)))))
          (is (= n (strat-rows @(:conn a))) "writer A sees its own rows and B's")
          (is (= n (strat-rows @(:conn b))) "and writer B sees both as well")
          ;; B transacted last (i = n-1 is odd), so the head must name B's
          ;; flush — and that flush contains A's rows too.
          (is (= @last-key (head-strat-key @(:wrapped-atom (:conn b))))
              "the head names the LATER writer's flush")
          (finally
            (release-separate-process a)
            (release-separate-process b))))
      (let [observer (connect-as-separate-process c)]
        (try
          (is (= n (strat-rows @(:conn observer)))
              "and a third, freshly connected process restores every row")
          (finally (release-separate-process observer)))))))

(deftest deref-keeps-the-secondary-index-and-follows-the-head
  (testing "@conn on a non-streaming connection rebuilds only when the head
            moved: rebuilding on every deref re-runs the secondary restore,
            which contends with the live writer's lock and silently drops the
            index. It must still see another process's commit."
    (let [c (stratum-cfg "sec-deref")]
      (fresh-db! c)
      (let [a (connect-as-separate-process c)]
        (try
          (with-stratum-index! (:conn a))
          (d/transact (:conn a) [{:p/name "alice"}])
          (is (seq (:secondary-indices @(:conn a))) "@conn keeps the index")
          (is (some? (:writer @(:conn a))) "and the connection's writer")
          (is (= 1 (strat-rows @(:conn a))))
          (let [b (connect-as-separate-process c)]
            (try
              (d/transact (:conn b) [{:p/name "bob"}])
              (finally (release-separate-process b))))
          (is (= 2 (count (d/datoms @(:conn a) :aevt :p/name)))
              "@conn reflects the external commit")
          (is (= 2 (strat-rows @(:conn a)))
              "including its secondary index")
          (finally (release-separate-process a)))))))

(deftest unmoved-head-is-not-rebuilt
  (testing "the head cid IS the identity of the stored record, so a re-read
            that finds the same cid rebuilds nothing — no stored->db, and in
            particular no secondary-index restore, per transaction or per deref"
    (let [c (cfg "no-rebuild" false)]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          ;; warm: after the first commit the writer holds the committed db
          (d/transact conn [{:db/id -1 :name "warm"}])
          (let [rebuilds (atom 0)
                orig     dw/stored->db]
            (with-redefs [dw/stored->db (fn [& args]
                                          (swap! rebuilds inc)
                                          (apply orig args))]
              (dotimes [i 10] (d/transact conn [{:db/id -1 :name (str i)}]))
              (dotimes [_ 5] (d/datoms @conn :eavt)))
            (is (zero? @rebuilds)
                (str "a single-process non-streaming writer never moves its own "
                     "head under itself, so nothing should be rebuilt; got "
                     @rebuilds)))
          (finally (release-separate-process p)))))))

(deftest dropped-restore-keeps-the-stored-index-pointer
  (testing "a restore that fails is transient; DELETING the index pointer is
            not. The next commit must carry the stored key-map forward, or a
            reconnect gets an empty skeleton marked :ready that nothing
            backfills."
    (let [c (stratum-cfg "sec-restore-failure")]
      (fresh-db! c)
      (let [p (connect-as-separate-process c)]
        (try
          (with-stratum-index! (:conn p))
          (d/transact (:conn p) [{:p/name "alice"} {:p/name "bob"}])
          (finally (release-separate-process p))))
      (let [before (let [p (connect-as-separate-process c)]
                     (try (head-strat-key @(:wrapped-atom (:conn p)))
                          (finally (release-separate-process p))))]
        ;; Two redefs, both needed to hold the db in the state this is about:
        ;; create-index throwing IS the restore failure, and
        ;; finalize-secondary-indices is the other repair path — it re-creates
        ;; a missing instance from the schema on the next transaction (empty,
        ;; :building, backfilled afterwards). Stubbing it out leaves the commit
        ;; facing an ident that is declared in the schema and has no instance.
        (with-redefs [sec/create-index (fn [& _] (throw (ex-info "restore boom" {})))
                      dbtx/finalize-secondary-indices identity]
          (let [p (connect-as-separate-process c)]
            (try
              (is (nil? (strat-rows @(:wrapped-atom (:conn p))))
                  "the failed restore dropped the ident, as it does today")
              (d/transact (:conn p) [{:p/name "carol"}])
              (is (= before (head-strat-key @(:wrapped-atom (:conn p))))
                  "and the commit carried the stored pointer forward untouched")
              (finally (release-separate-process p)))))
        (let [p (connect-as-separate-process c)]
          (try
            (is (= 2 (strat-rows @(:wrapped-atom (:conn p))))
                "so a clean reconnect still finds the index")
            (finally (release-separate-process p))))))))

(deftest removed-index-loses-its-pointer
  (testing "carrying pointers forward must not resurrect a REMOVED index:
            'no live instance' is not 'delete', but a retracted schema entry is"
    (let [c (stratum-cfg "sec-removal")]
      (fresh-db! c)
      (let [p (connect-as-separate-process c)]
        (try
          (with-stratum-index! (:conn p))
          (d/transact (:conn p) [{:p/name "alice"}])
          (let [db      @(:wrapped-atom (:conn p))
                key-map (head-strat-key db)
                ;; the state a dropped restore leaves behind: declared in the
                ;; schema, carried in :secondary-index-keys, no live instance
                dropped (-> db
                            (assoc :secondary-index-keys {:idx/strat key-map})
                            (update :secondary-indices dissoc :idx/strat))]
            (is (= {:idx/strat key-map}
                   (:secondary-index-keys (second (dw/db->stored dropped true))))
                "the pointer of a declared-but-missing index is kept")
            (is (nil? (:secondary-index-keys
                       (second (dw/db->stored (update dropped :schema dissoc :idx/strat)
                                              true))))
                "the pointer of an undeclared index is dropped"))
          (finally (release-separate-process p)))))))

;; ---------------------------------------------------------------------------
;; Failure and configuration handling

(deftest failed-head-read-fails-the-caller-not-the-writer
  (testing "a branch-head read that throws must reach the caller of THAT
            transaction and leave the writer usable. Escaping the loop would
            kill it with transaction-queue still open, so every later transact
            would enqueue happily and hang forever."
    (let [c (cfg "head-read-failure" false)]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (d/transact conn [{:db/id -1 :name "before"}])
          (let [branch (:branch (:config @(:wrapped-atom conn)))
                orig   k/get
                boom?  (atom true)
                result (with-redefs [k/get (fn [store key & args]
                                             (if (and @boom? (= key branch))
                                               (throw (ex-info "storage is down" {:key key}))
                                               (apply orig store key args)))]
                         ;; BOUNDED: an unbounded (is (thrown? ...)) would hang
                         ;; the whole suite the day this regresses
                         (let [f (future (try (d/transact conn [{:db/id -1 :name "during"}])
                                              ::no-throw
                                              (catch Exception _ ::threw)))
                               r (deref f 10000 ::timed-out)]
                           (reset! boom? false)
                           r))]
            (is (= ::threw result)
                "the failed transaction's caller gets the error, promptly"))
          ;; bounded for the same reason: on regression the writer is dead with
          ;; its transaction-queue still open, so this would never come back
          (is (= ::ok (deref (future (d/transact conn [{:db/id -1 :name "after"}]) ::ok)
                             10000 ::timed-out))
              "and the writer still works afterwards")
          (is (= #{"before" "after"} (set (map :v (d/datoms @conn :aevt :name)))))
          (finally (release-separate-process p)))))))

(deftest self-writer-rejects-unknown-keys
  (testing "a typo like :streaming (no ?) would silently select the unsafe
            default, which is exactly the setting whose failure is silent"
    (let [c (assoc (cfg "unknown-key" nil) :writer {:backend :self :streaming false})]
      (fresh-db! (cfg "unknown-key" nil))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Unknown key"
                            (connect-as-separate-process c))))))

(deftest streaming-flag-is-not-part-of-the-stored-config
  (testing "a database CREATED with :streaming? false can be connected to
            without it (the reverse is covered by streaming-is-a-connect-time-
            choice) — the flag is a property of the connecting process"
    (let [created   (cfg "created-with-flag" false)
          connected (assoc created :writer {:backend :self})]
      (fresh-db! created)
      (let [{:keys [conn] :as p} (connect-as-separate-process connected)]
        (try
          (is (true? (w/streaming? (:writer @(:wrapped-atom conn))))
              "the connect config wins")
          (d/transact conn [{:db/id -1 :name "x"}])
          (is (= 1 (count (d/datoms @conn :eavt))))
          (finally (release-separate-process p))))))

  (testing "and the stored/connect comparison ignores it. NOTE: connector's
            'if we connect to remote allow writer to be different' test compares
            dc/self-writer against the WHOLE config, so it is always false and
            :writer is dropped from both sides — writer consistency is not
            enforced at all today (fixing that is its own change). Rebinding
            self-writer takes the branch that the eventual fix will take, and
            pins that :streaming? must not make the pair mismatch there."
    (let [conf   {:branch :db :writer {:backend :self}}
          stored {:branch :db :writer {:backend :self :streaming? false}}]
      (with-redefs [dc/self-writer conf]
        (is (nil? (dcon/ensure-stored-config-consistency conf stored)))))))
