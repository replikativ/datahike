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
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.config :as dc]
            [datahike.connections :as conns]
            [datahike.connector :as dcon]
            [datahike.db.transaction :as dbtx]
            [datahike.index.secondary :as sec]
            [datahike.index.secondary.stratum]
            [datahike.writer :as w]
            [datahike.tx-preds :as txp]
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
;; Batching. The head re-read is per BATCH, not per transaction: the loop chains
;; queued transactions onto one head the way the streaming writer chains onto
;; :db-after, and only stops to wait when the queue drains or the batch bound is
;; reached. Without that, `:streaming? false` degrades a burst of N concurrent
;; transacts into N round-trips — which on an object store is the whole cost.

(defn- burst!
  "Fire `n` transactions at `conn` at once and wait for all of them."
  [conn n]
  (->> (range n)
       (mapv (fn [i] (future (d/transact conn [{:db/id -1 :name (str "e" i)}]))))
       (mapv #(deref % 60000 ::timed-out))))

(deftest non-streaming-batches-a-burst
  (testing "concurrently queued transactions share one head read and one commit"
    (let [c (cfg "batching" false)
          n 100]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (d/transact conn [{:db/id -1 :name "warm"}])
          (let [reads  (atom 0)
                branch (:branch (:config @(:wrapped-atom conn)))
                orig   k/get
                reports (with-redefs [k/get (fn [store key & args]
                                              (when (= key branch) (swap! reads inc))
                                              (apply orig store key args))]
                          (burst! conn n))]
            (is (empty? (filter #{::timed-out} reports))
                "no caller is left waiting for a signal that never comes")
            (is (= (inc n) (count (d/datoms @conn :eavt)))
                "and every transaction in the burst landed")
            (is (< @reads n)
                (str "a burst of " n " must not cost " n " head reads; got " @reads))
            ;; The real bar is an object-store one: this used to be n GETs.
            (is (< @reads (quot n 4))
                (str "batching is barely working: " @reads " head reads for " n
                     " queued transactions")))
          (finally (release-separate-process p)))))))

(deftest batched-commits-all-stay-on-the-lineage
  (testing "each commit of a batch is the parent of the next. Stamping the
            batch's head cid onto every member instead would make them all claim
            the SAME parent, orphaning every commit but the last — the datom
            count still looks right, so only the chain shows it."
    ;; Deliberately FEW, HEAVY transactions rather than many tiny ones. The
    ;; property only has teeth when one batch spans SEVERAL commits: that is the
    ;; case where a chained transaction carries no parent stamp and the commit
    ;; loop must fall back to the cid it threaded itself. Tiny transactions let
    ;; the commit loop drain the whole batch in one group (measured: 60 tiny
    ;; transactions collapse to ~2 commits under CI load, and the assertion below
    ;; then holds vacuously — or flakes). Applying a transaction of this size
    ;; takes longer than committing one, so the commit loop keeps finding the
    ;; queue empty and each transaction becomes its own group.
    (let [c   (cfg "batch-lineage" false)
          n   20
          per 400]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (let [reads   (atom 0)
                branch  (:branch (:config @(:wrapped-atom conn)))
                orig    k/get
                reports (with-redefs [k/get (fn [store key & args]
                                              (when (= key branch) (swap! reads inc))
                                              (apply orig store key args))]
                          (->> (range n)
                               (mapv (fn [i]
                                       (future (d/transact
                                                conn (mapv (fn [j] {:name (str i "-" j)})
                                                           (range per))))))
                               (mapv #(deref % 180000 ::timed-out))))
                cids    (into #{} (map #(get-in % [:tx-meta :db/commitId])) reports)]
            (is (empty? (filter #{::timed-out} reports)))
            ;; NOT (< (count cids) n): on a machine where applying 400 datoms is
            ;; slower than committing them — the very regime this shape is chosen
            ;; for — every transaction becomes its own commit group and cids = n.
            ;; That is a fine outcome here; batching itself is pinned by
            ;; non-streaming-batches-a-burst. What this test needs is only that
            ;; SOME batch spanned several commits.
            (is (>= (count cids) 2) "more than one commit to chain")
            (is (< @reads (count cids))
                (str "at least one batch must span several commits, or the "
                     "threaded-parent path is untested: " @reads " head reads for "
                     (count cids) " commits"))
            (is (= (inc (count cids)) (commit-chain-length conn))
                "and every distinct commit is reachable from the head")
            (is (every? #(some? (get-in % [:db-after :meta :datahike/commit-id])) reports)
                "every caller gets a committed :db-after, not its private report db"))
          (finally (release-separate-process p))))
      ;; and it is durable, not just live state
      (let [o (connect-as-separate-process c)]
        (try
          (is (= (* n per) (count (d/datoms @(:conn o) :eavt))))
          (finally (release-separate-process o)))))))

(deftest batches-still-re-read-between-processes
  (testing "batching moves the head read to the batch boundary, it does not
            remove it: another process committing between our bursts must still
            be picked up"
    (let [c (cfg "batch-alternating" false)
          rounds 5 per-round 8]
      (fresh-db! c)
      (let [a (connect-as-separate-process c)
            b (connect-as-separate-process c)]
        (try
          (dotimes [r rounds]
            (burst! (:conn a) per-round)
            (d/transact (:conn b) [{:db/id -1 :name (str "b" r)}]))
          (finally
            (release-separate-process a)
            (release-separate-process b))))
      (let [o (connect-as-separate-process c)]
        (try
          (is (= (* rounds (inc per-round)) (count (d/datoms @(:conn o) :eavt)))
              "nothing from either process was clobbered")
          (finally (release-separate-process o)))))))

(defn- head-reads-during
  "Branch-head reads performed by `f`."
  [conn f]
  (let [branch (:branch (:config @(:wrapped-atom conn)))
        reads  (atom 0)
        orig   k/get]
    (with-redefs [k/get (fn [store key & args]
                          (when (= key branch) (swap! reads inc))
                          (apply orig store key args))]
      (f))
    @reads))

;; Determinism here is structural, not timing-based. A gated `:write-fn-map`
;; entry holds the transaction loop INSIDE op-fn until we release it, and
;; `w/dispatch!` enqueues synchronously (unlike `d/transact`, whose put! happens
;; inside a go block), so "the trailing op is queued behind the gated one" is
;; established by construction rather than by a sleep that CI can lose.

(defn- register-gate!
  "Returns a gate-state atom. While it holds {:entered p :gate p}, the next
   transaction the loop applies delivers :entered and blocks on :gate.

   A tx-pred rather than a `:write-fn-map` entry: the writer config is
   SERIALIZED into the stored db, so it cannot carry a function, while a tx-pred
   is registered out of band by store-id. It also runs exactly where this test
   needs the hold — inside op-fn, on the transaction loop."
  [c]
  (let [state (atom nil)]
    (txp/register-tx-pred! (get-in c [:store :id])
                           (fn [_report]
                             (when-let [{:keys [entered gate]} @state]
                               (reset! state nil) ;; hold exactly one transaction
                               (deliver entered true)
                               @gate)))
    state))

(defn- dispatch-tx! [conn tx-data]
  (w/dispatch! (:writer @(:wrapped-atom conn))
               {:op 'transact! :args [{:tx-data tx-data}]}))

(defn- with-trailing-noncommit!
  "Open a batch and make `trailing-op` the LAST thing the transaction loop
   processes in it: hold the loop inside a transaction, enqueue `trailing-op`
   behind it, release. `trailing-op` must take a loop arm that enqueues no
   commit, which before the fix also left the batch open."
  [state conn held-tx-data trailing-op]
  (let [entered (promise)
        gate    (promise)]
    (reset! state {:entered entered :gate gate})
    (let [held (dispatch-tx! conn held-tx-data)]
      (is (not= ::timed-out (deref entered 30000 ::timed-out))
          "the loop is inside the held transaction")
      ;; synchronous enqueue: this is now provably behind `held`
      (let [trailing (trailing-op)]
        (deliver gate true)
        (is (not= ::timed-out (deref (future (async/<!! held)) 30000 ::timed-out)))
        (is (not= ::timed-out (deref (future (async/<!! trailing)) 60000 ::timed-out)))))))

(deftest a-batch-is-closed-before-the-writer-waits-for-work
  (testing "a batch must be bounded in TIME, not only in count. If the last item
            the loop processes neither commits nor closes the batch — a rejected
            transaction, an async op — the loop parks with the batch still open,
            and the NEXT transaction (seconds or hours later) skips the head read
            and clobbers whatever another process committed in between."
    (doseq [[label trailing] [[:rejected #(dispatch-tx! % [{:not-in-schema 1}])]
                              ;; an async op takes the `chan? res` arm, which
                              ;; likewise enqueues no commit
                              [:async #(w/dispatch! (:writer @(:wrapped-atom %))
                                                    {:op 'gc-storage! :args []})]]]
      (let [c     (assoc (cfg (str "strand-" (name label)) false)
                         :schema-flexibility :write)
            state (register-gate! c)]
        (fresh-db! c)
        (let [{:keys [conn] :as p} (connect-as-separate-process c)]
          (try
            (d/transact conn [{:db/ident       :name
                               :db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/one}])
            (with-trailing-noncommit! state conn [{:name "held"}] #(trailing conn))
            (is (= 1 (head-reads-during conn #(d/transact conn [{:name "after"}])))
                (str "after a batch whose last item was a " (name label)
                     " operation, the next transaction must re-read the head"))
            (finally
              (txp/unregister-tx-pred! (get-in c [:store :id]))
              (release-separate-process p))))))))

(deftest bursts-with-rejections-still-see-the-other-process
  (testing "the integration form: each of A's batches ENDS on a rejected
            transaction, and B commits between them. Every one of B's commits
            must survive — this is the exact data loss :streaming? false exists
            to prevent, and a rejected transaction must not reopen it."
    (let [c      (assoc (cfg "strand-alternating" false) :schema-flexibility :write)
          state  (register-gate! c)
          rounds 6]
      (fresh-db! c)
      (let [a (connect-as-separate-process c)
            b (connect-as-separate-process c)]
        (try
          (d/transact (:conn a) [{:db/ident       :name
                                  :db/valueType   :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          (dotimes [r rounds]
            (with-trailing-noncommit! state (:conn a) [{:name (str "a" r)}]
              #(dispatch-tx! (:conn a) [{:not-in-schema r}]))
            (d/transact (:conn b) [{:name (str "b" r)}]))
          (finally
            (txp/unregister-tx-pred! (get-in c [:store :id]))
            (release-separate-process a)
            (release-separate-process b))))
      (let [o (connect-as-separate-process c)]
        (try
          (let [names (into #{} (map :v) (d/datoms @(:conn o) :aevt :name))]
            ;; BOTH sides, because which one loses depends on who commits last:
            ;; a stranded A overwrites B's head, and B's next commit — B re-reads
            ;; correctly — overwrites A's stale-head commit in turn.
            (is (= (set (map #(str "b" %) (range rounds)))
                   (into #{} (filter #(re-matches #"b\d+" %)) names))
                "every one of the other process's commits survived")
            (is (= (set (map #(str "a" %) (range rounds)))
                   (into #{} (filter #(re-matches #"a\d+" %)) names))
                "and so did every one of ours"))
          (finally (release-separate-process o)))))))

(deftest a-burst-larger-than-the-pending-put-cap-survives
  (testing "commit-done is unbuffered and core.async THROWS on the 1025th pending
            put — inside the commit loop that closes every queue and kills the
            writer. MAX_NONSTREAMING_BATCH is what keeps the count unreachable,
            so drive MORE transactions than the cap through one writer and check
            that it lost nothing and is still alive."
    (is (< w/MAX_NONSTREAMING_BATCH 1024)
        "the bound must stay below core.async's pending-put cap")
    (let [c (cfg "over-the-cap" false)
          n 1200]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (let [rs (->> (range n)
                        (mapv (fn [i] (future (d/transact conn [{:db/id -1 :name (str i)}]))))
                        (mapv #(deref % 180000 ::timed-out)))]
            (is (empty? (filter #{::timed-out} rs))
                "no caller was left waiting — a thrown commit loop parks all of them")
            (is (= n (count (d/datoms @conn :eavt)))))
          (is (= ::ok (deref (future (d/transact conn [{:db/id -1 :name "still-alive"}]) ::ok)
                             30000 ::timed-out))
              "and the writer still works afterwards")
          (finally (release-separate-process p)))))))

(deftest a-failure-mid-batch-does-not-strand-the-batch
  (testing "a rejected transaction commits nothing, so it must neither consume a
            durability signal owed to another transaction nor leave one behind —
            either way the writer parks forever"
    (let [c (assoc (cfg "batch-failure" false) :schema-flexibility :write)
          n 100]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (d/transact conn [{:db/ident       :name
                             :db/valueType   :db.type/string
                             :db/cardinality :db.cardinality/one}])
          (let [rs (->> (range n)
                        (mapv (fn [i]
                                (future
                                  (try (d/transact conn [(if (odd? i)
                                                           {:not-in-schema i}
                                                           {:name (str i)})])
                                       ::ok
                                       (catch Exception _ ::threw)))))
                        (mapv #(deref % 60000 ::timed-out)))]
            (is (empty? (filter #{::timed-out} rs)) "nobody hangs")
            (is (= (quot n 2) (count (filter #{::ok} rs))))
            (is (= (quot n 2) (count (filter #{::threw} rs))))
            (is (= (quot n 2) (count (d/datoms @conn :aevt :name)))
                "and exactly the accepted half persisted"))
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

(deftest a-failed-restore-of-a-stored-index-fails-the-connect
  (testing "an index that EXISTS in storage and cannot be restored must abort
            the connect. Coming up without it is silent data loss on a delay:
            finalize-secondary-indices cannot tell the resulting instance-less
            ident from a brand-new index, so the next transaction builds an
            empty one and the commit overwrites the stored pointer with it."
    (let [c (stratum-cfg "sec-restore-fails-loudly")]
      (fresh-db! c)
      (let [p (connect-as-separate-process c)]
        (try
          (with-stratum-index! (:conn p))
          (d/transact (:conn p) [{:p/name "alice"}])
          (finally (release-separate-process p))))
      (with-redefs [sec/create-index (fn [& _] (throw (ex-info "restore boom" {})))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Could not restore a secondary index"
                              (connect-as-separate-process c))))
      ;; the escape hatch, for a deployment that would rather be degraded
      (with-redefs [sec/create-index (fn [& _] (throw (ex-info "restore boom" {})))]
        (binding [dw/*on-secondary-restore-failure* :drop]
          (let [p (connect-as-separate-process c)]
            (try (is (nil? (strat-rows @(:wrapped-atom (:conn p)))))
                 (finally (release-separate-process p))))))
      ;; and an index with NO stored pointer is not a failure at all: there is
      ;; nothing to lose, and backfill is the normal path
      (let [p (connect-as-separate-process c)]
        (try (is (= 1 (strat-rows @(:wrapped-atom (:conn p))))
                 "a clean reconnect is unaffected")
             (finally (release-separate-process p)))))))

(deftest a-failed-restore-closes-the-indices-it-already-restored
  (testing "the accumulator dies with the raise, so anything already restored is
            unreachable. For a lock-holding backend the leaked lock is what makes
            the NEXT attempt fail too, turning a transient failure permanent."
    (let [c (stratum-cfg "sec-restore-leak")]
      (fresh-db! c)
      (let [p (connect-as-separate-process c)]
        (try
          (d/transact (:conn p) [{:db/ident :p/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :p/tag :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          (d/transact (:conn p) [{:db/ident :idx/one :db.secondary/type :stratum
                                  :db.secondary/attrs [:p/name]}
                                 {:db/ident :idx/two :db.secondary/type :stratum
                                  :db.secondary/attrs [:p/tag]}])
          (d/transact (:conn p) [{:p/name "alice" :p/tag "x"}])
          (finally (release-separate-process p))))
      ;; Fail the SECOND index whichever ident that turns out to be — schema is a
      ;; map, so the order is not ours to choose. The stub is Closeable but not
      ;; IVersionedSecondaryIndex, so it lands in the accumulator as-is, which is
      ;; the state this is about.
      (let [attempts (atom 0)
            closed   (atom 0)]
        (with-redefs [sec/create-index
                      (fn [& _]
                        (if (= 1 (swap! attempts inc))
                          (reify java.io.Closeable (close [_] (swap! closed inc)))
                          (throw (ex-info "restore boom" {}))))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Could not restore a secondary index"
                                (connect-as-separate-process c))))
        (is (= 2 @attempts) "both indices were attempted")
        (is (= 1 @closed)
            "and the one that succeeded was closed on the way out")))))

(deftest dropped-restore-keeps-the-stored-index-pointer
  (testing "under :drop, a restore that fails is transient; DELETING the index
            pointer is not. The next commit must carry the stored key-map
            forward, or a reconnect gets an empty skeleton marked :ready that
            nothing backfills."
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
                      dbtx/finalize-secondary-indices identity
                      dw/*on-secondary-restore-failure* :drop]
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

(deftest a-vanished-head-raises-on-deref
  (testing "another process deleted the database under a live connection. @conn
            must say so — it used to build a db out of a nil head record (nil
            :max-tx, no index roots), which queries answer emptily and then die
            on with a bare IllegalArgumentException much further away.

            Only reachable with :streaming? false, because that is the only
            writer whose @conn reads the head from storage at all."
    (let [c (cfg "vanished-head" false)]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process c)]
        (try
          (d/transact conn [{:db/id -1 :name "before"}])
          (is (= 1 (count (d/datoms @conn :eavt))) "healthy to start with")
          ;; delete the branch head out from under the live connection, the way
          ;; another process's delete-database would
          (let [db (:wrapped-atom conn)]
            (k/dissoc (:store @db) (:branch (:config @db)) {:sync? true}))
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Branch head vanished"
                                @conn)
              "the deref raises rather than handing back a malformed db")
          (is (= :branch-head-does-not-exist-in-store
                 (try @conn (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
              "and it is the same typed error the writer's re-read raises")
          (finally
            ;; The head is gone, so `release` would deref straight back through
            ;; the path under test and `fresh-db!` would find a store directory
            ;; that `database-exists?` (which reads the head) says is not there.
            ;; Drop the connection and remove the store outright.
            (swap! (:registry p) empty)
            (try (d/delete-database c) (catch Exception _ nil))))))))

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

(deftest self-writer-rejects-a-non-boolean-streaming-flag
  (testing "everything but nil and false is truthy, so :streaming? \"false\" out
            of an env var or a JSON config would read as TRUE — selecting the
            single-writer assumption in the deployment that cannot hold it"
    (fresh-db! (cfg "bad-streaming" nil))
    (doseq [v ["false" 0 :false]]
      (let [c (assoc (cfg "bad-streaming" nil) :writer {:backend :self :streaming? v})]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"must be true or false"
                              (connect-as-separate-process c))
            (str ":streaming? " (pr-str v))))))

  (testing "and an explicit nil, which is not reachable through connect —
            `remove-nils` in config loading strips it, so the documented default
            correctly applies there — but IS reachable by calling create-writer
            directly, where Clojure's `:or` would not fire for a present key and
            the writer would silently run non-streaming"
    (let [c (cfg "nil-streaming" nil)]
      (fresh-db! c)
      (let [{:keys [conn] :as p} (connect-as-separate-process
                                  (assoc c :writer {:backend :self :streaming? nil}))]
        (try
          (is (true? (w/streaming? (:writer @(:wrapped-atom conn))))
              "connect strips the nil and takes the default")
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"must be true or false"
                                (w/create-writer {:backend :self :streaming? nil} conn))
              "create-writer itself does not accept it")
          (finally (release-separate-process p)))))))

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
