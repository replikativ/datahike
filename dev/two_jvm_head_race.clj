(ns two-jvm-head-race
  "A harness for the branch-head race (#878), driven from TWO JVMs by hand.

   WHY TWO JVMS AND NOT TWO THREADS. On a filestore the cross-process primitive
   is `java.nio.channels.FileLock`, which is JVM-WIDE, not thread-wide:
   overlapping locks inside one JVM raise `OverlappingFileLockException`. Threads
   in one JVM therefore cannot exercise that path at all — they exercise
   konserve's `get-lock` retry loop instead. Two JVMs is the only configuration
   that tests the real thing. (Two `d/connect`s in one JVM with the registry
   rebound, as `writer_alternating_test` does, is enough for ALTERNATING writers,
   because nothing there depends on the lock. It is not enough for this.)

   WHY BY HAND AND NOT A RACE. The interleaving that matters is one specific
   window, and racing does not reliably hit it:

       A: read head H, apply the transaction        <- held open by the gate
       B: transact and commit                       -> head is now H'
       A: write head                                <- must be rejected

   Stepping it from two REPLs hits it every time, and the failure is a stated
   value rather than a flake.

   HOW THE WINDOW IS HELD OPEN. A store-level tx-pred (`datahike.tx-preds`) runs
   inside the writer's `op-fn`, i.e. AFTER the head read and the transaction has
   been applied, and BEFORE the commit is enqueued. Blocking there parks the
   writer in exactly the window above. It is registered out of band by store-id,
   so it needs no config change and is never serialized.

   USAGE — two shells, same store path:

     ;; both JVMs
     clj -M:test -m nrepl.cmdline          ; note each port
     (require 'two-jvm-head-race :reload)
     (def h (two-jvm-head-race/open! \"/tmp/dh-race\"))   ; A also calls (fresh! ...)

     ;; A: open the window and stop inside it
     (two-jvm-head-race/arm! h)
     (def f (two-jvm-head-race/transact-gated! h [{:db/id -1 :name \"a\"}]))
     (two-jvm-head-race/entered? h)        ; => true, A is parked in the window

     ;; B: commit while A is parked
     (two-jvm-head-race/transact! h [{:db/id -1 :name \"b\"}])

     ;; A: let the parked transaction finish its commit
     (two-jvm-head-race/release! h)
     @f

     ;; either: what survived?
     (two-jvm-head-race/report h)

   WITHOUT head fencing this reports `:b-survived? false` — A's commit was
   computed from head H and overwrote B's, whose commit is now an orphan: the
   datoms are gone and the chain is one shorter than the number of commits made.
   WITH fencing, A's head write must be rejected and retried against H', and both
   survive.

   The point of running it BEFORE the fix is to know the harness can fail. A
   test that cannot fail proves nothing, and on this branch that has already
   happened twice."
  (:require [datahike.api :as d]
            [datahike.connections :as conns]
            [datahike.tx-preds :as txp]
            [datahike.writer :as w]
            [hasch.core :refer [uuid]]
            [konserve.core :as k]))

(defn store-id
  "Derived from the PATH, not fixed. datahike's connection registry caches by
   store identity — which is `(:id config)` — so a hard-coded id makes two
   different paths look like one store: a second `open!` on a new path hands back
   the cached connection to the old one, silently, and the harness reports on a
   store nobody is writing to. Found exactly that way."
  [path]
  (uuid path))

(def ^:dynamic *store*
  "Which backend the harness races on. `:file` fences at :machine (an OS advisory
   lock); `:s3` at :global (S3 evaluates If-Match itself). The datahike side is
   identical for both — which is the point of running it twice."
  :file)

(defn store-config [path]
  (case *store*
    :file {:backend :file :path path :id (store-id path)}
    :s3   {:backend :s3 :region "us-east-1" :bucket "kcas-test" :id (store-id path)
           :access-key "minioadmin" :secret "minioadmin" :path-style-access? true
           :endpoint-override {:protocol :http :hostname "localhost" :port 9000}}))

(defn config [path]
  {:store  (store-config path)
   :schema-flexibility :read
   :keep-history? false
   ;; The writer under test. Shared ownership re-reads the head per batch,
   ;; which is what makes two processes ALTERNATING safe; it is deliberately not
   ;; enough for the overlap this harness produces.
   :writer {:backend :self :writer-ownership :shared}})

(defn fresh!
  "Create the store. Run in ONE JVM only, before the other connects."
  [path]
  (let [c (config path)]
    (when (d/database-exists? c) (d/delete-database c))
    (d/create-database c)
    c))

(defn open!
  "Connect this JVM to the store. Returns the handle every other fn takes."
  [path]
  {:conn  (d/connect (config path))
   :gate  (atom nil)
   :path  path})

(defn close! [h] (d/release (:conn h)))

;; ---------------------------------------------------------------------------
;; The gate

(defn arm!
  "Hold the NEXT transaction this JVM applies inside the writer, between the head
   read and the commit. Call in the JVM that should lose the race."
  [h]
  ;; `:armed?` is a separate flag and the promises STAY in the atom. Clearing the
  ;; atom to hold exactly one transaction also threw away the promise `release!`
  ;; needs, so only the parked thread still held it and the writer could never be
  ;; let go — the harness deadlocked on its first real run.
  (reset! (:gate h) {:entered (promise) :release (promise) :armed? true})
  (txp/register-tx-pred!
   (store-id (:path h))
   (fn [_report]
     (let [{:keys [entered release armed?]} @(:gate h)]
       (when armed?
         (swap! (:gate h) assoc :armed? false)   ; hold exactly one transaction
         (deliver entered true)
         @release))))
  :armed)

(defn entered?
  "Blocks (max 30s) until the armed transaction is parked in the window."
  [h]
  (= true (deref (:entered @(:gate h)) 30000 ::timeout)))

(defn release! [h]
  (deliver (:release @(:gate h)) true)
  (txp/unregister-tx-pred! (store-id (:path h)))
  :released)

;; ---------------------------------------------------------------------------
;; Driving

(defn transact-gated!
  "Dispatch a transaction that will park in the window. Returns a future; deref
   it AFTER `release!`."
  [h tx-data]
  (future (d/transact (:conn h) tx-data)))

(defn transact! [h tx-data]
  (d/transact (:conn h) tx-data))

;; ---------------------------------------------------------------------------
;; Observation

(defn head-cid [h]
  (get-in @(:wrapped-atom (:conn h)) [:meta :datahike/commit-id]))

(defn- chain-length
  "Commits reachable from the head through :datahike/parents. A clobbered commit
   is not reachable, so this is shorter than the number of commits made."
  [conn]
  (let [db @conn store (:store db)]
    (loop [cid (get-in db [:meta :datahike/commit-id]) n 0 seen #{}]
      (if (or (nil? cid) (seen cid))
        n
        (if-let [rec (k/get store cid nil {:sync? true})]
          (recur (first (get-in rec [:meta :datahike/parents])) (inc n) (conj seen cid))
          n)))))

(defn report
  "What actually survived, read from STORAGE.

   The registry is rebound because `d/connect` caches by store identity: calling
   it in a JVM that already holds a connection to this store hands back that
   connection, so a `report` meant to be authoritative reports the caller's own
   in-memory belief instead. It did exactly that on the first run and printed the
   opposite of what storage held.

   NOTE WHAT THIS CHECKS. `:chain` is NOT sufficient. In the observed race A's
   commit recorded B's commit as its PARENT — `commit!` resolves parents from a
   fresh branch-head read — while its DB was still the one built on the stale
   head. The lineage therefore looks perfectly healthy and the datoms are gone.
   Only the datom sets show it."
  [h]
  (let [reg  (atom {})
        conn (binding [conns/*connections* reg] (d/connect (config (:path h))))]
    (try
      (let [names (into #{} (map :v) (d/datoms @conn :aevt :name))]
        {:names       names
         :a-survived? (contains? names "a")
         :b-survived? (contains? names "b")
         :chain       (chain-length conn)})
      (finally (binding [conns/*connections* reg] (d/release conn))))))

(comment
  ;; Single-JVM smoke test of the gate itself — NOT the race. Confirms the pred
  ;; parks and releases where it should before you go to the trouble of two REPLs.
  (let [c (fresh! "/tmp/dh-race-smoke")
        h (assoc (open! "/tmp/dh-race-smoke") :path "/tmp/dh-race-smoke")]
    (arm! h)
    (let [f (transact-gated! h [{:db/id -1 :name "a"}])]
      (entered? h)
      (release! h)
      @f)
    (close! h)
    (report h)))
