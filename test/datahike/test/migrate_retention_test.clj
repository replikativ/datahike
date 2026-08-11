(ns datahike.test.migrate-retention-test
  "The export paths must not RETAIN the record stream.

   `async+sync` compiles the same source to a plain `let` under `:sync? true` and
   to a core.async `go` block under `:sync? false`. core.async only decomposes
   subforms that CONTAIN a park — but once a park anywhere in the enclosing form
   forces decomposition, every `let`/`loop` binding in that region becomes a local
   inside the state machine's `try`, where Clojure's locals-clearing does not
   apply, and one read from a second block goes into the state array, which is
   never nulled. A named record stream is therefore held for the whole block: the
   entire database.

   Measured, 400k records, used heap sampled inside the block (baseline 15 MB):

       bind -> park -> consume                     63 MB
       bind -> consume SYNCHRONOUSLY -> park       63 MB   <- no park in between
       bind -> if(parking arm NOT taken) -> use    63 MB   <- untaken arm leaks too
       inlined into the arm                        15 MB
       loop parking every iteration, seq unnamed   14 MB

   `default-sync?` is FALSE on ClojureScript, so async is the only mode on Node
   and none of this is visible on the JVM default.

   ## Why a WeakReference and not a heap measurement

   Sampling used heap needs `-Xmx` tuning, a fixture big enough to hurt, and a
   threshold — all flaky. The actual property is REACHABILITY: while the export is
   mid-stream, nothing should still point at the head. A `WeakReference` tests
   exactly that, deterministically, on a three-datom database.

   These tests would have passed against the pre-fix code on the JVM default. They
   pin `{:sync? false}` for that reason."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]
            [clojure.core.async :as a])
  (:import [java.lang.ref WeakReference]))

(defn- fresh-conn []
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? true
                   :schema-flexibility :write}))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- populate! [conn]
  (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])
  (doseq [n ["Ann" "Bob" "Cid" "Dee" "Eve"]]
    (d/transact conn [{:name n}]))
  conn)

(defn- watching-sorted-record-seq
  "Redef `sorted-record-seq` to stash a WeakReference to every stream it hands
   out. The var is private, so reach it the way `migrate-source-test` reaches
   `run-import`.

   It must be THIS function and not `export-record-seq`. Under `:sort? true` —
   the default — `external-sort` DRAINS `export-record-seq`'s output and returns
   a different seq, so watching the inner one passes whether or not the outer one
   is bound. The first version of this test did exactly that: reintroducing the
   leaky binding left it green, which is how a retention test becomes a
   decoration. The object that gets bound is the one to watch."
  [seen]
  (let [orig @#'m/sorted-record-seq]
    (fn [db opts tmp-dir]
      (let [s (orig db opts tmp-dir)]
        (swap! seen conj (WeakReference. s))
        s))))

(defn- any-still-reachable? [seen]
  (System/gc)
  (Thread/sleep 100)
  (boolean (some #(some? (.get ^WeakReference %)) @seen)))

(deftest export-to-sink-does-not-retain-the-stream
  (testing "mid-stream, nothing points at the head of the record seq"
    (let [conn (populate! (fresh-conn))
          seen (atom [])
          checked (atom nil)]
      (with-redefs [m/sorted-record-seq (watching-sorted-record-seq seen)]
        (a/<!! (m/export-to-sink
                @conn
                {:open  (fn [_] (a/go :ctx))
                 :write (fn [ctx _recs]
                          (a/go
                            ;; On the first chunk the stream is live and being
                            ;; consumed; if the head is still REACHABLE, someone
                            ;; has bound it to a name that outlives its use.
                            (when (nil? @checked)
                              (reset! checked (any-still-reachable? seen)))
                            ctx))
                 :close (fn [_] (a/go :done))}
                {:history? true :chunk-size 1 :sync? false})))
        (is (seq @seen) "precondition: sorted-record-seq was actually called")
        (is (false? @checked)
            "the record stream was still reachable mid-export — a binding is
             holding its head, which in async mode retains the whole database")
      (teardown conn))))

(deftest export-db-does-not-retain-the-stream-on-either-arm
  (testing "the filesystem arm too. The store arm's park decomposes the shared
            `if`, so a binding shared by both arms is promoted for the
            NON-parking arm as well — measured, a seq consumed in the untaken
            arm is retained exactly the same. Fixing only the store arm would
            leave `write-chunked!` holding the whole dump."
    (let [conn (populate! (fresh-conn))
          seen (atom [])
          dir  (str "/tmp/claude-1000/retention-" (System/currentTimeMillis))]
      (with-redefs [m/sorted-record-seq (watching-sorted-record-seq seen)]
        (a/<!! (m/export-db @conn dir {:history? true :sync? false})))
      (is (seq @seen) "precondition: sorted-record-seq was called")
      (is (false? (any-still-reachable? seen))
          "after the export the stream must be unreachable; if it is not, a local
           in the go block is still holding it")
      (teardown conn))))
