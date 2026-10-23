(ns datahike.test.gc-fail-closed-test
  "A collection whose MARK did not complete must not SWEEP.

   Regression for the dev.simm.is system-DB loss (2026-09-29): a live server's
   periodic `gc-storage` left exactly one key in the store — `:branches` — and
   deleted every head, commit and index node. The mark's go block had died on a
   `java.lang.AssertionError` (core.async's 1024-pending-takes limit, reached
   by an unrelated leak in the same JVM). `go-try` catches `Exception`, not
   `Error`, so the dead walk's channel closed EMPTY, `async/merge` yielded no
   result for that branch, and the sweep ran against a whitelist of
   `#{:branches}`.

   Each test injects one way a read inside the mark can come back empty and
   asserts the store is untouched. Without the fail-closed checks every one of
   them deletes the whole database."
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.migrate.fs :as fs]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]))

(def ^:private schema [{:db/ident       :age
                        :db/cardinality :db.cardinality/one
                        :db/valueType   :db.type/long}])

(defn- fresh-conn [label]
  (let [cfg {:store {:backend :file
                     :path (fs/temp-store-path! label)
                     :id (java.util.UUID/randomUUID)}
             ;; Exclusive: no sweep floor, so anything the mark misses is
             ;; deleted at once — the condition the server was in once its
             ;; system DB had been idle for a while.
             :writer {:backend :self :writer-ownership :exclusive}
             :keep-history? true
             :schema-flexibility :write}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn schema)
      (dotimes [i 20] (d/transact conn [{:age i}]))
      [cfg conn])))

(defn- store-keys [conn]
  (set (map :key (k/keys (:store @conn) {:sync? true}))))

(defn- in-mark? []
  (some #(re-find #"datahike\.gc\$reachable_in_branch" (.getClassName ^StackTraceElement %))
        (.getStackTrace (Throwable.))))

(defn- closed-chan [] (doto (async/chan) async/close!))

(defn- gc-error [conn]
  (try (<?? S (d/gc-storage conn (Date. 0))) nil
       (catch Throwable e e)))

(defn- assert-untouched [cfg conn before e expected-type]
  (is (some? e) "the collection refuses instead of sweeping")
  (is (= expected-type (:type (ex-data e))))
  (is (= before (store-keys conn)) "not one key was deleted")
  (let [c2 (d/connect cfg)]
    (try
      (is (= 20 (count (d/q '[:find ?e :where [?e :age]] @c2)))
          "the database still opens and reads from storage")
      (finally (d/release c2)))))

(deftest a-mark-that-dies-on-an-error-does-not-sweep
  (testing "an Error escaping a branch walk (the server's AssertionError)"
    (let [[cfg conn] (fresh-conn "dh-gc-fail-closed-error")
          before (store-keys conn)
          orig k/get-in]
      (try
        (let [e (with-redefs [k/get-in (fn [store ks & more]
                                         (if (in-mark?)
                                           (throw (AssertionError. "No more than 1024 pending takes are allowed on a single channel."))
                                           (apply orig store ks more)))]
                  (gc-error conn))]
          ;; The walk catches the Error itself and returns it as a refusal,
          ;; with the original Error as cause; `require-complete-mark` stays
          ;; as the backstop for a walk that closes without any result.
          (assert-untouched cfg conn before e :datahike/gc-mark-failed)
          (is (some #(instance? AssertionError %) (take-while some? (iterate ex-cause e)))))
        (finally (d/release conn))))))

(deftest unreadable-branches-do-not-sweep
  (testing "`:branches` reads back as nothing (a dead read, not an empty store)"
    (let [[cfg conn] (fresh-conn "dh-gc-fail-closed-branches")
          before (store-keys conn)
          orig k/get]
      (try
        (let [e (with-redefs [k/get (fn [store key & more]
                                      (if (= key :branches)
                                        (closed-chan)
                                        (apply orig store key more)))]
                  (gc-error conn))]
          (assert-untouched cfg conn before e :gc-branches-unreadable))
        (finally (d/release conn))))))

(deftest an-unreadable-branch-head-does-not-sweep
  (testing "the branch head record reads back as nothing during the mark"
    (let [[cfg conn] (fresh-conn "dh-gc-fail-closed-head")
          before (store-keys conn)
          orig k/get]
      (try
        (let [e (with-redefs [k/get (fn [store key & more]
                                      (if (and (= key :db) (in-mark?))
                                        (closed-chan)
                                        (apply orig store key more)))]
                  (gc-error conn))]
          (assert-untouched cfg conn before e :gc-branch-head-unreadable))
        (finally (d/release conn))))))

(deftest a-healthy-collection-still-collects
  (testing "the checks refuse only an incomplete mark — a normal sweep proceeds"
    (let [[cfg conn] (fresh-conn "dh-gc-fail-closed-control")]
      (try
        (let [swept (<?? S (d/gc-storage conn (Date.)))]
          (is (seq swept) "superseded snapshots are collected as before")
          (let [c2 (d/connect cfg)]
            (try (is (= 20 (count (d/q '[:find ?e :where [?e :age]] @c2))))
                 (finally (d/release c2)))))
        (finally (d/release conn))))))
