(ns datahike.test.delete-database-test
  "`delete-database` must have actually deleted the database when it returns.

   Regression: `-delete-database*` ended with `(ks/delete-store (:store config))`
   inside a `go-try-`, without awaiting it. konserve's `delete-store` defaults to
   `{:sync? false}` and the async backends hand back a channel, so the go-try- yielded
   that CHANNEL as its value — `delete-database` resolved (returning a raw core.async
   channel to the caller) while the store was still being deleted.

   Observable: `delete-database` followed immediately by `database-exists?` still
   reported the database as present, and delete-then-recreate raced its own deletion.
   On S3 nothing was deleted at all, because konserve-s3's `-delete-store` had the same
   missing await — so offboarding a tenant / GDPR erasure silently did not happen.

   Needs konserve >= 0.9.357, where every backend's `-delete-store` honours `:sync?`."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   #?(:clj [clojure.core.async.impl.protocols]
      :cljs [cljs.core.async.impl.protocols])
   [datahike.api :as d])
  #?(:clj (:import [java.util UUID])))

(defn- uuid* []
  #?(:clj (UUID/randomUUID) :cljs (random-uuid)))

(defn- mem-cfg []
  {:store {:backend :memory :id (uuid*)}
   :keep-history? false
   :schema-flexibility :read})

#?(:clj
   (defn- file-cfg []
     {:store {:backend :file
              :path (str (System/getProperty "java.io.tmpdir")
                         "/dh-delete-db-test-" (System/currentTimeMillis) "-" (rand-int 100000))
              :id (uuid*)}
      :keep-history? false
      :schema-flexibility :read}))

(defn- seed! [cfg]
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn [{:name "alice"}])
    (d/release conn)))

(defn- channel? [x]
  #?(:clj  (satisfies? clojure.core.async.impl.protocols/ReadPort x)
     :cljs (satisfies? cljs.core.async.impl.protocols/ReadPort x)))

(deftest delete-database-does-not-return-a-channel
  ;; THE discriminating assertion. Without the `<?-`, `-delete-database*` yields
  ;; konserve's un-awaited channel as its value, and `delete-database` hands that raw
  ;; core.async channel to the caller — while the deletion may still be in flight.
  ;;
  ;; This is what the backend-level assertions below CANNOT catch: konserve's :memory
  ;; and :file perform the deletion eagerly and only wrap the *result* in a channel, so
  ;; a missing await produces no observable race there. It is the genuinely deferred
  ;; backends (konserve-s3 dispatches to a virtual thread) where the un-awaited channel
  ;; means the objects are still being deleted — or, before konserve-s3#12, never were.
  ;; The leaked channel is the one symptom visible on every backend.
  (testing "memory"
    (let [cfg (mem-cfg)]
      (seed! cfg)
      (is (not (channel? (d/delete-database cfg)))
          "delete-database must not return a core.async channel — it must await the deletion")))
  #?(:clj
     (testing "file"
       (let [cfg (file-cfg)]
         (seed! cfg)
         (is (not (channel? (d/delete-database cfg)))
             "delete-database must not return a core.async channel — it must await the deletion")))))

(deftest delete-database-is-complete-when-it-returns
  (testing "memory: database is gone IMMEDIATELY after delete-database returns"
    (let [cfg (mem-cfg)]
      (seed! cfg)
      (is (true? (d/database-exists? cfg)))
      (d/delete-database cfg)
      ;; No sleep, no retry: if this needs a grace period, delete-database is lying.
      (is (false? (d/database-exists? cfg))
          "delete-database returned before the database was actually deleted")))

  #?(:clj
     (testing "file: database is gone IMMEDIATELY after delete-database returns"
       (let [cfg (file-cfg)]
         (seed! cfg)
         (is (true? (d/database-exists? cfg)))
         (d/delete-database cfg)
         (is (false? (d/database-exists? cfg))
             "delete-database returned before the database was actually deleted")))))

(deftest delete-then-recreate-does-not-race
  (testing "a database can be recreated immediately after deletion"
    ;; This is the shape that bit real callers: deleting a tenant and recreating it
    ;; (a reset, a test fixture, a re-provision) raced the un-awaited deletion, so the
    ;; new database could be clobbered by the old one's delete landing afterwards.
    (let [cfg (mem-cfg)]
      (seed! cfg)
      (d/delete-database cfg)
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:name "bob"}])
        (is (= #{["bob"]}
               (d/q '[:find ?n :where [?e :name ?n]] @conn))
            "recreated database must contain only its own data")
        (d/release conn))
      (d/delete-database cfg))))
