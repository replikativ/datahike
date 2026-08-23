(ns datahike.test.versioning-fencing-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [konserve.core :as k])
  (:import [java.util.concurrent CyclicBarrier TimeUnit]
           [java.util.concurrent.atomic AtomicInteger]))

(defn- cfg [tag]
  (let [id (random-uuid)]
    {:store {:backend :file
             :path (str (System/getProperty "java.io.tmpdir") "/dh-versioning-fence-" tag "-" id)
             :id id}
     :keep-history? false
     :schema-flexibility :write}))

(defn- call-result [f]
  (try
    (f)
    ::ok
    (catch Throwable e e)))

(defn- await-result [f]
  (deref f 30000 ::timed-out))

(defn- revisioned-read? [args]
  (boolean (some #(and (map? %) (:with-revision? %)) args)))

(deftest concurrent-branch-creations-compose-in-the-registry
  (testing "two creators of different branches cannot overwrite one another's
            update to the :branches GC whitelist"
    (let [c (cfg "different")]
      (d/delete-database c)
      (d/create-database c)
      (let [conn (d/connect c)
            store (:store @conn)
            orig-get k/get
            reads (AtomicInteger.)
            barrier (CyclicBarrier. 2)]
        (try
          (with-redefs [k/get (fn [s key & args]
                                (let [result (apply orig-get s key args)]
                                  (when (and (= key :branches)
                                             (revisioned-read? args)
                                             (<= (.incrementAndGet reads) 2))
                                    (.await barrier 30 TimeUnit/SECONDS))
                                  result))]
            (let [a (future (call-result #(v/branch! conn :db :a)))
                  b (future (call-result #(v/branch! conn :db :b)))]
              (is (= [::ok ::ok] [(await-result a) (await-result b)]))))
          (is (= #{:db :a :b} (k/get store :branches nil {:sync? true})))
          (is (some? (k/get store :a nil {:sync? true})))
          (is (some? (k/get store :b nil {:sync? true})))
          (finally
            (d/release conn)
            (d/delete-database c)))))))

(deftest concurrent-creators-of-one-name-have-one-winner
  (testing "the target head itself is conditional, including when the key is
            absent, so equal-name creators cannot silently overwrite"
    (let [c (cfg "same")]
      (d/delete-database c)
      (d/create-database c)
      (let [conn (d/connect c)
            store (:store @conn)
            orig-get k/get
            reads (AtomicInteger.)
            barrier (CyclicBarrier. 2)]
        (try
          (let [results
                (with-redefs [k/get (fn [s key & args]
                                      (let [result (apply orig-get s key args)]
                                        (when (and (= key :same)
                                                   (revisioned-read? args)
                                                   (<= (.incrementAndGet reads) 2))
                                          (.await barrier 30 TimeUnit/SECONDS))
                                        result))]
                  (let [a (future (call-result #(v/branch! conn :db :same)))
                        b (future (call-result #(v/branch! conn :db :same)))]
                    [(await-result a) (await-result b)]))
                errors (remove #{::ok} results)]
            (is (= 1 (count (filter #{::ok} results))))
            (is (= 1 (count errors)))
            (is (= :branch-already-exists (:type (ex-data (first errors))))))
          (is (= #{:db :same} (k/get store :branches nil {:sync? true})))
          (finally
            (d/release conn)
            (d/delete-database c)))))))

(deftest concurrent-create-and-delete-compose-in-the-registry
  (testing "a branch deletion and an unrelated creation both survive"
    (let [c (cfg "create-delete")]
      (d/delete-database c)
      (d/create-database c)
      (let [conn (d/connect c)
            store (:store @conn)]
        (v/branch! conn :db :old)
        (let [orig-get k/get
              reads (AtomicInteger.)
              barrier (CyclicBarrier. 2)]
          (try
            (with-redefs [k/get (fn [s key & args]
                                  (let [result (apply orig-get s key args)]
                                    (when (and (= key :branches)
                                               (revisioned-read? args)
                                               (<= (.incrementAndGet reads) 2))
                                      (.await barrier 30 TimeUnit/SECONDS))
                                    result))]
              (let [created (future (call-result #(v/branch! conn :db :new)))
                    deleted (future (call-result #(v/delete-branch! conn :old)))]
                (is (= [::ok ::ok]
                       [(await-result created) (await-result deleted)]))))
            (is (= #{:db :new} (k/get store :branches nil {:sync? true})))
            (finally
              (d/release conn)
              (d/delete-database c))))))))

(deftest concurrent-database-creation-has-one-winner
  (testing "the initial :db head is claimed with an absent-key CAS"
    (let [c (cfg "database")
          orig-get k/get
          reads (AtomicInteger.)
          barrier (CyclicBarrier. 2)]
      (d/delete-database c)
      (try
        (let [results
              (with-redefs [k/get (fn [s key & args]
                                    (let [result (apply orig-get s key args)]
                                      (when (and (= key :db)
                                                 (nil? result)
                                                 (<= (.incrementAndGet reads) 2))
                                        (.await barrier 30 TimeUnit/SECONDS))
                                      result))]
                (let [a (future (call-result #(d/create-database c)))
                      b (future (call-result #(d/create-database c)))]
                  [(await-result a) (await-result b)]))
              errors (remove #{::ok} results)]
          (is (= 1 (count (filter #{::ok} results))))
          (is (= 1 (count errors)))
          ;; Filestore may refuse the second store creator before Datahike
          ;; reaches its :db CAS. Object stores let both connect and the CAS is
          ;; the arbiter. Either is a safe, explicit loser.
          (is (or (= :db-already-exists (:type (ex-data (first errors))))
                  (re-find #"File store already exists" (ex-message (first errors))))
              (pr-str {:class (class (first errors))
                       :message (ex-message (first errors))
                       :data (ex-data (first errors))
                       :cause (some-> (ex-cause (first errors)) class)}))
          (is (d/database-exists? c)))
        (finally
          (d/delete-database c))))))

(deftest database-creation-passes-the-absent-revision
  (let [c (cfg "database-option")
        orig-assoc k/assoc
        db-opts (atom nil)]
    (d/delete-database c)
    (try
      (with-redefs [k/assoc (fn [store key value & args]
                              (when (= key :db)
                                (reset! db-opts (last args)))
                              (apply orig-assoc store key value args))]
        (d/create-database c))
      (is (= k/absent (:expected-revision @db-opts))
          "the check-then-create path closes its race at the mutable head write")
      (finally
        (d/delete-database c)))))
