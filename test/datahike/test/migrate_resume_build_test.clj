(ns datahike.test.migrate-resume-build-test
  "`{:build-indexes? true :resume? true}`: an interrupted index build continues
   from its checkpoint and publishes the database an uninterrupted build would.

   What a resumed build may reuse, and what the tests pin:

     trees     each finished tree is flushed and named in a durable GC root
               whose id is derived from the source and the options
     spool     reused when its manifest checks out file by file and its digest
               equals the one the checkpoint recorded; otherwise re-spooled,
               and the trees are kept only if the new digest is the same

   Crashes are injected between two trees, in the middle of a sort and in the
   middle of the spool; the target is a `:file` store, reconnected after each."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.gc-roots :as roots]
            [datahike.migrate :as m]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.init :as init]
            [datahike.migrate.sort :as msort]
            [konserve.store :as ks]))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])

(defn- cfg [backend]
  (cond-> {:store {:backend backend :id (java.util.UUID/randomUUID)}
           :keep-history? true :schema-flexibility :write
           :index :datahike.index/persistent-set}
    (= :file backend) (assoc-in [:store :path] (fs/temp-store-path! "dh-resume-build-"))))

(defn- dump! []
  (let [c (cfg :memory)
        _ (d/create-database c)
        conn (d/connect c)
        store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
        target {:store store :prefix "resume-build"}]
    (d/transact conn schema)
    (doseq [k (range 10)]
      (d/transact conn (vec (for [i (range 6)]
                              {:name (str "e" k "-" i) :score (* k i) :tag [(keyword (str "t" i))]}))))
    (doseq [k (range 10)]
      (d/transact conn [{:db/id [:name (str "e" k "-0")] :score (- k)}
                        [:db/retract [:name (str "e" k "-1")] :tag :t1]
                        {:db/id [:name (str "e" k "-2")] :pal [:name (str "e" k "-3")]}]))
    (d/transact conn [[:db/retractEntity [:name "e3-4"]]])
    (m/export-db @conn target {:history? true :chunk-size 50})
    (d/release conn) (d/delete-database c)
    target))

(defn- fingerprint [db]
  {:history (mapv (juxt :e :a :v :tx :added) (d/datoms (d/history db) :eavt))
   :avet (mapv (juxt :e :a :v :tx) (d/datoms db :avet))
   :max-eid (:max-eid db) :max-tx (:max-tx db) :hash (:hash db)})

(defn- create! [c] (d/create-database c) (d/connect c))
(defn- crash! [conn] (try (d/release conn) (catch Throwable _ nil)))
(defn- teardown [conn] (let [c (:config @conn)] (crash! conn) (d/delete-database c)))

(defn- reference [dump]
  (let [conn (create! (cfg :memory))]
    (try (m/import-db conn dump {:sync? true :build-indexes? true :sort-buffer 64})
         (fingerprint @conn)
         (finally (teardown conn)))))

(defn- opts [scratch] {:sync? true :build-indexes? true :resume? true
                       :scratch-dir scratch :sort-buffer 64 :spool-chunk-size 50})

(defn- scratch-of
  "The build's scratch directory under `scratch`, or nil. Its name carries the
   build id and the target store."
  [scratch id]
  (some #(when (.startsWith ^String % (str "dh-import-" id)) (str scratch "/" %))
        (or (fs/list-names scratch) [])))

(defn- fail-nth-tree
  "The k-th tree build throws: trees 1..k-1 are built and checkpointed."
  [k f]
  (let [n (atom 0)
        orig @#'init/build-index!]
    (with-redefs [init/build-index! (fn [& args]
                                      (when (= k (swap! n inc))
                                        (throw (ex-info "injected: tree build died" {:error :test/injected})))
                                      (apply orig args))]
      (f))))

(defn- fail-mid-sort
  "The k-th external sort reads half its input, then throws."
  [k f]
  (let [n (atom 0)
        orig msort/external-sort-to-file]
    (with-redefs [msort/external-sort-to-file
                  (fn [records & more]
                    (if (= k (swap! n inc))
                      (do (dorun (take 100 records))
                          (throw (ex-info "injected: sort died" {:error :test/injected})))
                      (apply orig records more)))]
      (f))))

(defn- count-tree-builds [f]
  (let [n (atom 0)
        orig @#'init/build-index!]
    (with-redefs [init/build-index! (fn [& args] (swap! n inc) (apply orig args))]
      (let [r (f)] [r @n]))))

(defn- interrupt-and-resume
  "First run under `inject` (must throw), then a resume in a NEW connection.
   `between` runs on the scratch dir between the two."
  [dump inject & {:keys [between]}]
  (let [c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        err (try (inject #(m/import-db conn dump (opts scratch))) nil
                 (catch Exception e (ex-data e)))
        _ (crash! conn)
        conn (d/connect c)
        roots-before (roots/roots (:store @conn) {:sync? true})
        _ (when between (between scratch (:import-id err)))
        [rep builds] (count-tree-builds #(m/import-db conn dump (opts scratch)))]
    (try
      {:err err :roots-before roots-before :report rep :builds builds
       :fp (fingerprint @conn) :scratch scratch
       :roots-after (roots/roots (:store @conn) {:sync? true})}
      (finally (teardown conn)))))

(deftest a-crash-between-trees-keeps-the-finished-trees
  (let [dump (dump!)
        ref (reference dump)
        r (interrupt-and-resume dump #(fail-nth-tree 3 %))]
    (is (= :import/build-interrupted (:error (:err r))) "precondition: the first run died")
    (is (contains? (:roots-before r) (:import-id (:err r))) "its checkpoint root outlived it")
    (is (= (:import-id (:err r)) (:import-id (:report r))) "the resume found it by the derived id")
    (is (= ref (:fp r)) "the published database equals an uninterrupted build's")
    (is (true? (:verified? (:report r))))
    (is (true? (:resumed? (:report r))))
    (is (true? (:spool-reused? (:report r))) "the spool was reused, not re-read")
    (is (= 2 (:trees-reused (:report r))) "the two finished trees were reused")
    (is (= 4 (:builds r)) "and only the other four were built")
    (is (not (contains? (:roots-after r) (:import-id (:report r)))) "the root is released on success")
    (is (nil? (scratch-of (:scratch r) (:import-id (:report r))))
        "and the scratch deleted")))

(deftest a-crash-in-the-middle-of-a-sort
  (let [dump (dump!)
        ref (reference dump)
        ;; sort 2 is the :aevt family's, after :eavt's two trees
        r (interrupt-and-resume dump #(fail-mid-sort 2 %))]
    (is (= :import/build-interrupted (:error (:err r))))
    (is (= ref (:fp r)))
    (is (true? (:verified? (:report r))))
    (is (= 2 (:trees-reused (:report r))) ":eavt's trees survived")
    (is (= 4 (:builds r)))))

(deftest a-finished-family-leaves-no-sorted-file-behind
  (let [dump (dump!)
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        ;; tree 4 is :aevt's temporal: :eavt finished, :aevt's sort still owned
        err (try (fail-nth-tree 4 #(m/import-db conn dump (opts scratch))) nil
                 (catch Exception e (ex-data e)))
        dir (scratch-of scratch (:import-id err))
        manifest (read-string (slurp (str dir "/spool.edn")))
        listed (set (concat (:files manifest) (:schema-files manifest) ["spool.edn" ".lock"]))
        others (remove listed (fs/list-names dir))]
    (is (some? (:import-id err)))
    (is (<= (count others) 1) (str "at most the in-flight family's sorted file, got " (vec others)))
    (m/abandon-import! conn {:import-id (:import-id err) :scratch-dir scratch})
    (teardown conn)))

(deftest a-damaged-spool-is-re-spooled-and-the-trees-kept
  (let [dump (dump!)
        ref (reference dump)
        r (interrupt-and-resume
           dump #(fail-nth-tree 3 %)
           :between (fn [scratch id]
                      (let [dir (scratch-of scratch id)
                            f (first (:files (read-string (slurp (str dir "/spool.edn")))))]
                        (spit (str dir "/" f) "garbage" :append true))))]
    (is (= ref (:fp r)))
    (is (false? (:spool-reused? (:report r))) "the damaged spool was not trusted")
    (is (= 2 (:trees-reused (:report r))) "the re-spooled records had the same digest, so the trees stand")))

(deftest a-crash-while-spooling-starts-the-spool-over
  (let [dump (dump!)
        ref (reference dump)
        n (atom 0)
        boom (map (fn [r] (when (= 150 (swap! n inc)) (throw (ex-info "injected" {}))) r))
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)]
    ;; the same transform with a throw injected: the caller says so with the
    ;; same :resume-key (a CHANGED transform is refused, see below)
    (is (thrown? Exception (m/import-db conn dump (assoc (opts scratch) :xform boom :resume-key "id-v1"))))
    (crash! conn)
    (let [conn (d/connect c)
          rep (m/import-db conn dump (assoc (opts scratch) :xform (map identity) :resume-key "id-v1"))]
      (is (= ref (fingerprint @conn)))
      (is (false? (:spool-reused? rep)))
      (is (zero? (:trees-reused rep)))
      (is (true? (:verified? rep)))
      (teardown conn))))

(deftest resuming-a-published-build-only-verifies
  (let [dump (dump!)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! (cfg :file))
        first-rep (m/import-db conn dump (opts scratch))
        before (fingerprint @conn)
        again (m/import-db conn dump (opts scratch))]
    (is (true? (:verified? first-rep)))
    (is (= before (fingerprint @conn)))
    (is (= {:status :complete} (:resumed-from again)))
    (is (true? (:verified? again)))
    (is (= (:tx-count first-rep) (:tx-count again)))
    (teardown conn)))

(deftest abandon-import-gives-the-build-up
  (let [dump (dump!)
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        err (try (fail-nth-tree 3 #(m/import-db conn dump (opts scratch))) nil
                 (catch Exception e (ex-data e)))
        id (:import-id err)
        res (m/abandon-import! conn {:import-id id :scratch-dir scratch})]
    (is (= [id] (:released res)))
    (is (not (contains? (roots/roots (:store @conn) {:sync? true}) id)))
    (is (nil? (scratch-of scratch id)))
    (let [rep (m/import-db conn dump (opts scratch))]
      (is (false? (:resumed? rep)) "the next run starts over")
      (is (zero? (:trees-reused rep)))
      (is (true? (:verified? rep))))
    (teardown conn)))

;; ---------------------------------------------------------------------------
;; review findings (#1130)

(defn- crashed-target
  "A file target whose resumable build died at the 3rd tree. [conn cfg scratch err]"
  [dump]
  (let [c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        err (try (fail-nth-tree 3 #(m/import-db conn dump (opts scratch))) nil
                 (catch Exception e (ex-data e)))]
    (crash! conn)
    [(d/connect c) c scratch err]))

(defn- refusal [f] (try (f) nil (catch Exception e (:error (ex-data e)))))

(deftest changed-options-are-refused-not-published
  (testing "A resume under different options used to reuse the spool the old
            options wrote and publish it — counts replayed from the old manifest,
            so verification could not see it."
    (let [dump (dump!)
          [conn _ scratch _] (crashed-target dump)]
      (is (= :import/resume-mismatch
             (refusal #(m/import-db conn dump (assoc (opts scratch) :eids :allocate))))
          "another :eids")
      (is (= :import/resume-mismatch
             (refusal #(m/import-db conn dump (assoc (opts scratch)
                                                     :xform (remove (fn [r] (= :score (nth r 1))))
                                                     :resume-key "drop-score"))))
          "another transform")
      (is (= :import/resume-needs-key
             (refusal #(m/import-db conn dump (assoc (opts scratch) :xform (map identity)))))
          "a transform it cannot compare")
      (is (empty? (d/q '[:find ?e :where [?e :name _]] @conn)) "and nothing was published")
      (teardown conn))))

(deftest a-collection-between-crash-and-resume-spares-the-checkpoint
  (let [dump (dump!)
        ref (reference dump)
        [conn _ scratch _] (crashed-target dump)]
    (d/gc-storage conn (java.util.Date.) {:min-age-ms 0})
    (let [rep (m/import-db conn dump (opts scratch))]
      (is (= 2 (:trees-reused rep)) "the checkpointed trees survived the sweep")
      (is (= ref (fingerprint @conn)))
      (is (true? (:verified? rep))))
    (teardown conn)))

(deftest an-expired-checkpoint-starts-over-correctly
  (let [dump (dump!)
        ref (reference dump)
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        short (assoc (opts scratch) :checkpoint-ttl-ms 300)]
    (try (fail-nth-tree 3 #(m/import-db conn dump short)) (catch Exception _ nil))
    (crash! conn)
    (Thread/sleep 1000)
    (let [conn (d/connect c)]
      (roots/reap-expired! (:store @conn) {:sync? true})
      (d/gc-storage conn (java.util.Date.) {:min-age-ms 0})
      (let [rep (m/import-db conn dump (opts scratch))]
        (is (false? (:resumed? rep)) "the root is gone, so this is a new build")
        (is (zero? (:trees-reused rep)) "and it trusts no tree")
        (is (= ref (fingerprint @conn)))
        (is (true? (:verified? rep))))
      (teardown conn))))

(deftest a-failure-after-publish-is-itself-and-cleans-up
  (testing "Verification and the dangling-ref check run after the publishing
            commit. Their failure is not an interrupted build."
    (let [dump (dump!)
          c (cfg :file)
          scratch (fs/temp-store-path! "dh-resume-scratch-")
          conn (create! c)
          err (with-redefs [m/dangling-refs (fn [& _] (throw (ex-info "injected: after publish"
                                                                      {:error :test/after-publish})))]
                (try (m/import-db conn dump (assoc (opts scratch) :check-refs? true)) nil
                     (catch Exception e (ex-data e))))]
      (is (= :test/after-publish (:error err)) "the original error, not :import/build-interrupted")
      (is (empty? (filter (fn [[_ e]] (get-in e [:owner :datahike/import]))
                          (roots/roots (:store @conn) {:sync? true})))
          "the root is released")
      (is (empty? (filter #(.startsWith ^String % "dh-import-") (or (fs/list-names scratch) [])))
          "and the scratch deleted")
      (teardown conn))))

(deftest a-crash-between-publish-and-cleanup
  (let [dump (dump!)
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        _ (with-redefs [m/quiet-release-root! (fn [& _] (throw (ex-info "injected: died in cleanup" {})))]
            (try (m/import-db conn dump (opts scratch)) (catch Exception _ nil)))
        _ (crash! conn)
        conn (d/connect c)
        before (fingerprint @conn)
        rep (m/import-db conn dump (opts scratch))]
    (is (= {:status :complete} (:resumed-from rep)) "the publish had landed")
    (is (true? (:verified? rep)))
    (is (= before (fingerprint @conn)))
    (is (empty? (filter (fn [[_ e]] (get-in e [:owner :datahike/import]))
                        (roots/roots (:store @conn) {:sync? true})))
        "the leftover root is released now")
    (is (empty? (filter #(.startsWith ^String % "dh-import-") (or (fs/list-names scratch) [])))
        "and the leftover scratch deleted")
    (teardown conn)))

(deftest two-builds-of-one-target-do-not-share-a-spool
  (let [dump (dump!)
        ref (reference dump)
        c (cfg :file)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        conn (create! c)
        run #(try (m/import-db conn dump (opts scratch)) (catch Exception e (ex-data e)))
        a (future (run))
        b (future (run))
        rs [(deref a 120000 ::hung) (deref b 120000 ::hung)]]
    (is (not-any? #{::hung} rs))
    (is (some :verified? rs) "one of them published")
    (is (every? #(or (:verified? %)
                     (#{:import/scratch-in-use :import/build-interrupted} (:error %)))
                rs)
        (str "the other was refused: " (pr-str rs)))
    (is (= ref (fingerprint @conn)))
    (teardown conn)))

(deftest two-targets-from-one-dump-keep-their-own-scratch
  (let [dump (dump!)
        scratch (fs/temp-store-path! "dh-resume-scratch-")
        c1 (cfg :file) c2 (cfg :file)
        conn1 (create! c1) conn2 (create! c2)
        e1 (try (fail-nth-tree 3 #(m/import-db conn1 dump (opts scratch))) nil (catch Exception e (ex-data e)))
        e2 (try (fail-nth-tree 3 #(m/import-db conn2 dump (opts scratch))) nil (catch Exception e (ex-data e)))]
    (is (= (:import-id e1) (:import-id e2)) "the same build id: same dump, same options")
    (is (= 2 (count (filter #(.startsWith ^String % "dh-import-") (fs/list-names scratch))))
        "but two scratch directories")
    (let [r1 (m/import-db conn1 dump (opts scratch))
          r2 (m/import-db conn2 dump (opts scratch))]
      (is (true? (:spool-reused? r1)))
      (is (true? (:spool-reused? r2)) "neither deleted the other's spool")
      (is (= (fingerprint @conn1) (fingerprint @conn2))))
    (teardown conn1) (teardown conn2)))
