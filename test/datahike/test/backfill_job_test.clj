(ns datahike.test.backfill-job-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.backfill.job :as job]
            [datahike.db :as db]
            [datahike.test.utils :as utils]
            [datahike.writing :as writing]
            [konserve.core :as k]))

(deftest descriptor-retains-only-canonical-durable-plan-fields
  (let [database (d/db-with (db/empty-db nil {:schema-flexibility :write})
                            [{:db/ident :existing :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one :db/index true}
                             {:db/ident :target :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
        plan {:patch {:target {:db/index true}} :attrs #{:target}
              :private-runtime (Object.)}
        descriptor (job/descriptor database plan)]
    (is (= #{:id :status :patch :attrs :schema-token :branch} (set (keys descriptor))))
    (is (= :building (:status descriptor)))
    (is (contains? (:attrs descriptor) :target))
    (is (contains? (:attrs descriptor) :existing))
    (is (job/matches? database descriptor))
    (is (not (job/matches? (assoc-in database [:config :branch] :elsewhere) descriptor)))
    (is (not (job/matches? (assoc-in database [:schema :target :db/doc] "changed") descriptor)))
    (is (not= (:id descriptor) (:id (job/descriptor database plan))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (job/descriptor (assoc database :avet-build descriptor) plan)))))

(deftest begin-and-cancel-reports-do-not-activate-schema
  (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                              :index :datahike.index/persistent-set
                              :writer {:backend :self :writer-ownership :shared
                                       :require-fencing :process}})
        config (:config @conn)]
    (try
      (d/transact conn [{:db/ident :target :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (let [before @conn
            report (writing/begin-avet-build! before {:target {:db/unique :db.unique/value}})
            after (:db-after report)
            id (get-in after [:avet-build :id])
            with-duplicates (d/db-with after [{:db/id 10000 :target 1}
                                              {:db/id 10001 :target 1}])
            canceled (:db-after (writing/cancel-avet-build! after id))]
        (is (identical? before (:db-before report)))
        (is (= before @conn))
        (is (nil? (:avet-build @conn)))
        (is (= (:schema before) (:schema after) (:schema canceled)))
        (is (job/matches? after (:avet-build after)))
        (is (= 2 (count (d/q '[:find ?e :where [?e :target 1]] with-duplicates))))
        (is (nil? (:avet-build canceled)))
        (is (= {:id id :status :canceled :reason :canceled} (:avet-build-result canceled)))
        (is (thrown? clojure.lang.ExceptionInfo
                     (writing/cancel-avet-build! after (random-uuid)))))
      (finally (d/release conn) (d/delete-database config)))))

(deftest capability-never-confuses-exclusive-ownership-with-fencing
  (let [required (first k/conditional-write-domains)
        database {:config {:index :datahike.index/persistent-set :crypto-hash? false
                           :writer {:backend :self :writer-ownership :shared
                                    :require-fencing required}}
                  :store {} :datahike.writing/head-revision (random-uuid)}]
    (with-redefs [k/conditional-write? (fn [_ domain] (= required domain))]
      (is (nil? (job/supported! database)))
      (doseq [unsupported [(update database :config dissoc :crypto-hash?)
                           (assoc-in database [:config :writer :writer-ownership] :exclusive)
                           (assoc-in database [:config :writer :backend] :remote)
                           (update-in database [:config :writer] dissoc :require-fencing)
                           (assoc-in database [:config :index] :datahike.index/hitchhiker-tree)
                           (assoc-in database [:store :datahike/diff-buf-size] 4)
                           (dissoc database :datahike.writing/head-revision)]]
        (is (thrown? clojure.lang.ExceptionInfo (job/supported! unsupported)))))
    (with-redefs [k/conditional-write? (constantly false)]
      (is (thrown? clojure.lang.ExceptionInfo (job/supported! database))))))
