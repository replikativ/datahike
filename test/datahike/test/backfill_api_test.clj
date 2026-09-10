(ns datahike.test.backfill-api-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.backfill.avet :as avet]
            [datahike.test.utils :as utils]))

(deftest begin-and-cancel-isolate-throwing-transaction-listeners
  (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                              :attribute-refs? false :keep-history? true
                              :index :datahike.index/persistent-set
                              :writer {:backend :self :writer-ownership :shared :require-fencing :process}})
        config (:config @conn) entered (promise) proceed (promise)
        begin-notified (promise) cancel-notified (promise) build avet/build!]
    (try
      (d/transact conn [{:db/ident :value :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                        {:db/id 10000 :value 1}])
      (d/listen conn ::throws (fn [_] (throw (ex-info "Deliberate listener failure" {}))))
      (d/listen conn ::observes
                (fn [report]
                  (when-let [id (:avet-build-id report)] (deliver begin-notified id))
                  (when (= :canceled (get-in report [:db-after :avet-build-result :status]))
                    (deliver cancel-notified (get-in report [:db-after :avet-build-result :id])))))
      (with-redefs [avet/build! (fn [& args]
                                  (deliver entered true)
                                  (when (= ::timeout (deref proceed 10000 ::timeout))
                                    (throw (ex-info "API listener test build gate timed out" {})))
                                  (apply build args))]
        (try
          (let [accepted @(d/begin-avet-build! conn {:value {:db/index true}})
                id (:avet-build-id accepted)]
            (is (uuid? id))
            (is (= id (deref begin-notified 10000 ::timeout)))
            (is (true? (deref entered 10000 false)))
            (let [canceled @(d/cancel-avet-build! conn id)]
              (is (= :canceled (get-in canceled [:db-after :avet-build-result :status])))
              (is (= id (deref cancel-notified 10000 ::timeout))))
            (is (not (get-in @conn [:schema :value :db/index]))))
          (finally (deliver proceed true))))
      (finally
        (deliver proceed true)
        (d/unlisten conn ::throws) (d/unlisten conn ::observes)
        (d/release conn) (d/delete-database config)))))
