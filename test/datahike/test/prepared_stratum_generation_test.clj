(ns datahike.test.prepared-stratum-generation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.stratum]
   [datahike.versioning :as dv]
   [konserve.core :as k]
   [stratum.storage :as ss]
   [superv.async :refer [<?? S]])
  (:import [java.util Date]))

(defn- await-ready [conn ident]
  (let [deadline (+ (System/currentTimeMillis) 10000)]
    (loop []
      (let [status (get-in (d/db conn) [:schema ident :db.secondary/status])]
        (cond
          (= :ready status) status
          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 20) (recur))
          :else status)))))

(defn- stored-key-map [db branch]
  (get-in (k/get (:store db) branch nil {:sync? true})
          [:secondary-index-keys :idx/columns]))

(defn- row-count [db]
  (sec/-estimate (get-in db [:secondary-indices :idx/columns]) {}))

(deftest datahike-is-the-only-publication-root-for-stratum
  (let [store-id (random-uuid)
        cfg {:store {:backend :file
                     :id store-id
                     :path (str "/tmp/datahike-prepared-stratum-" store-id)}
             :writer {:backend :self :writer-ownership :exclusive}
             :keep-history? false
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [main (d/connect cfg)]
      (try
        (d/transact main [{:db/ident :p/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact main [{:db/ident :idx/columns
                           :db.secondary/type :stratum
                           :db.secondary/attrs [:p/name]}])
        (is (= :ready (await-ready main :idx/columns)))
        (d/transact main [{:p/name "Alice"}])

        (let [main-db (d/db main)
              store (:store main-db)
              main-key (stored-key-map main-db :db)
              main-root (:dataset-commit-id main-key)]
          (testing "the primary key-map names an exact generation, not a native ref"
            (is (uuid? main-root))
            (is (= :stratum (:type main-key)))
            (is (nil? (:branch main-key)))
            (is (nil? (ss/list-dataset-branches store)))
            (is (nil? (ss/load-dataset-head store "db")))
            (is (contains? (sec/mark-from-key-map main-key store)
                           [:datasets :commits main-root])))

          (testing "a Datahike branch initially shares that generation O(1)"
            (dv/branch! main :db :feature)
            (is (= main-key (stored-key-map main-db :feature)))
            (is (nil? (ss/list-dataset-branches store))))

          (let [feature (d/connect (assoc cfg :branch :feature))]
            (try
              (is (= 1 (row-count (d/db feature))))
              (d/transact feature [{:p/name "Bob"}])
              (let [feature-db (d/db feature)
                    feature-key (stored-key-map feature-db :feature)
                    feature-root (:dataset-commit-id feature-key)]
                (is (= 2 (row-count feature-db)))
                (is (= 1 (row-count (d/db main))))
                (is (not= main-root feature-root))
                (is (nil? (ss/list-dataset-branches store)))

                (testing "restore and GC follow only Datahike roots"
                  (let [reopened (d/connect (assoc cfg :branch :feature))]
                    (try
                      (is (= 2 (row-count (d/db reopened))))
                      (finally
                        (d/release reopened))))

                  (<?? S (d/gc-storage main (Date.) {:min-age-ms 0}))
                  (is (some? (ss/load-dataset-commit store main-root)))
                  (is (some? (ss/load-dataset-commit store feature-root)))

                  (dv/delete-branch! main :feature)
                  (<?? S (d/gc-storage main (Date.) {:min-age-ms 0}))
                  (is (some? (ss/load-dataset-commit store main-root)))
                  (is (nil? (ss/load-dataset-commit store feature-root)))))
              (finally
                (d/release feature)))))
        (finally
          (d/release main)
          (d/delete-database cfg))))))
