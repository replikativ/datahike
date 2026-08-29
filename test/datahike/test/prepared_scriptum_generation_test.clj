(ns datahike.test.prepared-scriptum-generation-test
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.scriptum]
   [datahike.versioning :as dv]
   [konserve.core :as k]
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
          [:secondary-index-keys :idx/body]))

(defn- body-value [db eid]
  (sec/-sec-value (get-in db [:secondary-indices :idx/body]) :doc/body eid))

(defn- all-present? [store keys]
  (every? #(k/exists? store % {:sync? true}) keys))

(defn- delete-tree! [path]
  (doseq [f (reverse (file-seq (io/file path)))]
    (io/delete-file f true)))

(deftest datahike-roots-scriptum-snapshots-through-cold-reopen-and-gc
  (let [store-id (random-uuid)
        cache-path (str "/tmp/datahike-scriptum-cache-" store-id)
        cfg {:store {:backend :file
                     :id store-id
                     :path (str "/tmp/datahike-prepared-scriptum-" store-id)}
             :writer {:backend :self :writer-ownership :exclusive}
             :keep-history? false
             :schema-flexibility :write}]
    (d/create-database cfg)
    (try
      (let [{:keys [eid main-marks feature-marks]}
            (let [main (d/connect cfg)]
              (try
                (d/transact main [{:db/ident :doc/body
                                   :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one
                                   :db.secondary/only true}])
                (d/transact main [{:db/ident :idx/body
                                   :db.secondary/type :scriptum
                                   :db.secondary/attrs [:doc/body]
                                   :db.secondary/config {:path cache-path}}])
                (is (= :ready (await-ready main :idx/body)))
                (let [eid (get-in (d/transact main [{:db/id -1
                                                     :doc/body "main revision"}])
                                  [:tempids -1])
                      main-key (stored-key-map (d/db main) :db)
                      main-marks
                      (sec/mark-from-key-map main-key (:store (d/db main)))]
                  (is (= :scriptum (:type main-key)))
                  (is (uuid? (:snapshot-address main-key)))
                  (is (all-present? (:store (d/db main)) main-marks))
                  (dv/branch! main :db :feature)
                  (let [feature (d/connect (assoc cfg :branch :feature))]
                    (try
                      (d/transact feature
                                  [[:db/add eid :doc/body "feature revision"]])
                      (let [feature-key (stored-key-map (d/db feature) :feature)
                            feature-marks
                            (sec/mark-from-key-map
                             feature-key (:store (d/db feature)))]
                        (is (not= main-key feature-key))
                        (is (all-present? (:store (d/db feature)) feature-marks))
                        {:eid eid
                         :main-marks main-marks
                         :feature-marks feature-marks})
                      (finally
                        (d/release feature)))))
                (finally
                  (d/release main))))]

        ;; No reader or local Lucene file survives this point. A successful
        ;; reopen must reconstruct its snapshot from the marked Konserve blobs.
        (delete-tree! cache-path)
        (testing "cold readers restore the exact branch generation"
          (let [cold-main (d/connect cfg)
                cold-feature (d/connect (assoc cfg :branch :feature))]
            (try
              (is (= "main revision" (body-value (d/db cold-main) eid)))
              (is (= "feature revision" (body-value (d/db cold-feature) eid)))
              (<?? S (d/gc-storage cold-main (Date.) {:min-age-ms 0}))
              (is (all-present? (:store (d/db cold-main)) main-marks))
              (is (all-present? (:store (d/db cold-main)) feature-marks))
              (finally
                (d/release cold-feature)
                (d/release cold-main)))))

        (testing "deleting the branch makes only its generation collectible"
          (let [main (d/connect cfg)]
            (try
              (dv/delete-branch! main :feature)
              (<?? S (d/gc-storage main (Date.) {:min-age-ms 0}))
              (let [store (:store (d/db main))
                    feature-only (set/difference feature-marks main-marks)]
                (is (seq feature-only))
                (is (all-present? store main-marks))
                (is (every? #(not (k/exists? store % {:sync? true}))
                            feature-only)))
              (finally
                (d/release main)))))

        (delete-tree! cache-path)
        (testing "the surviving generation remains cold-reopenable after sweep"
          (let [cold-main (d/connect cfg)]
            (try
              (is (= "main revision" (body-value (d/db cold-main) eid)))
              (finally
                (d/release cold-main))))))
      (finally
        (d/delete-database cfg)
        (delete-tree! cache-path)))))
