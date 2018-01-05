(ns datahike.test.store
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.db :as db]
   [datahike.query-v3 :as q]
   [datahike.test.core :as tdc]
   [hitchhiker.konserve :as kons]
   [hitchhiker.tree.core :as hc :refer [<??]]
   [clojure.core.async :as async]
   [konserve.filestore :refer [new-fs-store]]))


(def db (d/db-with
         (d/empty-db {:name {:db/index true}})
         [{ :db/id 1, :name  "Ivan", :age   15 }
          { :db/id 2, :name  "Petr", :age   37 }
          { :db/id 3, :name  "Ivan", :age   37 }
          { :db/id 4, :age 15 }]))

(def store (kons/add-hitchhiker-tree-handlers
            (async/<!! (new-fs-store "/tmp/datahike-play"))))


(def backend (kons/->KonserveBackend store))

(defn store-db [db backend]
  (let [{:keys [eavt-durable aevt-durable avet-durable]} db]
    {:eavt-key (kons/get-root-key (:tree (<?? (hc/flush-tree eavt-durable backend))))
     :aevt-key (kons/get-root-key (:tree (<?? (hc/flush-tree aevt-durable backend))))
     :avet-key (kons/get-root-key (:tree (<?? (hc/flush-tree avet-durable backend))))}))

(defn load-db [stored-db]
  (let [{:keys [eavt-key aevt-key avet-key]} stored-db
        empty (d/empty-db)
        eavt-durable (<?? (kons/create-tree-from-root-key store eavt-key))]
    (assoc empty
           :max-eid (datahike.db/init-max-eid (:eavt empty) eavt-durable)
           :eavt-durable eavt-durable
           :aevt-durable (<?? (kons/create-tree-from-root-key store aevt-key))
           :avet-durable (<?? (kons/create-tree-from-root-key store avet-key)))))


(deftest testing-stored-db
  (testing "Testing one round-trip to disk")
  (let [stored-db (store-db db backend)
        loaded-db (load-db stored-db)]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] loaded-db)

           #{[3] [2] [1]}))
    (let [updated (d/db-with loaded-db
                             [{:db/id -1 :name "Hiker" :age 9999}])]
      (is (= (d/q '[:find ?e
                    :where [?e :name "Hiker"]] updated)

             #{[5]})))))


(comment
  (def new-db
    (time
     (d/db-with
      db
      (for [i (shuffle (range 5 10000))]
        {:db/id i :name (str "Bot" i) :age i}))))

  ;; TODO why is logarithmic query scaling so bad?
  ;; why is shuffeling impacting 2x query time?
  ;; seem to hit different sort algos depending on br size
  ;; how do aevt and avet interact with this query?
  ;; both do not seem to depend on position in index -> no range scan
  (time
   (doseq [i (range 100)]
     (d/q '[:find ?e
            :where [?e :name "Bot42"]] new-db)))

  (time (d/q '[:find ?e
               :where [?e :name "Ivan"]] db))

  )


