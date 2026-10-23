(ns datahike.test.tiered-populate-test
  "`:populate-on-connect? false`: a tiered store that skips the connect-time
   copy and fills its frontend by read-through.

   By default `ready-store :tiered` copies every backend key into the frontend
   on connect. On the JVM every konserve backend can be read synchronously, and
   under `:read-policy :frontend-first` a frontend miss reads through to the
   backend — so the copy is optional there. (It does not bound the frontend:
   what is read stays cached.)"
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.store :as ds]
            [datahike.migrate.fs :as fs]
            [konserve.core :as k]
            [konserve.store :as ks]))

(defn- cfg [populate?]
  (let [id (java.util.UUID/randomUUID)]
    {:store (cond-> {:backend :tiered
                     :id id
                     :frontend-config {:backend :memory :id id}
                     :backend-config {:backend :file
                                      :path (fs/temp-store-path! "dh-tiered-populate-")
                                      :id id}}
              (not populate?) (assoc :populate-on-connect? false))
     :keep-history? true :schema-flexibility :write
     :index :datahike.index/persistent-set}))

(defn- key-count [store] (count (k/keys store {:sync? true})))

(defn- filled! [c]
  (d/create-database c)
  (let [conn (d/connect c)]
    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (doseq [k (range 20)]
      (d/transact conn (vec (for [i (range 50)] {:name (str "n" k "-" i)}))))
    (d/release conn)))

(deftest a-frontend-can-be-a-cache
  (let [c (cfg false)]
    (filled! c)
    ;; a cold frontend, as after a restart or an eviction
    (ks/delete-store (get-in c [:store :frontend-config]))
    (let [conn (d/connect c)
          {:keys [frontend-store backend-store]} (:store @conn)
          after-connect (key-count frontend-store)
          backend (key-count backend-store)]
      (is (< after-connect backend) "connect no longer copies the whole backend")
      (is (= 1000 (count (d/q '[:find ?n :where [_ :name ?n]] @conn)))
          "and every query still answers, reading through the frontend")
      (is (< after-connect (key-count frontend-store)) "what was read is now cached")
      (d/transact conn [{:name "after"}])
      (is (= 1001 (count (d/q '[:find ?n :where [_ :name ?n]] @conn))))
      (d/release conn))
    (d/delete-database c)))

(deftest the-default-still-mirrors
  (let [c (cfg true)]
    (filled! c)
    (ks/delete-store (get-in c [:store :frontend-config]))
    (let [conn (d/connect c)
          {:keys [frontend-store backend-store]} (:store @conn)]
      (is (= (key-count backend-store) (key-count frontend-store)))
      (d/release conn))
    (d/delete-database c)))

(deftest frontend-only-reads-refuse-the-opt-out
  (let [c (assoc-in (cfg false) [:store :read-policy] :frontend-only)]
    (is (= :store/tiered-populate-required
           (try (d/create-database c) (d/connect c) nil
                (catch Exception e (or (:type (ex-data e))
                                       (:type (ex-data (ex-cause e))))))))
    (try (d/delete-database c) (catch Exception _ nil))))

(deftest the-option-must-be-a-boolean
  (is (true? (ds/tiered-populate-on-connect? {})))
  (is (false? (ds/tiered-populate-on-connect? {:populate-on-connect? false})))
  (is (= :store/invalid-populate-on-connect
         (try (ds/tiered-populate-on-connect? {:populate-on-connect? "no"}) nil
              (catch Exception e (:type (ex-data e))))))
  (let [c (assoc-in (cfg false) [:store :populate-on-connect?] "no")]
    (is (= :store/invalid-populate-on-connect
           (try (d/create-database c) (d/connect c) nil
                (catch Exception e (or (:type (ex-data e))
                                       (:type (ex-data (ex-cause e))))))))
    (try (d/delete-database c) (catch Exception _ nil))))
