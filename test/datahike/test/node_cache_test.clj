(ns datahike.test.node-cache-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.connections :as connections]))

(def schema
  [{:db/ident :node-cache/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn- config
  ([store-id]
   (config store-id 64))
  ([store-id store-cache-size]
   {:store {:backend :memory :id store-id}
    :schema-flexibility :write
    :store-cache-size store-cache-size
    :initial-tx schema}))

(defn- names [conn]
  (set (d/q '[:find [?name ...]
              :where [_ :node-cache/name ?name]]
            @conn)))

(deftest cache-reservations-share-by-store-and-threshold
  (let [registry (atom {})
        store-id (random-uuid)]
    (binding [connections/*connections* registry]
      (let [[_ main-cache] (connections/acquire-node-cache!
                            [store-id :db] 64 #(atom :main-cache))
            [_ branch-cache] (connections/acquire-node-cache!
                              [store-id :feature] 64 #(atom :unused))
            [_ small-cache] (connections/acquire-node-cache!
                             [store-id :small] 8 #(atom :small-cache))]
        (is (identical? main-cache branch-cache)
            "branches of one store and threshold share the cache")
        (is (not (identical? main-cache small-cache))
            "different configured bounds retain independent LRUs")))))

(deftest rebound-connection-registries-isolate-node-caches
  (let [store-id (random-uuid)
        first-registry (atom {})
        second-registry (atom {})
        [_ first-cache] (binding [connections/*connections* first-registry]
                          (connections/acquire-node-cache!
                           [store-id :db] 64 #(atom :first-cache)))
        [_ second-cache] (binding [connections/*connections* second-registry]
                           (connections/acquire-node-cache!
                            [store-id :db] 64 #(atom :second-cache)))]
    (is (not (identical? first-cache second-cache)))
    (is (identical? first-cache
                    (get-in @first-registry [[store-id :db] :node-cache])))
    (is (identical? second-cache
                    (get-in @second-registry [[store-id :db] :node-cache])))))

(deftest non-connection-store-operations-do-not-reserve-a-cache
  (let [registry (atom {})
        cfg (config (random-uuid))]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (is (d/database-exists? cfg))
        (is (empty? @registry))
        (finally
          (d/delete-database cfg))))))

(deftest failed-connect-abandons-its-cache-reservation
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (is (thrown? Exception
                     (d/connect (assoc cfg :branch :missing))))
        (is (not (contains? @registry [store-id :missing])))
        (finally
          (d/delete-database cfg))))))

(deftest invalidation-rejects-an-in-flight-connection
  (let [registry (atom {})
        store-id (random-uuid)
        conn-id [store-id :db]
        conn (atom :connected)]
    (binding [connections/*connections* registry]
      (let [lease (connections/reserve-connection! conn-id)]
        (connections/invalidate-store-connections! store-id)
        (is (false? (connections/add-connection! conn-id lease conn)))
        (is (empty? @registry))))))

(deftest invalidation-releases-exactly-the-connections-it-removes
  (let [registry (atom {})
        store-id (random-uuid)
        conn-id [store-id :db]
        conn (atom :connected)]
    (binding [connections/*connections* registry]
      (let [lease (connections/reserve-connection! conn-id)]
        (is (connections/add-connection! conn-id lease conn))
        (connections/invalidate-store-connections! store-id)
        (is (= :released @conn))
        (is (empty? @registry))))))

(deftest shared-cache-preserves-branch-write-isolation-and-reconnects
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)
        opened (atom [])]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (let [main (d/connect cfg)
              _ (swap! opened conj main)
              _ (d/transact main [{:node-cache/name "base"}])
              _ (d/branch! main :db :feature)
              feature (d/connect (assoc cfg :branch :feature))
              _ (swap! opened conj feature)
              first-cache (get-in @registry [[store-id :db] :node-cache])]
          (is (identical? first-cache
                          (get-in @registry [[store-id :feature] :node-cache])))

          (d/transact main [{:node-cache/name "main-only"}])
          (d/transact feature [{:node-cache/name "feature-only"}])
          (is (= #{"base" "main-only"} (names main)))
          (is (= #{"base" "feature-only"} (names feature)))

          ;; Drop every connection so the next reads must rebuild their DB values
          ;; from durable branch heads. They receive a fresh shared cache and must
          ;; still observe the independent branch histories.
          (d/release feature)
          (d/release main)
          (is (empty? @registry))

          (let [main' (d/connect cfg)
                _ (swap! opened conj main')
                feature' (d/connect (assoc cfg :branch :feature))
                _ (swap! opened conj feature')
                second-cache (get-in @registry [[store-id :db] :node-cache])]
            (is (not (identical? first-cache second-cache)))
            (is (identical? second-cache
                            (get-in @registry [[store-id :feature] :node-cache])))
            (is (= #{"base" "main-only"} (names main')))
            (is (= #{"base" "feature-only"} (names feature')))))
        (finally
          (doseq [conn (reverse @opened)]
            (d/release conn true))
          (d/delete-database cfg))))))
