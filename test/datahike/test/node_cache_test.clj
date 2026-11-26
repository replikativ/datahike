(ns datahike.test.node-cache-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.connections :as connections]
            [datahike.connector]
            [datahike.writer]))

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
        (is (nil? (connections/add-connection! conn-id lease conn)))
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

(deftest concurrent-connects-count-each-lease
  (dotimes [_ 5]
    (let [registry (atom {})
          store-id (random-uuid)
          cfg {:store {:backend :memory :id store-id} :schema-flexibility :read}
          conn-id [store-id :db]
          entry #(get @registry conn-id)]
      (binding [connections/*connections* registry]
        (d/create-database cfg)
        (try
          (let [go (java.util.concurrent.CountDownLatch. 1)
                fs (mapv (fn [_] (future (.await go) (d/connect cfg))) (range 4))
                _ (.countDown go)
                conns (mapv deref fs)]
            (is (every? #(identical? (first conns) %) conns) "one connection for all callers")
            (is (= 4 (:count (entry))))
            (doseq [c (butlast conns)]
              (d/release c)
              (is (some? (:conn (entry))) "the id stays until the last release"))
            (d/release (last conns))
            (is (nil? (entry))))
          (finally
            (d/delete-database cfg)))))))

(deftest a-second-publish-counts-a-lease-on-the-first-connection
  (let [registry (atom {})
        conn-id [(random-uuid) :db]
        first-conn (atom :first)
        second-conn (atom :second)]
    (binding [connections/*connections* registry]
      (let [lease-a (connections/reserve-connection! conn-id)
            lease-b (connections/reserve-connection! conn-id)]
        (is (identical? first-conn (connections/add-connection! conn-id lease-a first-conn)))
        (is (identical? first-conn (connections/add-connection! conn-id lease-b second-conn))
            "the second publish is handed the registered connection")
        (is (= 2 (get-in @registry [conn-id :count])))
        (is (empty? (get-in @registry [conn-id :leases])))
        (is (nil? (connections/add-connection! conn-id lease-b second-conn))
            "a lease is spent once")
        (is (= 2 (get-in @registry [conn-id :count])))))))

(deftest a-publish-that-cannot-reuse-replaces-the-registered-connection
  (let [registry (atom {})
        conn-id [(random-uuid) :db]
        first-conn (atom :first)
        second-conn (atom :second)]
    (binding [connections/*connections* registry]
      (let [lease-a (connections/reserve-connection! conn-id)
            lease-b (connections/reserve-connection! conn-id)]
        (connections/add-connection! conn-id lease-a first-conn)
        (is (identical? second-conn
                        (connections/add-connection! conn-id lease-b second-conn
                                                     (constantly false))))
        (is (= 1 (get-in @registry [conn-id :count])))))))

(defn- connect-missing-the-registry
  "A connect that does not see the registered connection at its lookup, as one
   does that started before the other published. It builds its own connection
   and meets the registered one only at publication."
  [cfg]
  (with-redefs [connections/get-connection (constantly nil)]
    (d/connect cfg)))

(deftest a-connect-that-loses-the-race-gets-the-registered-connection
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)
        conn-id [store-id :db]
        built (atom [])
        writers (atom [])
        create-writer datahike.writer/create-writer]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (let [winner (d/connect cfg)
              loser (with-redefs [datahike.writer/create-writer
                                  (fn [writer-config conn]
                                    (swap! built conj conn)
                                    (let [writer (create-writer writer-config conn)]
                                      (swap! writers conj writer)
                                      writer))]
                      (connect-missing-the-registry cfg))]
          (is (identical? winner loser))
          (is (= 2 (get-in @registry [conn-id :count])))
          (is (= 1 (count @built)) "the losing connect did build a connection")
          (is (= :released @(:wrapped-atom (first @built)))
              "and discarded it")
          (is (= ::closed
                 (async/alt!! (:commit-queue (first @writers)) ([v] (if (nil? v) ::closed v))
                              (async/timeout 5000) ::still-waiting))
              "its writer loop ends")
          (d/transact loser [{:node-cache/name "after the race"}])
          (is (= #{"after the race"} (names winner)))
          (d/release loser)
          (is (some? (get-in @registry [conn-id :conn])) "the id stays until the last release")
          (d/release winner)
          (is (nil? (get @registry conn-id))))
        (finally
          (d/delete-database cfg))))))

(deftest a-refused-reuse-gives-its-lease-back
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)
        ;; Passes the stored-config check, differs from the live connection.
        other (assoc cfg :allow-unsafe-config true)
        conn-id [store-id :db]]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (let [conn (d/connect cfg)]
          (is (thrown-with-msg? Exception #"does not match existing connections"
                                (d/connect other))
              "found in the registry")
          (is (= 1 (get-in @registry [conn-id :count])))
          (is (thrown-with-msg? Exception #"does not match existing connections"
                                (connect-missing-the-registry other))
              "met at publication")
          (is (= 1 (get-in @registry [conn-id :count])))
          (is (identical? conn (get-in @registry [conn-id :conn])))
          (d/release conn)
          (is (nil? (get @registry conn-id))))
        (finally
          (d/delete-database cfg))))))

(deftest a-strict-release-debits-only-the-connection-it-names
  ;; The rollback of a refused reuse gives back a lease it took a moment ago. If
  ;; the registry has replaced that connection since, it must not hand back the
  ;; successor's.
  (let [registry (atom {})
        conn-id [(random-uuid) :db]
        replaced (atom :replaced)
        successor (atom :successor)]
    (binding [connections/*connections* registry]
      (let [lease-a (connections/reserve-connection! conn-id)
            lease-b (connections/reserve-connection! conn-id)]
        (connections/add-connection! conn-id lease-a replaced)
        (connections/add-connection! conn-id lease-b successor (constantly false))
        (is (= :absent (connections/release-lease! conn-id replaced false true)))
        (is (= 1 (get-in @registry [conn-id :count])) "the successor keeps its lease")
        (is (= :successor @successor))
        (is (= :removed (connections/release-lease! conn-id successor false true)))
        (is (= :released @successor))
        (is (empty? @registry))
        (is (= :absent (connections/release-lease! conn-id successor false false))
            "nothing left to debit")))))

(deftest a-release-of-a-released-connection-debits-nothing
  ;; A release that read its connection before another caller released it, and
  ;; reaches the registry after a new connection was published under the id.
  (let [registry (atom {})
        conn-id [(random-uuid) :db]
        stale (atom :released)
        fresh (atom :fresh)]
    (binding [connections/*connections* registry]
      (let [lease (connections/reserve-connection! conn-id)]
        (connections/add-connection! conn-id lease fresh)
        (is (= :absent (connections/release-lease! conn-id stale false false)))
        (is (= :absent (connections/release-lease! conn-id stale true false)))
        (is (= 1 (get-in @registry [conn-id :count])))
        (is (= :fresh @fresh))))))

(deftest the-last-release-ends-a-reservation-in-flight
  ;; The connect in flight built its database from the head it read when it
  ;; began. The released connection may have committed since, so that snapshot
  ;; must not become the next connection.
  (let [registry (atom {})
        conn-id [(random-uuid) :db]
        first-conn (atom :first)
        second-conn (atom :second)]
    (binding [connections/*connections* registry]
      (let [lease-a (connections/reserve-connection! conn-id)
            lease-b (connections/reserve-connection! conn-id)]
        (connections/add-connection! conn-id lease-a first-conn)
        (is (= :removed (connections/release-lease! conn-id first-conn false false)))
        (is (= :released @first-conn))
        (is (empty? @registry))
        (is (nil? (connections/add-connection! conn-id lease-b second-conn)))
        (is (empty? @registry))))))

(deftest a-local-connect-does-not-take-a-replica-in-its-place
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)
        conn-id [store-id :db]
        ;; What a Kabel replica looks like to the registry: a connection that
        ;; carries its own retirement hook.
        replica (atom :replica :meta {:datahike/retire! (fn [])})]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (let [lease (connections/reserve-connection! conn-id)
              _ (connections/add-connection! conn-id lease replica)
              conn (connect-missing-the-registry cfg)]
          (is (not (identical? replica conn)))
          (is (identical? conn (get-in @registry [conn-id :conn])))
          (is (= 1 (get-in @registry [conn-id :count])))
          (d/release conn)
          (is (nil? (get @registry conn-id))))
        (finally
          (d/delete-database cfg))))))

(deftest a-writer-whose-connection-is-released-before-it-starts-ends
  (let [registry (atom {})
        store-id (random-uuid)
        cfg (config store-id)]
    (binding [connections/*connections* registry]
      (d/create-database cfg)
      (try
        (let [conn (d/connect cfg)
              ;; A connection that never shows its writer, as one discarded
              ;; before the writer loop first looked.
              unpublished (datahike.connector/->Connection
                           (atom (dissoc @(:wrapped-atom conn) :writer)))
              writer (datahike.writer/create-writer {:backend :self} unpublished)]
          (reset! unpublished :released)
          (datahike.writer/shutdown writer)
          ;; The transaction loop closes the commit queue as it ends, so a
          ;; take that completes says the loop got out of its startup wait.
          (is (= ::closed
                 (async/alt!! (:commit-queue writer) ([v] (if (nil? v) ::closed v))
                              (async/timeout 5000) ::still-waiting)))
          (d/release conn))
        (finally
          (d/delete-database cfg))))))

(deftest only-interchangeable-connections-are-shared
  (let [interchangeable? #'datahike.connector/interchangeable?
        existing (fn [writer-config]
                   (datahike.connector/->Connection (atom {:config {:writer writer-config}})))
        remote (fn [token] {:backend :datahike-server :url "http://localhost:4444" :token token})]
    (is (interchangeable? (existing nil)))
    (is (interchangeable? (existing (remote "a")))
        "the writer config is not compared, as for a connect that finds a registered connection")
    (is (not (interchangeable? (atom :replica :meta {:datahike/retire! (fn [])})))
        "a Kabel replica is not shared")))
