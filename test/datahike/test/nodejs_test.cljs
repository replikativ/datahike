(ns datahike.test.nodejs-test
  (:require [cljs.test :refer [deftest is async] :as t]
            [datahike.api :as d]
            [konserve.node-filestore] ;; Register :file backend for Node.js
            [cljs.core.async :refer [go <!] :include-macros true]
            [cljs.nodejs :as nodejs]))

(def fs (nodejs/require "fs"))
(def path (nodejs/require "path"))
(def os (nodejs/require "os"))

(defn tmp-dir []
  (let [dir (path.join (os.tmpdir) (str "datahike-node-test-" (rand-int 100000)))]
    dir))

(deftest roundtrip-test
  (let [dir (tmp-dir)
        store-id (random-uuid)
        cfg {:store {:backend :file :path dir :id store-id}
             :keep-history? true
             :schema-flexibility :write}]
    (async done
           (go
             (try
          ;; Create database
               (let [created (<! (d/create-database cfg))]
                 (is created "Database created"))

          ;; Connect
               (let [conn (d/connect cfg)]
                 (is conn "Connection established")

            ;; Add schema
                 (let [schema-tx [{:db/ident :name
                                   :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one}
                                  {:db/ident :age
                                   :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/one}]]
                   (let [report (<! (d/transact! conn schema-tx))]
                     (is (:db-after report) "Schema added")))

            ;; Transact data
                 (let [tx-report (<! (d/transact! conn [{:name "Alice" :age 30}
                                                        {:name "Bob" :age 25}]))]
                   (is (:db-after tx-report) "Data transacted")
                   (is (pos? (count (:tx-data tx-report))) "Datoms were added"))

            ;; Verify data via datoms API
                 (let [all-datoms (vec (d/datoms @conn :eavt))
                       name-datoms (filter #(= :name (:a %)) all-datoms)
                       age-datoms (filter #(= :age (:a %)) all-datoms)]
                   (is (= 2 (count name-datoms)) "Found 2 name datoms")
                   (is (= 2 (count age-datoms)) "Found 2 age datoms"))

            ;; Pull API
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (let [pulled (d/pull @conn [:name :age] e1)]
                       (is (:name pulled) "Pull retrieved name")
                       (is (:age pulled) "Pull retrieved age"))))

            ;; Query API
                 (let [q1 (d/q '[:find ?e :where [?e :name _]] @conn)]
                   (is (= 2 (count q1)) "Single pattern: found 2 entities")
                   (is (every? #(number? (first %)) q1) "Single pattern: entity IDs are numbers"))

                 (let [q2 (d/q '[:find ?v :where [_ :name ?v]] @conn)
                       names (set (map first q2))]
                   (is (= 2 (count q2)) "Value query: found 2 names")
                   (is (contains? names "Alice") "Value query: found Alice")
                   (is (contains? names "Bob") "Value query: found Bob"))

                 (let [q3 (d/q '[:find ?e ?name ?age
                                 :where
                                 [?e :name ?name]
                                 [?e :age ?age]] @conn)
                       results (into {} (map (fn [[e name age]] [name {:e e :age age}]) q3))]
                   (is (= 2 (count q3)) "Join query: found 2 entity/name/age tuples")
                   (is (number? (get-in results ["Alice" :e])) "Join query: Alice has valid entity ID")
                   (is (= 30 (get-in results ["Alice" :age])) "Join query: Alice is 30")
                   (is (number? (get-in results ["Bob" :e])) "Join query: Bob has valid entity ID")
                   (is (= 25 (get-in results ["Bob" :age])) "Join query: Bob is 25"))

            ;; Entity API
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (let [entity (d/entity @conn e1)]
                       (is (:name entity) "Entity has name")
                       (is (:age entity) "Entity has age"))))

            ;; Update for history
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (<! (d/transact! conn [[:db/add e1 :age 31]]))))

            ;; Test as-of DB
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)
                       before-update-tx (:max-tx @conn)]
                   (when e1
                     (<! (d/transact! conn [[:db/add e1 :age 99]])))
                   (let [current-age-after (when e1 (:age (d/entity @conn e1)))]
                     (is (= 99 current-age-after) "Current DB shows updated age"))
                   (let [as-of-db (d/as-of @conn before-update-tx)
                         as-of-age (when e1 (:age (d/entity as-of-db e1)))]
                     (is (= 31 as-of-age) "as-of DB shows old age value")))

            ;; History DB
                 (let [hist-db (d/history @conn)
                       hist-datoms (vec (filter #(= :age (:a %)) (d/datoms hist-db :eavt)))]
                   (is (>= (count hist-datoms) 4) "History contains multiple age values"))

                 (d/release conn))

          ;; Reconnect to verify persistence
               (let [conn2 (d/connect cfg)]
                 (is conn2 "Reconnected successfully")

                 (let [all-datoms (vec (d/datoms @conn2 :eavt))
                       name-datoms (filter #(= :name (:a %)) all-datoms)]
                   (is (= 2 (count name-datoms)) "Data persisted after reconnect"))

            ;; Test different index access
                 (let [aevt-datoms (take 5 (d/datoms @conn2 :aevt))
                       avet-datoms (take 5 (d/datoms @conn2 :avet))]
                   (is (seq aevt-datoms) "Got datoms from AEVT index")
                   (is (seq avet-datoms) "Got datoms from AVET index"))

                 (d/release conn2))

          ;; Delete database
               (let [deleted (<! (d/delete-database cfg))]
                 (is (nil? deleted) "Database deleted"))

               (is (not (fs.existsSync dir)) "Directory removed")

               (catch js/Error e
                 (is false (str "Error: " (.-message e))))
               (finally
                 (done)
            ;; Force immediate exit on next tick
            ;; Background operations in konserve/core.async keep event loop alive
                 (js/process.nextTick
                  (fn []
                    (.exit js/process 0)))))))))

(defn -main []
  (t/run-tests 'datahike.test.nodejs-test))
