(ns datahike.test.http.permissions-test
  "The eacl-backed permissions of the server: who may read, write, delete and
   grant, through the API and the remote writer, and across a restart."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.connections :as connections]
   [datahike.http.client :as client]
   [datahike.http.permissions :as permissions]
   [datahike.http.routes :as routes]
   [datahike.http.server :as server]
   [datahike.migrate.fs :as fs]
   [ring.adapter.jetty :refer [run-jetty]]))

(def ^:private root-token "root-token")

(defn- validator
  "alice and bob, by token — what a JWT validator would return."
  [request]
  (case (some->> (get-in request [:headers "authorization"]) (re-find #"token (.+)") second)
    "alice-token" {:sub "alice"}
    "bob-token"   {:sub "bob"}
    nil))

(defn- forbidden? [f]
  (try (f) false
       (catch Exception e
         (loop [e e]
           (cond (= :datahike.http/forbidden (:type (ex-data e))) true
                 (.getCause e) (recur (.getCause e))
                 :else false)))))

(deftest permissions-in-the-system-db
  (let [port     23210
        url      (str "http://localhost:" port)
        config   {:token root-token :validator validator
                  :system-db {:store {:backend :file :path (str (fs/temp-dir! "dh-system-") "/system")}}}
        start!   (fn []
                   (let [conns (atom {}) app (server/app config conns)]
                     {:server (run-jetty app {:port port :join? false}) :app app :conns conns}))
        stop!    (fn [{:keys [server app conns]}]
                   (.stop server)
                   (routes/release-all! conns)
                   (permissions/close! (::server/config (meta app))))
        peer     (fn [t] {:backend :datahike-server :url url :token t})
        store-id (random-uuid)
        cfg      {:store {:backend :file :path (str (fs/temp-dir! "dh-perm-") "/store") :id store-id}
                  :schema-flexibility :read}
        db-obj   {:type :database :id (str store-id)}
        grant!   (fn [t updates] (client/request-cbor :post "permissions/relationships!" (peer t) updates))
        list!    (fn [t] (client/request-cbor :get "databases" (peer t) (random-uuid)))
        s        (atom (start!))]
    (try
      (testing "root creates; nobody else may touch the database"
        (client/create-database (assoc cfg :remote-peer (peer root-token)))
        (is (= [{:store-id (str store-id) :state :active}]
               (mapv #(select-keys % [:store-id :state]) (list! root-token))))
        (is (empty? (list! "alice-token"))
            "the catalog does not reveal databases the caller cannot read")
        (is (forbidden? #(client/connect (assoc cfg :remote-peer (peer "alice-token"))))))

      (testing "root grants alice writer and bob reader in one batch; alice may read and write, not delete or grant"
        (is (= {:written 2}
               (grant! root-token [{:operation :touch
                                    :relationship {:subject {:type :user :id "alice"} :relation :writer :resource db-obj}}
                                   {:operation :touch
                                    :relationship {:subject {:type :user :id "bob"} :relation :reader :resource db-obj}}])))
        (is (= #{(str store-id)} (set (map :store-id (list! "alice-token")))))
        (is (= #{(str store-id)} (set (map :store-id (list! "bob-token")))))
        (let [alice (client/connect (assoc cfg :remote-peer (peer "alice-token")))]
          (client/transact alice [{:name "Ada"}])
          (is (= #{["Ada"]} (client/q '[:find ?n :where [?e :name ?n]] @alice)))
          (is (forbidden? #(client/delete-database (assoc cfg :remote-peer (peer "alice-token")))))
          (is (forbidden? #(grant! "alice-token" [{:operation :touch
                                                   :relationship {:subject {:type :user :id "bob"} :relation :reader :resource db-obj}}])))
          (is (true? (:allowed (client/request-cbor :post "permissions/check" (peer "alice-token")
                                                    {:permission :transact :resource db-obj}))))
          (is (false? (:allowed (client/request-cbor :post "permissions/check" (peer "alice-token")
                                                     {:permission :delete :resource db-obj}))))
          (is (forbidden? #(client/request-cbor :post "permissions/check" (peer "alice-token")
                                                {:subject {:type :user :id "bob"} :permission :read :resource db-obj}))
              "asking about someone else needs grant")
          (client/release alice)))

      (testing "bob reads and nothing else"
        (let [bob (client/connect (assoc cfg :remote-peer (peer "bob-token")))]
          (is (= #{["Ada"]} (client/q '[:find ?n :where [?e :name ?n]] @bob)))
          (is (forbidden? #(client/transact bob [{:name "Bob"}])))
          (client/release bob)))

      (testing "the remote writer is authorized the same way"
        ;; Two processes, so two registries: in one, bob's connect would hand
        ;; him alice's cached connection, writer token included.
        (let [alice (d/connect (assoc cfg :writer {:backend :datahike-server :url url :token "alice-token"}))
              bob   (binding [connections/*connections* (atom {})]
                      (d/connect (assoc cfg :writer {:backend :datahike-server :url url :token "bob-token"})))]
          (is (some? (:db-after (d/transact alice [{:name "Grace"}]))))
          (is (forbidden? #(d/transact bob [{:name "Bob"}])))
          (is (= #{["Ada"] ["Grace"]} (d/q '[:find ?n :where [?e :name ?n]] @alice)))
          (d/release alice)
          (d/release bob)))

      (testing "relationships survive a restart, and root stays admin"
        (stop! @s)
        (reset! s (start!))
        (is (= #{{:subject {:type :user :id "alice"} :relation :writer :resource db-obj}
                 {:subject {:type :user :id "bob"} :relation :reader :resource db-obj}}
               (set (client/request-cbor :post "permissions/relationships" (peer root-token) {:resource db-obj}))))
        (let [alice (client/connect (assoc cfg :remote-peer (peer "alice-token")))]
          (is (= #{["Ada"] ["Grace"]} (client/q '[:find ?n :where [?e :name ?n]] @alice)))
          (client/release alice))
        (is (= {:written 1}
               (grant! root-token [{:operation :delete
                                    :relationship {:subject {:type :user :id "alice"} :relation :writer :resource db-obj}}])))
        (is (forbidden? #(client/connect (assoc cfg :remote-peer (peer "alice-token"))))
            "a revoked grant is gone at once")
        (is (try (grant! root-token [{:operation :touch
                                      :relationship {:subject {:type :user :id "alice"} :relation :reader :resource db-obj}}
                                     {:operation :touch
                                      :relationship {:subject {:type :user :id "alice"} :relation :nonsense :resource db-obj}}])
                 false
                 (catch Exception _ true))
            "a batch with a bad relationship is refused whole")
        (is (forbidden? #(client/connect (assoc cfg :remote-peer (peer "alice-token"))))
            "…and its good half did not land")
        (client/delete-database (assoc cfg :remote-peer (peer root-token)))
        (is (empty? (list! root-token))
            "soft-deleted catalog entries are absent from the public list"))
      (finally
        (stop! @s)))))

(deftest memory-system-dbs-do-not-collide
  (let [a (permissions/configure {:token "a" :system-db {:store {:backend :memory}}})
        b (permissions/configure {:token "b" :system-db {:store {:backend :memory}}})]
    (try
      (is (not (identical? (::permissions/conn a) (::permissions/conn b))))
      (is (true? ((:authorize a) {:op :create :principal {:sub "root"} :db nil})))
      (is (true? ((:authorize b) {:op :create :principal {:sub "root"} :db nil})))
      (finally (permissions/close! a) (permissions/close! b)))))
