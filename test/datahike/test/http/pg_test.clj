(ns datahike.test.http.pg-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.connections :as connections]
   [datahike.http.pg :as listener]
   [datahike.http.system :as system]
   [datahike.pg.server :as pg])
  (:import
   [datahike.pg PgWireServer]
   [java.sql DriverManager SQLException]
   [javax.net.ssl SSLContext]))

(defn- memory-config [name]
  {:name name
   :store {:backend :memory :id (random-uuid)}})

(deftest listener-shares-catalog-connections-and-lifecycle
  (let [system-config (system/configure
                       {:system-db {:store {:backend :memory}}
                        :pg-listener {:host "127.0.0.1" :port 0}})
        registry      (atom {})
        stopped       (atom 0)
        first-config  (memory-config "first")
        second-config (memory-config "second")
        conflict-config (memory-config "first")]
    (binding [connections/*connections* registry]
      (try
        (d/create-database first-config)
        ((get system-config system/register-key) first-config {:sub "root"})
        (with-redefs [pg/start-server
                      (fn [initial _options]
                        {:registry-atom (atom initial)})
                      pg/stop-server (fn [_] (swap! stopped inc))]
          (let [runtime (listener/start! system-config registry)
                pg-registry (:registry-atom (:server runtime))]
            (is (= #{"first"} (set (keys @pg-registry))))
            (is (= 1 (count (filter (comp :conn val) @registry))))

            (testing "duplicate PostgreSQL names are rejected before storage changes"
              (is (thrown-with-msg?
                   clojure.lang.ExceptionInfo
                   #"identifies more than one catalog store"
                   ((get system-config system/prepare-register-key)
                    conflict-config
                    {:sub "root"})))
              (is (false? (d/database-exists? conflict-config)))
              (is (= #{"first"} (set (keys @pg-registry)))))

            (testing "HTTP-created databases become available to new pg sessions"
              (d/create-database second-config)
              ((get system-config system/register-key) second-config {:sub "root"})
              (is (= #{"first" "second"} (set (keys @pg-registry)))))

            (testing "delete preparation releases pg ownership before physical deletion"
              ((get system-config system/prepare-delete-key) second-config {:sub "root"})
              (is (= #{"first"} (set (keys @pg-registry))))
              (d/delete-database second-config)
              ((get system-config system/delete-key) second-config {:sub "root"})
              (is (= #{"first"} (set (keys @pg-registry)))))

            (testing "a failed physical delete restores listener availability"
              ((get system-config system/prepare-delete-key) first-config {:sub "root"})
              (is (empty? @pg-registry))
              ((get system-config system/cancel-delete-key) first-config {:sub "root"})
              (is (= #{"first"} (set (keys @pg-registry)))))

            (listener/stop! runtime)
            (is (= 1 @stopped))
            (is (empty? (filter (comp :conn val) @registry)))))
        (finally
          (when (d/database-exists? first-config)
            (d/delete-database first-config))
          (when (d/database-exists? second-config)
            (d/delete-database second-config))
          (system/close! system-config))))))

(deftest listener-requires-resolved-deployment-secrets
  (let [system-config (system/configure
                       {:system-db {:store {:backend :memory}}})
        fake-config   {:name "accounts"
                       :store {:backend :jdbc
                               :id (random-uuid)
                               :password "catalog-secret"}}
        registry      (atom {})]
    (try
      ((get system-config system/register-key) fake-config {:sub "root"})
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"deployment-side secret overrides"
           (listener/start! (assoc system-config :pg-listener {:port 0}) registry)))
      (let [connected (atom nil)
            released  (atom 0)]
        (with-redefs [d/connect (fn [config]
                                  (reset! connected config)
                                  ::connection)
                      d/release (fn [_] (swap! released inc))
                      pg/start-server (fn [initial _]
                                        {:registry-atom (atom initial)})
                      pg/stop-server (constantly nil)]
          (let [runtime
                (listener/start!
                 (assoc system-config
                        :pg-listener
                        {:port 0
                         :database-overrides
                         {"accounts" {:store {:password "deployment-secret"}}}})
                 registry)]
            (is (= "deployment-secret" (get-in @connected [:store :password])))
            (listener/stop! runtime)
            (is (= 1 @released)))))
      (finally
        (system/close! system-config)))))

(deftest failed-dynamic-listener-add-does-not-rewrite-a-successful-create
  (let [system-config (system/configure
                       {:system-db {:store {:backend :memory}}
                        :pg-listener {:host "127.0.0.1" :port 0}})
        database     (memory-config "temporarily-unavailable")
        registry     (atom {})]
    (try
      (with-redefs [pg/start-server (fn [initial _]
                                      {:registry-atom (atom initial)})
                    pg/stop-server (constantly nil)]
        (let [runtime (listener/start! system-config registry)]
          (try
            (with-redefs [d/connect (fn [_]
                                      (throw (ex-info "backend unavailable" {})))]
              (is (= database
                     ((get system-config system/register-key)
                      database
                      {:sub "root"})))
              (is (= :active
                     (:state (first (system/entries
                                     (get system-config system/conn-key)))))))
            (finally
              (listener/stop! runtime)))))
      (finally
        (system/close! system-config)))))

(deftest listener-refuses-an-independent-database-registry
  (is (nil? (listener/start! {:pg-listener {:enabled? false}}
                             (atom {}))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"requires :system-db"
       (listener/start! {:pg-listener {:port 0}} (atom {}))))
  (let [config (system/configure
                {:system-db {:store {:backend :memory}}
                 :pg-listener {:database-template {:store {:backend :memory}}}})]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"shared system catalog"
           (listener/start! config (atom {}))))
      (finally
        (system/close! config)))))

(deftest listener-delegates-wire-security-to-pg-datahike
  (testing "an unsafe public listener is rejected by pg-datahike"
    (let [config (system/configure
                  {:system-db {:store {:backend :memory}}
                   :pg-listener {:host "0.0.0.0" :port 0}})]
      (try
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"require password authentication and TLS"
             (listener/start! config (atom {}))))
        (finally
          (system/close! config)))))
  (testing "authentication and TLS options reach pg-datahike unchanged"
    (let [ssl-context (SSLContext/getDefault)
          options-seen (atom nil)
          config (system/configure
                  {:system-db {:store {:backend :memory}}
                   :pg-listener {:host "0.0.0.0"
                                 :port 0
                                 :users {"app" "secret"}
                                 :ssl-context ssl-context}})]
      (try
        (with-redefs [pg/start-server (fn [_ options]
                                        (reset! options-seen options)
                                        ::server)
                      pg/stop-server (constantly nil)]
          (listener/stop! (listener/start! config (atom {}))))
        (is (= {:host "0.0.0.0"
                :port 0
                :users {"app" "secret"}
                :ssl-context ssl-context}
               @options-seen))
        (finally
          (system/close! config))))))

(deftest catalog-name-routes-a-real-postgresql-session
  (let [database      (memory-config "analytics")
        registry      (atom {})
        system-config (system/configure
                       {:system-db {:store {:backend :memory}}
                        :pg-listener {:host "127.0.0.1"
                                      :port 0
                                      :users {"datahike" "secret"}}})]
    (binding [connections/*connections* registry]
      (try
        (d/create-database database)
        ((get system-config system/register-key) database {:sub "root"})
        (let [runtime (listener/start! system-config registry)
              wire    ^PgWireServer (:server (:server runtime))
              url     (str "jdbc:postgresql://127.0.0.1:" (.getPort wire)
                           "/analytics?sslmode=disable&user=datahike&password=secret")]
          (try
            (with-open [sql-conn (DriverManager/getConnection url)
                        statement (.createStatement sql-conn)
                        result    (.executeQuery statement "SELECT current_database()")]
              (is (.next result))
              (is (= "analytics" (.getString result 1))))
            (let [error (try
                          (DriverManager/getConnection
                           (str "jdbc:postgresql://127.0.0.1:" (.getPort wire)
                                "/analytics?sslmode=disable&user=datahike&password=wrong"))
                          nil
                          (catch SQLException e e))]
              (is (= "28P01" (.getSQLState ^SQLException error))))
            (finally
              (listener/stop! runtime))))
        (finally
          (when (d/database-exists? database)
            (d/delete-database database))
          (system/close! system-config))))))
