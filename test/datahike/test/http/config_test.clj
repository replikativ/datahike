(ns datahike.test.http.config-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.http.config :as config]))

(defn- temp-file [contents]
  (doto (java.io.File/createTempFile "datahike-server-config-" ".edn")
    (.deleteOnExit)
    (spit contents)))

(deftest environment-names-are-mechanical
  (is (= "DATAHIKE_PORT" (config/env-name :port)))
  (is (= "DATAHIKE_DEV_MODE" (config/env-name :dev-mode)))
  (is (= "DATAHIKE_SHUTDOWN_TIMEOUT_MS" (config/env-name :shutdown-timeout-ms)))
  (is (= "DATAHIKE_LOG_FORMAT" (config/env-name :log-format)))
  (is (= "DATAHIKE_AUTH_DB_PATH" (config/env-name :auth-db-path))))

(deftest standalone-bind-safety
  (testing "literal and named loopback hosts need no authentication"
    (doseq [host ["127.0.0.1" "127.23.4.5" "::1" "localhost"]]
      (is (config/loopback-host? host) host)
      (let [validated (config/assert-safe-bind! {:host host :port 4444})]
        (is (= 4444 (:port validated)) host)
        (is (config/loopback-host? (:host validated)) host))))
  (testing "wildcard and non-loopback binds require effective authentication"
    (doseq [host [nil "0.0.0.0" "::" "192.0.2.1"]]
      (let [error (try
                    (config/assert-safe-bind! {:host host})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (= :datahike.http/unsafe-bind (:type (ex-data error))) (pr-str host))
        (is (= host (:host (ex-data error))) (pr-str host)))))
  (testing "a token or custom validator protects a remote bind"
    (is (= "0.0.0.0"
           (:host (config/assert-safe-bind! {:host "0.0.0.0" :token "secret"}))))
    (is (= "0.0.0.0"
           (:host (config/assert-safe-bind! {:host "0.0.0.0"
                                             :validator (constantly {:sub "user"})})))))
  (testing "authentication bypasses do not make a public bind safe"
    (doseq [unsafe [{:host "0.0.0.0" :token "secret" :dev-mode true}
                    {:host "0.0.0.0" :validator identity :dev-mode true}
                    {:host "0.0.0.0" :auth :upstream}
                    {:host "0.0.0.0" :auth :upstream :token "secret"}
                    {:host "0.0.0.0" :auth :upstream :validator identity}
                    {:host "0.0.0.0" :token ""}]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Refusing unauthenticated HTTP bind"
                            (config/assert-safe-bind! unsafe))
          (pr-str unsafe)))))

(deftest command-line-overrides-environment-overrides-file
  (let [file (temp-file (pr-str {:port 1001
                                 :host "file-host"
                                 :token "file-token"
                                 :dev-mode false
                                 :shutdown-timeout-ms 10000
                                 :level :info
                                 :metrics false
                                 :auth-db {:store {:backend :memory}}}))
        env  {"DATAHIKE_PORT" "2002"
              "DATAHIKE_HOST" "env-host"
              "DATAHIKE_TOKEN" "env-token"
              "DATAHIKE_DEV_MODE" "true"
              "DATAHIKE_SHUTDOWN_TIMEOUT_MS" "20000"
              "DATAHIKE_LEVEL" "warn"
              "DATAHIKE_LOG_FORMAT" "text"
              "DATAHIKE_AUTH_DB_PATH" "/env/auth"}
        args ["--config" (.getPath file)
              "--port" "3003"
              "--host" "cli-host"
              "--token" "cli-token"
              "--dev-mode" "false"
              "--shutdown-timeout-ms" "30000"
              "--level" "error"
              "--log-format" "json"
              "--auth-db-path" "/cli/auth"]
        resolved (:config (config/resolve-config args env))]
    (is (= 3003 (:port resolved)))
    (is (= "cli-host" (:host resolved)))
    (is (= "cli-token" (:token resolved)))
    (is (false? (:dev-mode resolved)))
    (is (= 30000 (:shutdown-timeout-ms resolved)))
    (is (= :error (:level resolved)))
    (is (= :json (:log-format resolved)))
    (is (false? (:metrics resolved)) "unoverridden EDN remains the full surface")
    (is (= {:backend :file :path "/cli/auth"}
           (get-in resolved [:auth-db :store])))))

(deftest positional-config-remains-backward-compatible
  (let [file (temp-file "{:port 4444}")]
    (is (= {:action :run :config {:port 4444}}
           (config/resolve-config [(.getPath file)] {})))))

(deftest token-files-are-secret-mount-friendly-and-layered
  (let [env-token (temp-file "environment-secret\n")
        cli-token (temp-file "cli-secret\r\n")]
    (is (= "environment-secret"
           (get-in (config/resolve-config [] {"DATAHIKE_TOKEN_FILE" (.getPath env-token)})
                   [:config :token])))
    (is (= "cli-secret"
           (get-in (config/resolve-config ["--token-file" (.getPath cli-token)]
                                          {"DATAHIKE_TOKEN" "environment-secret"})
                   [:config :token]))
        "a CLI token file overrides the environment layer")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"both token and token-file"
                          (config/resolve-config []
                                                 {"DATAHIKE_TOKEN" "secret"
                                                  "DATAHIKE_TOKEN_FILE" (.getPath env-token)})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"both token and token-file"
                          (config/resolve-config ["--token" "secret"
                                                  "--token-file" (.getPath cli-token)] {})))))

(deftest invalid-input-fails-before-the-server-starts
  (testing "typed environment and CLI values are strict"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid DATAHIKE_PORT"
                          (config/resolve-config [] {"DATAHIKE_PORT" "not-a-port"})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid command line"
                          (config/resolve-config ["--dev-mode" "perhaps"] {})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid DATAHIKE_SHUTDOWN_TIMEOUT_MS"
                          (config/resolve-config [] {"DATAHIKE_SHUTDOWN_TIMEOUT_MS" "-1"})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid DATAHIKE_LOG_FORMAT"
                          (config/resolve-config [] {"DATAHIKE_LOG_FORMAT" "xml"}))))
  (testing "config parse errors never echo the possibly secret EDN value"
    (let [secret "must-not-appear"
          file   (temp-file (str "{:token \"" secret "\" :broken"))]
      (try
        (config/resolve-config [(.getPath file)] {})
        (is false "malformed config must fail")
        (catch Exception e
          (is (not (re-find (re-pattern secret) (ex-message e))))))))
  (is (= :help (:action (config/resolve-config ["--help"] {}))))
  (is (= :version (:action (config/resolve-config ["--version"] {})))))
