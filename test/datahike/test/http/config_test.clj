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
  (is (= "DATAHIKE_AUTH_DB_PATH" (config/env-name :auth-db-path))))

(deftest command-line-overrides-environment-overrides-file
  (let [file (temp-file (pr-str {:port 1001
                                 :host "file-host"
                                 :token "file-token"
                                 :dev-mode false
                                 :level :info
                                 :metrics false
                                 :auth-db {:store {:backend :memory}}}))
        env  {"DATAHIKE_PORT" "2002"
              "DATAHIKE_HOST" "env-host"
              "DATAHIKE_TOKEN" "env-token"
              "DATAHIKE_DEV_MODE" "true"
              "DATAHIKE_LEVEL" "warn"
              "DATAHIKE_AUTH_DB_PATH" "/env/auth"}
        args ["--config" (.getPath file)
              "--port" "3003"
              "--host" "cli-host"
              "--token" "cli-token"
              "--dev-mode" "false"
              "--level" "error"
              "--auth-db-path" "/cli/auth"]
        resolved (:config (config/resolve-config args env))]
    (is (= 3003 (:port resolved)))
    (is (= "cli-host" (:host resolved)))
    (is (= "cli-token" (:token resolved)))
    (is (false? (:dev-mode resolved)))
    (is (= :error (:level resolved)))
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
                          (config/resolve-config ["--dev-mode" "perhaps"] {}))))
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
