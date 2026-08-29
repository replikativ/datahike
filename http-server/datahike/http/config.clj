(ns datahike.http.config
  "Standalone-server configuration from EDN, environment, and CLI.

   The EDN file remains the complete configuration surface. Environment and
   command-line options deliberately cover the small set operators need to
   change at deployment time. Precedence is CLI > environment > file."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]))

(defn- parse-port [value]
  (let [port (parse-long value)]
    (when-not (and port (<= 0 port 65535))
      (throw (ex-info "must be an integer from 0 through 65535" {})))
    port))

(defn- parse-bool [value]
  (case (str/lower-case value)
    "true" true
    "false" false
    (throw (ex-info "must be true or false" {}))))

(def ^:private levels #{:trace :debug :info :warn :error :fatal})

(defn- parse-level [value]
  (let [level (keyword (str/lower-case value))]
    (when-not (levels level)
      (throw (ex-info "must be trace, debug, info, warn, error, or fatal" {})))
    level))

(defn- non-blank [value]
  (when (str/blank? value)
    (throw (ex-info "must not be blank" {})))
  value)

(def ^:private settings
  [{:key :port         :parse parse-port}
   {:key :host         :parse non-blank}
   {:key :token        :parse non-blank}
   {:key :dev-mode     :parse parse-bool}
   {:key :level        :parse parse-level}
   ;; A deployment-friendly shorthand for the full
   ;; {:auth-db {:store {:backend :file :path ...}}} EDN shape.
   {:key :auth-db-path :parse non-blank}])

(defn env-name
  "The mechanical DATAHIKE_* name for a supported top-level config key."
  [key]
  (str "DATAHIKE_"
       (-> key name str/upper-case (str/replace "-" "_"))))

(def ^:private cli-options
  [["-f" "--config FILE" "Base EDN configuration file"]
   ["-p" "--port PORT" "HTTP port" :parse-fn parse-port]
   ["-H" "--host HOST" "HTTP bind address" :parse-fn non-blank]
   [nil "--token TOKEN" "Shared authentication token" :parse-fn non-blank]
   [nil "--token-file FILE" "Read the shared token from FILE" :parse-fn non-blank]
   [nil "--dev-mode BOOLEAN" "Enable or disable development authentication" :parse-fn parse-bool]
   ["-l" "--level LEVEL" "Log level" :parse-fn parse-level]
   [nil "--auth-db-path PATH" "File-backed permissions database path" :parse-fn non-blank]
   ["-h" "--help" "Show this help"]
   [nil "--version" "Show the Datahike version"]])

(defn- read-token-file [path]
  (try
    (let [token (some-> (slurp (io/file path)) str/trim-newline not-empty)]
      (or token
          (throw (ex-info "Token file is empty"
                          {:type :datahike.http/invalid-token-file :path path}))))
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (ex-info (str "Cannot read token file " path)
                      {:type :datahike.http/invalid-token-file
                       :path path
                       :cause-class (.getName (class e))}
                      e)))))

(defn- token-from-layer [config token-file source]
  (when (and (:token config) token-file)
    (throw (ex-info (str source " sets both token and token-file")
                    {:type :datahike.http/ambiguous-token-source
                     :source source})))
  (cond-> config
    token-file (assoc :token (read-token-file token-file))))

(defn- env-setting [env {:keys [key parse]}]
  (when-some [value (get env (env-name key))]
    [key (try
           (parse value)
           (catch Exception e
             (throw (ex-info (str "Invalid " (env-name key))
                             {:type :datahike.http/invalid-environment
                              :variable (env-name key)
                              :cause-class (.getName (class e))}
                             e))))]))

(defn env-config
  "The supported server overrides present in environment map `env`."
  [env]
  (let [config (into {} (keep #(env-setting env %)) settings)]
    (token-from-layer config (get env "DATAHIKE_TOKEN_FILE") "environment")))

(defn- cli-config [options]
  (let [config (select-keys options (map :key settings))]
    (token-from-layer config (:token-file options) "command line")))

(defn- file-config [path]
  (if-not path
    {}
    (try
      (let [config (edn/read-string (slurp (io/file path)))]
        (when-not (map? config)
          (throw (ex-info "Server config must be an EDN map" {})))
        config)
      (catch Exception e
        ;; EDN reader messages can quote the malformed value. Do not copy one
        ;; into startup logs: the same map commonly contains the bearer token.
        (throw (ex-info (str "Cannot read server config file " path)
                        {:type :datahike.http/invalid-config-file
                         :path path
                         :cause-class (.getName (class e))}
                        e))))))

(defn- normalize-shorthands [config]
  (if (contains? config :auth-db-path)
    (let [path (non-blank (:auth-db-path config))]
      (-> config
          (dissoc :auth-db-path)
          (assoc-in [:auth-db :store :backend] :file)
          (assoc-in [:auth-db :store :path] path)))
    config))

(defn usage [summary]
  (str "Usage: java -jar datahike-http-server.jar [options] [config.edn]\n\n"
       "Configuration precedence: command line > DATAHIKE_* environment > EDN file.\n\n"
       summary))

(defn resolve-config
  "Parse `args` and resolve a server action. The two-argument form accepts an
   environment map for tests and embedding launchers."
  ([args] (resolve-config args (System/getenv)))
  ([args env]
   (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
         positional-file (first arguments)]
     (cond
       (:help options)
       {:action :help :message (usage summary)}

       (:version options)
       {:action :version}

       (seq errors)
       (throw (ex-info (str "Invalid command line: " (str/join "; " errors))
                       {:type :datahike.http/invalid-command-line}))

       (> (count arguments) 1)
       (throw (ex-info "Only one positional config file is allowed"
                       {:type :datahike.http/invalid-command-line}))

       (and (:config options) positional-file)
       (throw (ex-info "Use either --config or a positional config file, not both"
                       {:type :datahike.http/invalid-command-line}))

       :else
       (let [path   (or (:config options) positional-file)
             config (-> (merge (file-config path)
                               (env-config env)
                               (cli-config options))
                        normalize-shorthands)]
         {:action :run :config config})))))
