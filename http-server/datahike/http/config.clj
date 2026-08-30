(ns datahike.http.config
  "Standalone-server configuration from EDN, environment, and CLI.

   The EDN file remains the complete configuration surface. Environment and
   command-line options deliberately cover the small set operators need to
   change at deployment time. Precedence is CLI > environment > file."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]])
  (:import [java.net InetAddress]))

(defn- parse-port [value]
  (let [port (parse-long value)]
    (when-not (and port (<= 0 port 65535))
      (throw (ex-info "must be an integer from 0 through 65535" {})))
    port))

(defn- parse-nonnegative-long [value]
  (let [number (parse-long value)]
    (when-not (and number (<= 0 number))
      (throw (ex-info "must be a nonnegative integer" {})))
    number))

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

(defn- parse-log-format [value]
  (case (str/lower-case value)
    "text" :text
    "json" :json
    (throw (ex-info "must be text or json" {}))))

(defn- non-blank [value]
  (when (str/blank? value)
    (throw (ex-info "must not be blank" {})))
  value)

(def ^:private settings
  [{:key :port         :parse parse-port}
   {:key :host         :parse non-blank}
   {:key :token        :parse non-blank}
   {:key :dev-mode     :parse parse-bool}
   {:key :shutdown-timeout-ms :parse parse-nonnegative-long}
   {:key :level        :parse parse-level}
   {:key :log-format   :parse parse-log-format}
   {:key :nrepl-port   :parse parse-port}
   {:key :nrepl-bind   :parse non-blank}
   {:key :nrepl-socket :parse non-blank}
   ;; A deployment-friendly shorthand for the full
   ;; {:system-db {:store {:backend :file :path ...}}} EDN shape.
   {:key :system-db-path :parse non-blank}])

(defn env-name
  "The mechanical DATAHIKE_* name for a supported top-level config key."
  [key]
  (str "DATAHIKE_"
       (-> key name str/upper-case (str/replace "-" "_"))))

(def ^:private cli-options
  [["-f" "--config FILE" "Base EDN configuration file"]
   ["-p" "--port PORT" "HTTP port" :parse-fn parse-port]
   ["-H" "--host HOST" "HTTP bind address (unauthenticated must be loopback)" :parse-fn non-blank]
   [nil "--token TOKEN" "Shared authentication token" :parse-fn non-blank]
   [nil "--token-file FILE" "Read the shared token from FILE" :parse-fn non-blank]
   [nil "--dev-mode BOOLEAN" "Enable or disable development authentication" :parse-fn parse-bool]
   [nil "--shutdown-timeout-ms MILLIS" "Grace period for in-flight requests" :parse-fn parse-nonnegative-long]
   ["-l" "--level LEVEL" "Log level" :parse-fn parse-level]
   [nil "--log-format FORMAT" "Log format: text or json" :parse-fn parse-log-format]
   [nil "--nrepl-port PORT" "Enable nREPL on a loopback TCP port" :parse-fn parse-port]
   [nil "--nrepl-bind ADDRESS" "nREPL loopback bind address (default 127.0.0.1)" :parse-fn non-blank]
   [nil "--nrepl-socket PATH" "Enable nREPL on an absolute Unix socket path" :parse-fn non-blank]
   [nil "--system-db-path PATH" "File-backed catalog and permissions database path" :parse-fn non-blank]
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

(defn- normalize-system-db-path [config]
  (if (contains? config :system-db-path)
    (let [path (non-blank (:system-db-path config))]
      (-> config
          (dissoc :system-db-path)
          (assoc-in [:system-db :store :backend] :file)
          (assoc-in [:system-db :store :path] path)))
    config))

(defn- normalize-nrepl-shorthands [config]
  (let [nrepl-keys (select-keys config [:nrepl-port :nrepl-bind :nrepl-socket])]
    (cond-> (apply dissoc config (keys nrepl-keys))
      (seq nrepl-keys)
      (assoc :nrepl (cond-> {}
                      (contains? nrepl-keys :nrepl-port)
                      (assoc :port (:nrepl-port nrepl-keys))
                      (contains? nrepl-keys :nrepl-bind)
                      (assoc :bind (:nrepl-bind nrepl-keys))
                      (contains? nrepl-keys :nrepl-socket)
                      (assoc :socket (:nrepl-socket nrepl-keys)))))))

(defn- merge-nrepl [lower higher]
  (cond
    (not (contains? higher :nrepl)) (:nrepl lower)
    (nil? (:nrepl higher)) nil
    (not (map? (:nrepl higher))) (:nrepl higher)
    :else
    (let [override (:nrepl higher)
          base (cond
                 (contains? override :socket) (dissoc (:nrepl lower) :port :bind)
                 (contains? override :port) (dissoc (:nrepl lower) :socket)
                 :else (:nrepl lower))]
      (merge base override))))

(defn- merge-layers [lower higher]
  (let [nrepl (merge-nrepl lower higher)
        merged (merge lower higher)]
    (if (or (contains? lower :nrepl) (contains? higher :nrepl))
      (assoc merged :nrepl nrepl)
      merged)))

(defn- loopback-addresses [host]
  (when (and (string? host) (not (str/blank? host)))
    (try
      (let [addresses (seq (InetAddress/getAllByName host))]
        (when (and addresses
                   (every? #(.isLoopbackAddress ^InetAddress %) addresses))
          addresses))
      (catch Exception _
        nil))))

(defn loopback-host?
  "True when every address `host` resolves to is a loopback address. A missing,
   wildcard, unknown, or partly non-loopback host is not safe for an
   unauthenticated standalone server."
  [host]
  (boolean (loopback-addresses host)))

(defn assert-safe-nrepl!
  "Validate and normalize optional nREPL configuration. TCP nREPL is always
   restricted to loopback because nREPL evaluates arbitrary JVM code and has
   no authentication layer of its own."
  [config]
  (if-not (contains? config :nrepl)
    config
    (let [nrepl (:nrepl config)
          fail (fn [message data]
                 (throw (ex-info message
                                 (assoc data :type :datahike.http/invalid-nrepl))))]
      (cond
        (nil? nrepl) (dissoc config :nrepl)
        (not (map? nrepl)) (fail ":nrepl must be a map" {:nrepl nrepl})
        (seq (remove #{:port :bind :socket} (keys nrepl)))
        (fail ":nrepl contains unsupported keys" {:keys (keys nrepl)})
        (= (contains? nrepl :port) (contains? nrepl :socket))
        (fail ":nrepl must contain exactly one of :port or :socket" {})
        (contains? nrepl :socket)
        (cond
          (contains? nrepl :bind)
          (fail ":nrepl :bind cannot be used with :socket" {})
          (or (not (string? (:socket nrepl))) (str/blank? (:socket nrepl)))
          (fail ":nrepl :socket must be a nonblank string" {})
          (not (.isAbsolute (io/file (:socket nrepl))))
          (fail ":nrepl :socket must be an absolute path" {:socket (:socket nrepl)})
          :else config)
        :else
        (let [port (:port nrepl)
              bind (get nrepl :bind "127.0.0.1")]
          (when-not (and (integer? port) (<= 0 port 65535))
            (fail ":nrepl :port must be an integer from 0 through 65535" {:port port}))
          (if-let [addresses (loopback-addresses bind)]
            (assoc config :nrepl (assoc nrepl :bind (.getHostAddress ^InetAddress (first addresses))))
            (fail ":nrepl :bind must resolve only to loopback addresses" {:bind bind})))))))

(defn- configured-authentication?
  [{:keys [dev-mode auth token validator]}]
  ;; Dev mode and upstream auth precede the token/validator chain and admit
  ;; requests themselves, so credentials beside either bypass do not make the
  ;; effective standalone config safe.
  (and (not dev-mode)
       (not= :upstream auth)
       (or (and (string? token) (not (str/blank? token)))
           (ifn? validator))))

(defn assert-safe-bind!
  "Refuse to expose the standalone server without effective authentication.
   An unauthenticated loopback hostname is pinned to the numeric address that
   was checked, so the HTTP adapter does not perform a second DNS resolution.
   Returns the validated config."
  [{:keys [host] :as config}]
  (if (configured-authentication? config)
    config
    (if-let [addresses (loopback-addresses host)]
      (assoc config :host (.getHostAddress ^InetAddress (first addresses)))
      (throw (ex-info
              (str "Refusing unauthenticated HTTP bind to " (pr-str host)
                   "; configure a nonblank :token or :validator, or set :host to a loopback address")
              {:type :datahike.http/unsafe-bind
               :host host})))))

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
             layers (map normalize-nrepl-shorthands
                         [(file-config path) (env-config env) (cli-config options)])
             config (-> (reduce merge-layers {} layers)
                        normalize-system-db-path
                        assert-safe-nrepl!)]
         {:action :run :config config})))))
