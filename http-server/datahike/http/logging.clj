(ns datahike.http.logging
  "Process-wide logging for the standalone server artifact.

   Datahike events arrive through Trove and dependency events through SLF4J.
   Both are filtered at the configured level and written only to stdout."
  (:require [clojure.string :as str]
            [jsonista.core :as json]
            [taoensso.trove :as trove]
            [taoensso.trove.console :as trove-console])
  (:import [ch.qos.logback.classic Level Logger LoggerContext PatternLayout]
           [ch.qos.logback.classic.spi ILoggingEvent ThrowableProxyUtil]
           [ch.qos.logback.core ConsoleAppender LayoutBase]
           [ch.qos.logback.core.encoder LayoutWrappingEncoder]
           [java.io PrintWriter StringWriter]
           [java.time Instant]
           [org.slf4j LoggerFactory]
           [org.slf4j.event KeyValuePair]
           [org.slf4j.spi LoggingEventBuilder]))

(def ^:private level-rank
  {:trace 10 :debug 20 :info 30 :warn 40 :error 50 :fatal 60 :report 70})

(defn- enabled? [minimum level]
  (>= (get level-rank level -1)
      (get level-rank minimum (level-rank :info))))

(defn- throwable-map [^Throwable error]
  (let [writer (StringWriter.)]
    (.printStackTrace error (PrintWriter. writer))
    {:class (.getName (class error))
     :message (.getMessage error)
     :stack-trace (str writer)}))

(declare json-safe)

(defn- json-map [value]
  (into {}
        (map (fn [[key item]]
               [(if (or (keyword? key) (string? key)) key (str key))
                (json-safe item)]))
        value))

(defn- json-safe [value]
  (cond
    (nil? value) nil
    (or (string? value) (number? value) (boolean? value)) value
    (keyword? value) (subs (str value) 1)
    (symbol? value) (str value)
    (instance? Throwable value) (throwable-map value)
    (map? value) (json-map value)
    (set? value) (mapv json-safe value)
    (sequential? value) (mapv json-safe value)
    (and value (.isArray (class value))) (mapv json-safe (seq value))
    :else (str value)))

(defn- event-name [id]
  (when id
    (if (or (keyword? id) (symbol? id))
      (subs (str id) 1)
      (str id))))

(defn trove-event-map
  "Convert one forced Trove event to the standalone JSON schema. Public for
   consumers that want to validate their log pipeline against the schema."
  [namespace coords level id {:keys [msg data error]}]
  (cond-> {:timestamp (str (Instant/now))
           :level (name level)
           :logger (str namespace)}
    coords (assoc :source {:line (first coords) :column (second coords)})
    id (assoc :event (event-name id))
    msg (assoc :message msg)
    (not-empty data) (assoc :data (json-safe data))
    error (assoc :error (throwable-map error))))

(def ^:private stdout-lock (Object.))

(defn json-log-fn [minimum]
  (fn [namespace coords level id lazy-event]
    (when (enabled? minimum level)
      (let [line (json/write-value-as-string
                  (trove-event-map namespace coords level id (force lazy-event)))]
        (locking stdout-lock
          (println line)
          (flush))))))

(defn- slf4j-event-map [^ILoggingEvent event]
  (let [throwable (.getThrowableProxy event)
        key-values (not-empty
                    (into {}
                          (map (fn [^KeyValuePair pair]
                                 [(.-key pair) (json-safe (.-value pair))]))
                          (.getKeyValuePairs event)))]
    (cond-> {:timestamp (str (.getInstant event))
             :level (str/lower-case (str (.getLevel event)))
             :logger (.getLoggerName event)
             :thread (.getThreadName event)
             :message (.getFormattedMessage event)}
      (not-empty (.getMDCPropertyMap event))
      (assoc :mdc (.getMDCPropertyMap event))

      key-values
      (assoc :data key-values)

      throwable
      (assoc :error {:class (.getClassName throwable)
                     :message (.getMessage throwable)
                     :stack-trace (ThrowableProxyUtil/asString throwable)}))))

(defn- json-layout []
  (proxy [LayoutBase] []
    (doLayout [event]
      (str (json/write-value-as-string (slf4j-event-map event)) "\n"))))

(defn- text-layout []
  (doto (PatternLayout.)
    (.setPattern "%date{ISO8601,UTC} %-5level [%thread] %logger - %msg%n%ex")))

(defn- logback-level [level]
  (case level
    :trace Level/TRACE
    :debug Level/DEBUG
    :info Level/INFO
    :warn Level/WARN
    :error Level/ERROR
    :fatal Level/ERROR
    :report Level/ERROR
    Level/INFO))

(defn- external-logback-config? []
  (some? (System/getProperty "logback.configurationFile")))

(defn- configure-logback! [level format]
  (let [factory (LoggerFactory/getILoggerFactory)]
    (when-not (instance? LoggerContext factory)
      (throw (ex-info "The standalone server requires its bundled Logback provider"
                      {:type :datahike.http/invalid-logging-provider
                       :provider (.getName (class factory))})))
    (let [^LoggerContext context factory
          ^Logger root (.getLogger context Logger/ROOT_LOGGER_NAME)]
      (if (external-logback-config?)
        (.setLevel root (logback-level level))
        (let [^LayoutBase layout (if (= :json format) (json-layout) (text-layout))
              encoder (doto (LayoutWrappingEncoder.)
                        (.setContext context)
                        (.setLayout layout))
              appender (doto (ConsoleAppender.)
                         (.setContext context)
                         (.setName "stdout")
                         (.setTarget "System.out")
                         (.setEncoder encoder))]
          (.reset context)
          (.setContext layout context)
          (.start layout)
          (.start encoder)
          (.start appender)
          (.setLevel root (logback-level level))
          (.addAppender root appender))))))

(defn- slf4j-log-fn [namespace coords level id lazy-event]
  (let [logger (LoggerFactory/getLogger ^String (str namespace))
        ^LoggingEventBuilder builder
        (case level
          :trace (when (.isTraceEnabled logger) (.atTrace logger))
          :debug (when (.isDebugEnabled logger) (.atDebug logger))
          :info (when (.isInfoEnabled logger) (.atInfo logger))
          :warn (when (.isWarnEnabled logger) (.atWarn logger))
          :error (when (.isErrorEnabled logger) (.atError logger))
          :fatal (when (.isErrorEnabled logger) (.atError logger))
          :report (.atInfo logger)
          nil)]
    (when builder
      (let [{:keys [msg data error]} (force lazy-event)]
        (when id (.addKeyValue builder "event" (event-name id)))
        (when coords (.addKeyValue builder "source" (str coords)))
        (doseq [[key value] data]
          (.addKeyValue builder (if (keyword? key) (name key) (str key)) (str value)))
        (when msg (.setMessage builder ^String (str msg)))
        (when error (.setCause builder error))
        (.log builder)))))

(defn configure!
  "Configure Datahike and dependency logging for the standalone process."
  [{:keys [level log-format] :or {level :info log-format :text}}]
  (when-not (contains? level-rank level)
    (throw (ex-info "Log level must be trace, debug, info, warn, error, or fatal"
                    {:type :datahike.http/invalid-log-level})))
  (when-not (#{:text :json} log-format)
    (throw (ex-info "Log format must be text or json"
                    {:type :datahike.http/invalid-log-format})))
  (trove/set-log-fn!
   (cond
     (external-logback-config?) slf4j-log-fn
     (= :json log-format) (json-log-fn level)
     :else (trove-console/get-log-fn {:min-level level})))
  (configure-logback! level log-format)
  {:level level :log-format log-format})

(defn- bootstrap-config []
  ;; Invalid values are reported later by config/resolve-config inside -main's
  ;; error boundary. Bootstrap only prevents early dependency logs from going
  ;; to stderr or using the wrong layout.
  (let [env (System/getenv)
        level (some-> (get env "DATAHIKE_LEVEL") str/lower-case keyword)
        format (some-> (get env "DATAHIKE_LOG_FORMAT") str/lower-case keyword)]
    {:level (if (contains? level-rank level) level :info)
     :log-format (if (#{:text :json} format) format :text)}))

(configure! (bootstrap-config))
