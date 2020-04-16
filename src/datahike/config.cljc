(ns datahike.config
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [environ.core :refer [env]])
  (:import [java.net URI]))

(s/def ::backend #{:mem :file :level :pg})
(s/def ::username (s/nilable string?))
(s/def ::password (s/nilable string?))
(s/def ::host (s/nilable string?))
(s/def ::port (s/nilable int?))
(s/def ::path (s/nilable string?))
(s/def ::dbname (s/nilable string?))
(s/def ::ssl (s/nilable boolean?))
(s/def ::sslfactory (s/nilable string?))

(s/def ::schema-on-read boolean?)
(s/def ::temporal-index boolean?)

(s/def :datahike/store (s/keys :req-un [::backend]
                               :opt-un [::username ::password ::host ::port ::path ::dbname ::ssl ::sslfactory]))
(s/def :datahike/config (s/keys :req-un [:datahike/store]
                                :opt-un [::schema-on-read ::temporal-index]))

(s/fdef datahike.config/validate-edn
  :args (s/cat :config-str string?)
  :ret :datahike/config)

(defrecord Store [backend username password path host port dbname ssl sslfactory])
(defrecord Configuration [store schema-on-read temporal-index])

(defn int-from-env
  [key default]
  (try
    (Integer/parseInt (get env key (str default)))
    (catch Exception _ default)))

(defn bool-from-env
  [key default]
  (try
    (Boolean/parseBoolean (get env key default))
    (catch Exception _ default)))

(defn deep-merge
  "Recursively merges maps and records."
  [& maps]
  (letfn [(m [& xs]
            (if (some #(map? %) xs)
              (apply merge-with m xs)
              (last xs)))]
    (reduce m maps)))

(defn validate-config-attribute [attribute value config]
  (when-not (s/valid? attribute value)
    (throw (ex-info (str "Bad value " value " at " (name attribute)
                         ", value does not match configuration definition. Must be conform to: "
                         (s/describe attribute)) config))))

(defn validate-config [config]
  (when-not (s/valid? :datahike/config config)
    (throw (ex-info "Invalid datahike configuration." config))))

(defn- load-config
  "Loading config from Configuration-record passed as argument."
  ([]
   (load-config nil))
  ([config-as-arg]
   (let [config        (Configuration.
                        (Store.
                         (keyword (:datahike-store-backend env :mem))
                         (:datahike-store-username env)
                         (:datahike-store-password env)
                         (:datahike-store-path env)
                         (:datahike-store-host env)
                         (int-from-env :datahike-store-port nil)
                         (:datahike-store-dbname env)
                         (bool-from-env :datahike-store-ssl false)
                         (:datahike-store-sslfactory env))
                        (bool-from-env :datahike-schema-on-read false)
                        (bool-from-env :datahike-temporal-index true))
         merged-config (deep-merge config config-as-arg)
         {:keys [backend username password path host port] :as store} (:store merged-config)
         {:keys [schema-on-read temporal-index]} merged-config
         _             (validate-config-attribute ::backend backend store)
         _             (validate-config-attribute ::schema-on-read schema-on-read merged-config)
         _             (validate-config-attribute ::temporal-index temporal-index merged-config)
         _             (validate-config merged-config)]
     merged-config)))

(defonce
  ^{:doc "A record of the current configuration."}
  config (load-config))

(defn reload-config
  ([]
   (reload-config nil))
  ([additional-config]
   (alter-var-root #'config (fn [_] (load-config additional-config)))))

;; deprecation begin
(s/def ::backend-depr keyword?)
(s/def ::username-depr string?)
(s/def ::password-depr string?)
(s/def ::path-depr string?)
(s/def ::host-depr string?)
(s/def ::port-depr int?)
(s/def ::uri-depr string?)

(s/def :datahike/config-depr (s/keys :req-un [::backend]
                               :opt-un [::username ::password ::path ::host ::port]))

(defn uri->config [uri]
  (let [base-uri (URI. uri)
        _ (when-not (= (.getScheme base-uri) "datahike")
            (throw (ex-info "URI scheme is not datahike conform." {:uri uri})))
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        backend (keyword (.getScheme sub-uri))
        [username password] (when-let [user-info (.getUserInfo sub-uri)]
                              (clojure.string/split user-info #":"))
        credentials (when-not (and (nil? username) (nil? password))
                      {:username username
                       :password password})
        port (.getPort sub-uri)
        path (.getPath sub-uri)
        host (.getHost sub-uri)
        config (merge
                {:backend backend
                 :uri uri}
                credentials
                (when host
                  {:host host})
                (when-not (empty? path)
                  {:path path})
                (when (<= 0 port)
                  {:port port}))]
    (validate-config-attribute ::backend backend config)
    config))

(defn validate-config-depr [config]
  (when-not (s/valid? :datahike/config-depr config)
    (throw (ex-info "Invalid datahike configuration." config))))
;; deprecation end
