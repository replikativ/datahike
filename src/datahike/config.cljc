(ns ^:no-doc datahike.config
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [zufall.core :as z]
            [environ.core :refer [env]]
            [datahike.tools :as tools]
            [datahike.store :as ds]
            [datahike.constants :as c])
  (:import [java.net URI]))

(s/def ::index #{:datahike.index/hitchhiker-tree :datahike.index/persistent-set})
(s/def ::keep-history? boolean?)
(s/def ::schema-flexibility #{:read :write})
(s/def ::attribute-refs? boolean?)
(s/def ::entity (s/or :map associative? :vec vector?))
(s/def ::initial-tx (s/nilable (s/or :data (s/coll-of ::entity) :path string?)))
(s/def ::name string?)

(s/def ::index-config map?)
(s/def ::index-b-factor long)
(s/def ::index-log-size long)
(s/def ::index-data-node-size long)

(s/def ::store map?)

(s/def :datahike/config (s/keys :req-un [:datahike/store]
                                :opt-un [::index
                                         ::index-config
                                         ::keep-history?
                                         ::schema-flexibility
                                         ::attribute-refs?
                                         ::initial-tx
                                         ::name]))

(s/def :deprecated/schema-on-read boolean?)
(s/def :deprecated/temporal-index boolean?)
(s/def :deprecated/config (s/keys :req-un [:datahike/store]
                                  :opt-un [:deprecated/temporal-index :deprecated/schema-on-read]))

(defn from-deprecated
  [{:keys [backend username password path host port] :as backend-cfg}
   & {:keys [schema-on-read temporal-index index initial-tx]
      :as index-cfg
      :or {schema-on-read false
           index :datahike.index/hitchhiker-tree
           temporal-index true}}]
  {:store (merge {:backend backend}
                 (case backend
                   :mem {:id (or host path)}
                   :pg {:username username
                        :password password
                        :path path
                        :host host
                        :port port
                        :id (str (java.util.UUID/randomUUID))}
                   :level {:path path}
                   :file {:path path}))
   :index index
   :index-config {:index-b-factor       c/default-index-b-factor
                  :index-log-size       c/default-index-log-size
                  :index-data-node-size c/default-index-data-node-size}
   :keep-history? temporal-index
   :attribute-refs? false
   :initial-tx initial-tx
   :schema-flexibility (if (true? schema-on-read) :read :write)
   :cache-size 100000})

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

(defn map-from-env [key default]
  (try
    (edn/read-string (get env key (str default)))
    (catch Exception _ default)))

(defn validate-config-attribute [attribute value config]
  (when-not (s/valid? attribute value)
    (throw (ex-info (str "Bad value " value " at " (name attribute)
                         ", value does not match configuration definition. Must be conform to: "
                         (s/describe attribute)) config))))

(defn validate-config [{:keys [attribute-refs? schema-flexibility] :as config}]
  (when-not (s/valid? :datahike/config config)
    (throw (ex-info "Invalid Datahike configuration." (s/explain-data :datahike/config config))))
  (when (and attribute-refs? (= :read schema-flexibility))
    (throw (ex-info "Attribute references cannot be used with schema-flexibility ':read'." config))))

(defn storeless-config []
  {:store nil
   :keep-history? false
   :schema-flexibility :read
   :name (z/rand-german-mammal)
   :attribute-refs? false
   :index :datahike.index/hitchhiker-tree
   :cache-size 100000
   :index-config {:index-b-factor       c/default-index-b-factor
                  :index-log-size       c/default-index-log-size
                  :index-data-node-size c/default-index-data-node-size}})

(defn remove-nils
  "Thanks to https://stackoverflow.com/a/34221816"
  [m]
  (let [f (fn [x]
            (if (map? x)
              (let [kvs (filter (comp not nil? second) x)]
                (if (empty? kvs) nil (into {} kvs)))
              x))]
    (clojure.walk/postwalk f m)))

(defn load-config
  "Load and validate configuration with defaults from the store."
  ([]
   (load-config nil nil))
  ([config-as-arg]
   (load-config config-as-arg nil))
  ([config-as-arg opts]
   (let [config-as-arg (if (s/valid? :datahike/config-depr config-as-arg)
                         (apply from-deprecated config-as-arg (first opts))
                         config-as-arg)
         store-config (ds/default-config (merge
                                          {:backend (keyword (:datahike-store-backend env :mem))}
                                          (:store config-as-arg)))
         config {:store store-config
                 :initial-tx (:datahike-intial-tx env)
                 :keep-history? (bool-from-env :datahike-keep-history true)
                 :attribute-refs? (bool-from-env :datahike-attribute-refs false)
                 :name (:datahike-name env (z/rand-german-mammal))
                 :schema-flexibility (keyword (:datahike-schema-flexibility env :write))
                 :index (keyword "datahike.index" (:datahike-index env "hitchhiker-tree"))
                 :cache-size (:cache-size env 100000)
                 :index-config {:index-b-factor       (int-from-env :datahike-b-factor c/default-index-b-factor)
                                :index-log-size       (int-from-env :datahike-log-size c/default-index-log-size)
                                :index-data-node-size (int-from-env :datahike-data-node-size c/default-index-data-node-size)}}
         merged-config ((comp remove-nils tools/deep-merge) config config-as-arg)
         {:keys [schema-flexibility initial-tx store attribute-refs?]} merged-config
         config-spec (ds/config-spec store)]
     (when config-spec
       (when-not (s/valid? config-spec store)
         (throw (ex-info "Invalid store configuration." (s/explain-data config-spec store)))))
     (when-not (s/valid? :datahike/config merged-config)
       (throw (ex-info "Invalid Datahike configuration." (s/explain-data :datahike/config merged-config))))
     (when (and attribute-refs? (= :read schema-flexibility))
       (throw (ex-info "Attribute references cannot be used with schema-flexibility ':read'." config)))
     (if (string? initial-tx)
       (update merged-config :initial-tx (fn [path] (-> path slurp read-string)))
       merged-config))))

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
    config))

(defn validate-config-depr [config]
  (when-not (s/valid? :datahike/config-depr config)
    (throw (ex-info "Invalid datahike configuration." config))))
;; deprecation end
