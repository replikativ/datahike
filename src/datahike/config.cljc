(ns datahike.config
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [environ.core :refer [env]])
  (:import [java.net URI]))

(s/def ::index #{:datahike.index/hitchhiker-tree :datahike.index/persistent-set})
(s/def ::keep-history? boolean?)
(s/def ::schema-flexibility #{:read :write})
(s/def ::entity (s/or :map associative? :vec vector?))
(s/def ::initial-tx (s/nilable (s/or :data (s/coll-of ::entity) :path string?)))
(s/def ::name string?)

(s/def ::backend #{:mem :file :level :pg})
(s/def ::username (s/nilable string?)) ;;pg
(s/def ::password (s/nilable string?)) ;; :pg
(s/def ::path (s/nilable string?)) ;; :file, :leveldb, :pg
(s/def ::host (s/nilable string?)) ;; :pg
(s/def ::port (s/nilable int?)) ;; :pg
(s/def ::id (s/nilable string?)) ;; :mem

(s/def ::name string?)

(s/def :datahike/store (s/keys :req-un [::backend]
                               :opt-un [::username ::password ::path ::host ::port ::id]))
(s/def :datahike/config (s/keys :req-un [:datahike/store ]
                                :opt-un [::index
                                         ::keep-history?
                                         ::schema-flexibility
                                         ::initial-tx
                                         ::name]))

(s/def :deprecated/schema-on-read boolean?)
(s/def :deprecated/temporal-index boolean?)
(s/def :deprecated/config (s/keys :req-un [:datahike/store]
                                  :opt-un [:deprecated/temporal-index :deprecated/schema-on-read]))

(s/fdef datahike.config/validate-edn
  :args (s/cat :config-str string?)
  :ret :datahike/config)

(defrecord Store [backend username password path host port])
(defrecord Configuration [store schema-on-read temporal-index index])

(defn from-deprecated
  [{:keys [backend username password path host port] :as backend-cfg}
   & {:keys [schema-on-read temporal-index index initial-tx]
      :as index-cfg
      :or {schema-on-read false
           index :datahike.index/hitchhiker-tree
           temporal-index true}}]
  {:store (merge {:backend backend}
                 (case backend
                   :mem {:id host}
                   :pg {:username username
                        :password password
                        :path path
                        :host host
                        :port port
                        :id (str (java.util.UUID/randomUUID))}
                   :level {:path path}
                   :file {:path path}))
   :index index
   :keep-history? temporal-index
   :initial-tx initial-tx
   :schema-flexibility (if (true? schema-on-read) :read :write)})

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

(defn storeless-config []
  {:store nil
   :keep-history? false
   :schema-flexibility :read
   :name (str (java.util.UUID/randomUUID))
   :index :datahike.index/hitchhiker-tree})

(defn remove-nil [m]
  (into {} (remove (comp nil? second) m)))

(defn load-config
  "Loading config from Configuration-record passed as argument."
  ([]
   (load-config nil nil))
  ([config-as-arg]
   (load-config config-as-arg nil))
  ([config-as-arg opts]
   (let [config-as-arg (if (s/valid? :datahike/config-depr config-as-arg)
                         (apply from-deprecated config-as-arg (first opts))
                         config-as-arg)
         id (str (java.util.UUID/randomUUID))
         backend (keyword (:datahike-store-backend env :mem))
         store-config {:backend backend
                       :username (:datahike-store-username env)
                       :password (:datahike-store-password env)
                       :path (:datahike-store-path env)
                       :host (:datahike-store-host env)
                       :port (int-from-env :datahike-store-port nil)
                       :id (:datahike-store-id env id)}
         config {:store (remove-nil store-config)
                 :initial-tx (:datahike-intial-tx env)
                 :keep-history? (bool-from-env :datahike-keep-history true)
                 :name (:datahike-name env id)
                 :schema-flexibility (keyword (:datahike-schema-flexibility env :write))
                 :index (keyword "datahike.index" (:datahike-index env "hitchhiker-tree"))}
         merged-config ((comp remove-nil deep-merge) config config-as-arg)
         {:keys [backend username password path host port] :as store} (:store merged-config)
         {:keys [keep-history? name schema-flexibility index initial-tx]} merged-config]
     (validate-config-attribute ::backend backend store)
     (validate-config-attribute ::keep-history? keep-history? merged-config)
     (validate-config-attribute ::schema-flexibility schema-flexibility merged-config)
     (validate-config-attribute ::index index merged-config)
     (validate-config-attribute ::name name merged-config)
     (validate-config-attribute ::initial-tx initial-tx merged-config)
     (validate-config merged-config)
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
    (validate-config-attribute ::backend backend config)
    config))

(defn validate-config-depr [config]
  (when-not (s/valid? :datahike/config-depr config)
    (throw (ex-info "Invalid datahike configuration." config))))
;; deprecation end
