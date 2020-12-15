(ns datahike.config
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            #?(:clj [zufall.core :as z])
            [environ.core :refer [env]]
            [taoensso.timbre :as log]
            [datahike.store :as ds]
            [clojure.string :as str])
  #?(:clj (:import [java.net URI]))) ; Maybe use https://github.com/lambdaisland/uri ??

(s/def ::index #{:datahike.index/hitchhiker-tree :datahike.index/persistent-set})
(s/def ::keep-history? boolean?)
(s/def ::schema-flexibility #{:read :write})
(s/def ::entity (s/or :map associative? :vec vector?))
(s/def ::initial-tx (s/nilable (s/or :data (s/coll-of ::entity) :path string?)))
(s/def ::name string?)

(s/def ::store map?)

(s/def :datahike/config (s/keys :req-un [:datahike/store]
                                :opt-un [::index
                                         ::keep-history?
                                         ::schema-flexibility
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
                        :id (str #?(:clj (java.util.UUID/randomUUID)
                                    :cljs (random-uuid)))}
                   :level {:path path}
                   :file {:path path}))
   :index index
   :keep-history? temporal-index
   :initial-tx initial-tx
   :schema-flexibility (if (true? schema-on-read) :read :write)})

(defn int-from-env
  [key default]
  (let [int-str (get env key (str default))]
    #?(:clj (try
              (Integer/parseInt int-str)
              (catch Exception _ default))
       :cljs (try
               (js/parseInt int-str)
               (catch js/Error e e)))))

(defn bool-from-env
  [key default]
  (let [bool-str (get env key default)]
    #?(:clj (try
              (Boolean/parseBoolean bool-str)
              (catch Exception _ default))
       :cljs default #_(if (or (= bool-str "true") (= bool-str "false")) ;; TODO: fix environment settings for browsers
               (edn/read-string bool-str)
               (throw (js/Error. (str "Datom/-nth: Index out of bounds: " bool-str))))))

  (defn map-from-env [key default]
    (try
      (edn/read-string (get env key (str default)))
      #?(:clj (catch Exception _ default)
         :cljs (catch js/Error e e)))))

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
   :name #?(:clj (z/rand-german-mammal)
            :cljs "TODO: fix this")
   :index :datahike.index/hitchhiker-tree})

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
                 :name (:datahike-name env #?(:clj (z/rand-german-mammal)
                                              :cljs  "TODO: fix this"))
                 :schema-flexibility (keyword (:datahike-schema-flexibility env :write))
                 :index (keyword "datahike.index" (:datahike-index env "hitchhiker-tree"))}
         merged-config ((comp remove-nils deep-merge) config config-as-arg)
         _             (log/debug "Using config " merged-config)
         {:keys [keep-history? name schema-flexibility index initial-tx store]} merged-config
         config-spec (ds/config-spec store)]
     (when config-spec
       (when-not (s/valid? config-spec store)
         (throw (ex-info "Invalid store configuration." (s/explain-data config-spec store)))))
     (when-not (s/valid? :datahike/config merged-config)
       (throw (ex-info "Invalid Datahike configuration." (s/explain-data :datahike/config merged-config))))
     (if (string? initial-tx)
       merged-config #_(update merged-config :initial-tx (fn [path] (-> path slurp read-string)))
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

#?(:clj (defn uri->config [uri]
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
            config)))

(defn validate-config-depr [config]
  (when-not (s/valid? :datahike/config-depr config)
    (throw (ex-info "Invalid datahike configuration." config))))
;; deprecation end
