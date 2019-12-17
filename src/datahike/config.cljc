(ns datahike.config
  (:require [clojure.spec.alpha :as s])
  (:import [java.net URI]))


;; User configuration

(s/def ::username string?)
(s/def ::password string?)
(s/def ::path string?)
(s/def ::backend keyword?)
(s/def ::host string?)
(s/def ::port int?)
(s/def ::uri string?)

(s/def ::schema-on-read boolean?)
(s/def ::temporal-index boolean?)
(s/def ::index-type keyword?)

(s/def :datahike/config (s/keys :opt-un [::backend ::host ::port ::path ::username ::password ::schema-on-read ::temporal-index ::index-type]))


(defn validate-config-attribute [attribute value config]
  (when-not (s/valid? attribute value)
    (throw (ex-info (str "Bad value " value " at " (name attribute) ", value does not match configuration definition. Must be conform to: " (s/describe attribute) ) config))))

(defn validate-config [config]
  (when-not (s/valid? :datahike/config config)
    (throw (ex-info "Invalid datahike configuration." config))))


;; Internal configuration

(defrecord Configuration [backend username password host path port temporal-index schema-on-read index-type])


(defn complete-config
  ([] (complete-config {}))
  ([config]
   (validate-config config)
   (Configuration. (or (:backend config) :mem)
                   (:username config)
                   (:password config)
                   (:host config)
                   (:path config)
                   (:port config)
                   (if (nil? (:temporal-index config)) true (:temporal-index config))
                   (if (nil? (:schema-on-read config)) false (:schema-on-read config))
                   (or (:index-type config) :datahike.index/hitchhiker-tree)
                   )))


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
    (complete-config config)))