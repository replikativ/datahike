(ns datahike.config
  (:require [clojure.spec.alpha :as s])
  (:import [java.net URI]))

(defn parse-uri [uri]
  (let [base-uri (URI. uri)
        scheme (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        store-scheme (.getScheme sub-uri)
        path (if (= store-scheme "pg")
               (.getSchemeSpecificPart sub-uri)
               (.getPath sub-uri))]
    [scheme store-scheme path]))

(s/def ::username string?)
(s/def ::password string?)
(s/def ::path string?)
(s/def ::backend #{:mem :file :pg :level})
(s/def ::host string?)
(s/def ::port int?)
(s/def ::uri string?)

(s/def ::schema-on-read boolean?)
(s/def ::temporal-index boolean?)

(s/def :datahike/config (s/keys :req-un [::backend]
                                :opt-un [::host ::port ::path ::username ::password]))

(defn validate-config-attribute [attribute value config]
  (when-not (s/valid? attribute value)
    (throw (ex-info (str "Bad value " value " at " (name attribute) ", value does not match configuration definition. Must be conform to: " (s/describe attribute) ) config))))


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

(defn validate-config [config]
  (when-not (s/valid? :datahike/config config)
    (throw (ex-info "Invalid datahike configuration." config))))

