(ns datahike.config
  (:require [clojure.spec.alpha :as s])
  (:import [java.net URI]))

(s/def ::scheme #{"datahike"})
(s/def ::store-scheme #{"mem" "file" "level" "pg"})
(s/def ::uri-config (s/cat :meta string?
                           :store-scheme ::store-scheme
                           :path string?))

(s/fdef parse-uri
        :args (s/cat :uri string?)
        :ret ::uri-config)

(defn parse-uri [uri]
(let [base-uri (URI. uri)
        scheme (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        store-scheme (.getScheme sub-uri)
        path (if (=  store-scheme "pg")
               (.getSchemeSpecificPart sub-uri)
                 (.getPath sub-uri))]
    [scheme store-scheme path ]))

(s/def :storage/name string?)
(s/def :storage/password string?)
(s/def :storage/path string?)

(s/def ::storage-type #{:mem :file :pg :level})
(s/def ::host string?)
(s/def ::port int?)
(s/def ::storage-config (s/keys :opt-un [::username ::password ::path ::host ::port]))
(s/def ::temporal-index boolean?)
(s/def ::schema-on-read boolean?)
(s/def ::index-type #{:datahike.index/hitchhiker-tree :datahike.index/persistent-set})

(s/def :datahike/config (s/keys :req-un [::storage-type ::storage-config]
                                :opt-un [::temporal-index ::schema-on-read ::index-type]))

(defn uri->config [uri]
  (let [base-uri (URI. uri)
        scheme (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        storage-type (keyword (.getScheme sub-uri))
        path  (if (=  storage-type :pg)
                  (.getSchemeSpecificPart sub-uri)
                  (.getPath sub-uri))
        [username password] (when-let [user-info (.getUserInfo sub-uri)]
                              (clojure.string/split user-info  #":"))
        credentials (when-not (and  (nil? username) (nil? password))
                      {:username username
                       :password password})
        port (.getPort sub-uri)
        params (when-let [query (.getQuery sub-uri)] (->> (clojure.string/split query #"&")
                                                          (map #(clojure.string/split % #"="))
                                                          (into {})
                                                          (reduce-kv (fn [m k v]
                                                                       (assoc m (keyword k) (read-string v))) {})))
        path (.getPath sub-uri)
        host (.getHost sub-uri)]
    (merge  {:storage-type storage-type
             :storage-config (merge  {}
                                     credentials
                                     (when host
                                       {:host host})
                                     (when-not (empty? path)
                                       {:path path})
                                     (when (<= 0 port)
                                       {:port port}))}
            params)))

(comment
  (def uri "datahike:file:///tmp/config-test")

  (uri->config uri)


  )
