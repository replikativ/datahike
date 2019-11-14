(ns datahike.ddb-s3-store
  (:require [datahike.config :refer [uri->config*]]
            [datahike.store :refer [empty-store delete-store connect-store scheme->index release-store]]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve-ddb-s3.core :as ddb-s3]
            [superv.async :as sv]
            [clojure.string :as string])
  (:import [java.net URI]))

(def ^:dynamic *ddb-client* nil)
(def ^:dynamic *s3-client* nil)

(defmethod uri->config* :ddb+s3
  [{:keys [uri] :as config}]
  (let [sub-uri (URI. (.getSchemeSpecificPart (URI. uri)))
        region (.getHost sub-uri)
        [table bucket database & etc] (->> (string/split (.getPath sub-uri) #"/")
                                           (remove string/blank?))]
    (when (or (nil? region) (nil? table) (nil? bucket) (not-empty etc))
      (throw (ex-info "URI does not conform to ddb+s3 scheme" {:uri uri})))
    (assoc config
      :region region
      :table table
      :bucket bucket
      :consistent-key #{:db}
      :database (or database :datahike))))

(defn merge-clients
  [config]
  (merge config
         (when *ddb-client* {:ddb-client *ddb-client*})
         (when *s3-client* {:s3-client *s3-client*})))

(defmethod empty-store :ddb+s3
  [config]
  (kons/add-hitchhiker-tree-handlers
    (sv/<?? sv/S (ddb-s3/empty-store (merge-clients config)))))

(defmethod delete-store :ddb+s3
  [config]
  (sv/<?? sv/S (ddb-s3/delete-store (merge-clients config))))

(defmethod connect-store :ddb+s3
  [config]
  (sv/<?? sv/S (ddb-s3/connect-store (merge-clients config))))

(defmethod release-store :ddb+s3
  [_ store]
  (.close store))

(defmethod scheme->index :ddb+s3
  [_]
  :datahike.index/hitchhiker-tree)