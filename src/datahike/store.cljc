(ns datahike.store
  (:require [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve.memory :as mem]
            [konserve-carmine.core :as rs]
            [superv.async :refer [<?? S]]
            [hasch.core :as h]
            [taoensso.carmine :as car])
  (:import (java.net URI)))

(doseq [s ['empty-store 'delete-store 'connect-store]]
  (ns-unmap (symbol (str  *ns*)) s))


(defmulti empty-store
  "Creates an empty store"
  {:arglists '([config])}
  :backend)

(defmethod empty-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't create a store with scheme: " backend))))

(defmulti delete-store
          "Deletes an existing store"
          {:arglists '([config])}
          :backend)

(defmethod delete-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't delete a store with scheme: " backend))))

(defmulti connect-store
          "Makes a connection to an existing store"
          {:arglists '([config])}
          :backend)

(defmethod connect-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't connect to store with scheme: " backend))))

(defmulti release-store
          "Releases the connection to an existing store (optional)."
          {:arglists '([config store])}
          (fn [{:keys [backend]} store]
            backend))

(defmethod release-store :default [_ _]
  nil)

(defmulti scheme->index
          "Returns the index type to use for this store"
          {:arglists '([config])}
          :backend)

;; memory

(def memory (atom {}))

(defmethod empty-store :mem [{:keys [host]}]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc host store)
    store))

(defmethod delete-store :mem [{:keys [host]}]
  (swap! memory dissoc host))

(defmethod connect-store :mem [{:keys [host]}]
  (@memory host))

(defmethod scheme->index :mem [_]
  :datahike.index/hitchhiker-tree)

;; file

(defmethod empty-store :file [{:keys [path]}]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (fs/new-fs-store path))))

(defmethod delete-store :file [{:keys [path]}]
  (fs/delete-store path))

(defmethod connect-store :file [{:keys [path]}]
  (<?? S (fs/new-fs-store path)))

(defmethod scheme->index :file [_]
  :datahike.index/hitchhiker-tree)

;; redis

(defn ^:private  parse-uri
  "Parse a datahike uri in a carmine options spec.

  Create a carmine connection map from the datahike URI.

  Formats:

  datahike:redis://host
  datahike:redis://host:port
  datahike:redis://doesntmatter:password@host
  datahike:redis://doesntmatter:password@host:port
  datahike:redis://host/db
  datahike:redis://host:port/db
  datahike:redis://doesntmatter:password@host/db
  datahike:redis://doesntmatter:password@host:port/db

  Example formats:
  
  \"datahike:redis://localhost:6379/2\"
  \"datahike:redis://redis-server:snort@localhost:6379/2\"
  \"datahike:redis://redis-server:snort@localhost:6380/3\"
  "
  [uri]
  ;; credit -- adapted from Peter Taoussanis's carmine
  ;; https://github.com/ptaoussanis/carmine/blob/master/src/taoensso/carmine/connections.clj#L197
  (when uri
    (let [uri (clojure.string/replace-first uri #"datahike[:]" "")
          ^URI uri (if (instance? URI uri) uri (URI. uri))
          [user password] (.split (str (.getUserInfo uri)) ":")
          port (.getPort uri)
          db (if-let [[_ db-str] (re-matches #"/(\d+)$" (.getPath uri))]
               (Integer. ^String db-str))]
      (-> {:host (.getHost uri)}
          (#(if (pos? port)        (assoc % :port     port)     %))
          (#(if (and db (pos? db)) (assoc % :db       db)       %))
          (#(if password           (assoc % :password password) %))))))

(comment
  (parse-uri "datahike:redis://localhost:6379/2")
  (parse-uri "datahike:redis://redis-server:snort@localhost:6379/2")
  (parse-uri  "datahike:redis://redis-server:snort@localhost:6380/3"))

(defn ^:private carmine-conn
  "Create a carmine connection map from the datahike URI.

  Formats:

  datahike:redis://host
  datahike:redis://host:port
  datahike:redis://doesntmatter:password@host
  datahike:redis://doesntmatter:password@host:port
  datahike:redis://host/db
  datahike:redis://host:port/db
  datahike:redis://doesntmatter:password@host/db
  datahike:redis://doesntmatter:password@host:port/db

  Example formats:
  
  \"datahike:redis://localhost:6379/2\"
  \"datahike:redis://redis-server:snort@localhost:6379/2\"
  \"datahike:redis://redis-server:snort@localhost:6380/3\""
  [uri] {:pool {} :spec (parse-uri uri)})

(defmethod empty-store :redis [{:keys [uri] :as opts}]
  (kons/add-hitchhiker-tree-handlers
   (<?? S (rs/new-carmine-store (carmine-conn uri)))))

(defmethod delete-store :redis [{:keys [uri] :as opts}]
  ;; why (str (h/uuid :db)) ?
  ;; tl;dr -- the hitchhiker tree is stored at a key based on uuid created from the keyword :db in redis
  ;; see   https://github.com/replikativ/konserve-carmine/blob/master/src/konserve_carmine/core.clj#L21
  ;; then  https://github.com/replikativ/datahike/blob/master/src/datahike/connector.cljc#L101
  (car/wcar (carmine-conn uri) (car/del (str (h/uuid :db)))))

(defmethod connect-store :redis [{:keys [uri] :as opts}]
  (<?? S (rs/new-carmine-store (carmine-conn uri))))

(defmethod scheme->index :redis [_]
  :datahike.index/hitchhiker-tree)

