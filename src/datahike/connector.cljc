(ns ^:no-doc datahike.connector
  (:require [datahike.core :as d]
            [datahike.store :as ds]
            [datahike.storing :as dsi]
            [datahike.config :as dc]
            [datahike.tools :as dt :refer [throwable-promise]]
            [datahike.transactor :as t]
            [konserve.core :as k]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]
            [clojure.data :refer [diff]]
            [clojure.core.async :refer [go <!]])
  (:import [clojure.lang IDeref IAtom IMeta ILookup]))

;; connection

(defprotocol PConnector
  (-connect [config]))

(declare deref-conn)

(deftype Connection [wrapped-atom]
  IDeref
  (deref [conn] (deref-conn conn))
  ;; These interfaces should not be used from the outside, they are here to keep
  ;; the internal interfaces lean and working.
  ILookup
  (valAt [c k] (if (= k :wrapped-atom) wrapped-atom nil))
  IAtom
  (swap [_ f] (swap! wrapped-atom f))
  (swap [_ f arg] (swap! wrapped-atom f arg))
  (swap [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
  (swap [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
  (compareAndSet [_ oldv newv] (compare-and-set! wrapped-atom oldv newv))
  (reset [_ newval] (reset! wrapped-atom newval))

  IMeta
  (meta [_] (meta wrapped-atom)))

(defn deref-conn [^Connection conn]
  (let [wrapped-atom (.-wrapped-atom conn)]
    (if (not (t/streaming? (get @wrapped-atom :transactor)))
      (let [store  (:store @wrapped-atom)
            stored (k/get store (:branch (:config @wrapped-atom)) nil {:sync? true})]
        (log/trace "Fetched db for deref: " (:config stored))
        (dsi/stored->db stored store))
      @wrapped-atom)))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (Connection. (atom db :meta {:listeners (atom {})})))

(s/def ::connection #(and (instance? Connection %)
                          (not= @(:wrapped-atom %) :deleted)))

(def connections (atom {}))

(defn get-connection [conn-id]
  (when-let [conn (get-in @connections [conn-id :conn])]
    (swap! connections update-in [conn-id :count] inc)
    conn))

(defn add-connection! [conn-id conn]
  (swap! connections assoc conn-id {:conn conn :count 1}))

(defn delete-connection! [conn-id]
  (reset! (get-connection conn-id) :deleted)
  (swap! connections dissoc conn-id))

(defn ensure-stored-config-consistency [config stored-config]
  (when-not (= config (dissoc stored-config :initial-tx :name :index-config))
    (dt/raise "Configuration does not match stored configuration."
              {:type          :config-does-not-match-stored-db
               :config        config
               :stored-config stored-config
               :diff          (diff config stored-config)})))

(extend-protocol PConnector
  String
  (-connect [uri]
    (-connect (dc/uri->config uri)))

  clojure.lang.IPersistentMap
  (-connect [raw-config]
    (let [config (dissoc (dc/load-config raw-config) :initial-tx)
          _ (log/debug "Using config " (update-in config [:store] dissoc :password))
          store-config (:store config)
          store-id (ds/store-identity store-config)
          conn-id (conj store-id (:branch config))]
      (if-let [conn (get-connection conn-id)]
        (let [conn-config (:config @(:wrapped-atom conn))]
          (when-not (= config conn-config)
            (dt/raise "Configuration does not match existing connections."
                      {:type :config-does-not-match-existing-connections
                       :config config
                       :existing-connections-config conn-config
                       :diff (diff config conn-config)}))
          conn)
        (let [raw-store (ds/connect-store store-config)
              _         (when-not raw-store
                          (dt/raise "Backend does not exist." {:type   :backend-does-not-exist
                                                               :config store-config}))
              store     (ds/add-cache-and-handlers raw-store config)
              stored-db (k/get store (:branch config) nil {:sync? true})
              _         (when-not stored-db
                          (ds/release-store store-config store)
                          (dt/raise "Database does not exist." {:type   :db-does-not-exist
                                                                :config config}))
              [config store stored-db]
              (let [intended-index (:index config)
                    stored-index   (get-in stored-db [:config :index])]
                (if-not (= intended-index stored-index)
                  (do
                    (log/warn (str "Stored index does not match configuration. Please set :index explicitly to " stored-index " in config. The default index is now :datahike/persistent-set. Using stored index setting now, but this might throw an error in the future."))
                    (let [config    (assoc config :index stored-index)
                          store     (ds/add-cache-and-handlers raw-store config)
                          stored-db (k/get store (:branch config) nil {:sync? true})]
                      [config store stored-db]))
                  [config store stored-db]))
              _ (ensure-stored-config-consistency config (:config stored-db))
              conn      (conn-from-db (dsi/stored->db (assoc stored-db :config config) store))]
          (swap! (:wrapped-atom conn) assoc :transactor
                 (t/create-transactor (:transactor config) conn))
          (add-connection! conn-id conn)
          conn)))))

;; public API

(defn connect
  ([]
   (-connect {}))
  ([config]
   (-connect config)))

(defn release [connection]
  (let [db      @(:wrapped-atom connection)
        conn-id (conj (ds/store-identity (get-in db [:config :store]))
                      (get-in db [:config :branch]))]
    (if-not (get @connections conn-id)
      (log/info "Connection already released." conn-id)
      (let [new-conns (swap! connections update-in [conn-id :count] dec)]
        (when (zero? (get-in new-conns [conn-id :count]))
          (delete-connection! conn-id)
          (t/shutdown (:transactor db))
          nil)))))

(defn transact!
  [connection {:keys [tx-data tx-meta]}]
  {:pre [(d/conn? connection)]}
  (let [p (throwable-promise)
        transactor (:transactor @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (t/dispatch! transactor
                                       {:tx-fn 'datahike.core/transact
                                        :tx-data tx-data
                                        :tx-meta tx-meta}))]
        (deliver p tx-report)))
    p))

(defn transact [connection arg-map]
  (let [arg (cond
              (and (map? arg-map) (contains? arg-map :tx-data)) arg-map
              (vector? arg-map) {:tx-data arg-map}
              (seq? arg-map) {:tx-data arg-map}
              :else (dt/raise "Bad argument to transact, expected map with :tx-data as key.
                               Vector and sequence are allowed as argument but deprecated."
                              {:error :transact/syntax :argument arg-map}))
        _ (log/debug "Transacting" (count (:tx-data arg)) " objects with arguments: " (dissoc arg :tx-data))
        _ (log/trace "Transaction data" (:tx-data arg))]
    (try
      (deref (transact! connection arg))
      (catch Exception e
        (log/errorf "Error during transaction %s" (.getMessage e))
        (throw (.getCause e))))))

(defn load-entities [connection entities]
  (let [p (throwable-promise)
        transactor (:transactor @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (t/dispatch! transactor {:tx-fn 'datahike.core/load-entities
                                                   :tx-data entities
                                                   :tx-x-meta nil}))]
        (deliver p tx-report)))
    p))

