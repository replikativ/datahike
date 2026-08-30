(ns datahike.http.repl
  "Convenience functions available inside the standalone server's nREPL.

   nREPL remains an arbitrary-code interface; these functions make common
   read-only inspection explicit without pretending to be a security boundary."
  (:require [datahike.http.system :as system]
            [datahike.metrics :as metrics]))

(def ^:dynamic *context* nil)

(defn- context []
  (or *context*
      (throw (ex-info "Not running inside a Datahike server nREPL"
                      {:type :datahike.http/no-repl-context}))))

(defn config
  "The server's redacted startup configuration."
  []
  (:config (context)))

(defn catalog
  "All entries in the server system catalog, or an empty vector without one."
  []
  (if-let [conn (get (:configured-config (context)) system/conn-key)]
    (system/entries conn)
    []))

(defn runtime
  "Cheap process-local query, writer, and loaded-connection metrics."
  []
  (metrics/runtime-snapshot (:connections (context))))

(defn loaded-connections
  "Summaries of connections currently leased by HTTP clients."
  []
  (mapv (fn [[[store-id branch] {:keys [count]}]]
          {:store-id (str store-id)
           :branch (or branch :db)
           :leases (or count 1)})
        @(:connections (context))))
