(ns datahike.http.main
  "Lightweight entry point for the standalone server artifact. Configuration
  and logging are resolved before database backends or the HTTP adapter load."
  (:gen-class)
  (:require [datahike.http.config :as config]
            [datahike.http.logging :as logging]
            [datahike.tools :as tools]
            [replikativ.logging :as log]))

(defn- start! [server-config]
  ;; Loading the implementation registers bundled Konserve backends and loads
  ;; the HTTP adapter. Do it only after the chosen layout owns process logging.
  (require 'datahike.http.server)
  ((requiring-resolve 'datahike.http.server/start-main!) server-config))

(defn -main [& args]
  (try
    (let [{:keys [action message config]} (config/resolve-config args)]
      (case action
        :help (println message)
        :version (println "Datahike HTTP Server" tools/datahike-version)
        :run (do
               (logging/configure! config)
               (start! (config/assert-safe-bind! config)))))
    (catch Exception e
      (log/error :datahike/http-server-start-failed
                 (ex-message e)
                 {:error-class (.getName (class e))})
      (System/exit 1))))
