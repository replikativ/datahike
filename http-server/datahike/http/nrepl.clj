(ns datahike.http.nrepl
  "Lifecycle wrapper for the standalone server's opt-in nREPL endpoint."
  (:require [datahike.http.repl :as repl]
            [nrepl.server :as nrepl]
            [replikativ.logging :as log])
  (:import [java.nio.file Files Path]))

(def disabled-status {:enabled false})

(defn status-atom [] (atom disabled-status))

(defn- handler [context]
  (let [delegate (nrepl/default-handler)]
    (fn [message]
      (binding [repl/*context* context]
        (delegate message)))))

(defn start!
  "Start configured nREPL and update `status`. Returns an owned resource map,
   or nil when nREPL is disabled. Configuration must already be validated."
  [configured-config public-config connections status]
  (when-let [{:keys [port bind socket]} (:nrepl public-config)]
    (let [context {:configured-config configured-config
                   :config public-config
                   :connections connections}
          options (cond-> [:handler (handler context)]
                    socket (into [:socket socket])
                    (some? port) (into [:port port :bind bind]))
          server (apply nrepl/start-server options)
          endpoint (if socket
                     {:enabled true :transport :unix :socket socket}
                     {:enabled true :transport :tcp :bind bind :port (:port server)})]
      (reset! status endpoint)
      (log/info :datahike/nrepl-started endpoint)
      {:server server :socket socket :status status})))

(defn stop! [{:keys [server socket status]}]
  (when server
    (try
      (nrepl/stop-server server)
      (finally
        ;; nREPL marks Unix sockets for deletion at JVM exit, but a server
        ;; restart in the same process must not leave a stale socket behind.
        (when socket
          (Files/deleteIfExists (Path/of socket (make-array String 0))))
        (when status (reset! status disabled-status)))))
  nil)
