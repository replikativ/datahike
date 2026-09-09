(ns datahike.http.nrepl
  "Lifecycle wrapper for the standalone server's opt-in nREPL endpoint."
  (:require [datahike.http.repl :as repl]
            [nrepl.server :as nrepl]
            [nrepl.transport :as transport]
            [replikativ.logging :as log])
  (:import [java.nio.file Files Path]))

(def disabled-status {:enabled false})

(defn status-atom [] (atom disabled-status))

(defn- handler [context transports]
  (let [delegate (nrepl/default-handler)]
    (fn [message]
      ;; This check admits the message. Already admitted evaluations are in
      ;; flight; handlers queued until after shutdown must not dispatch. Do not
      ;; hold the registry lock across middleware or socket writes.
      (when (some? @transports)
        (binding [repl/*context* context]
          (delegate message))))))

(def ^:private closed-transport
  (reify
    transport/Transport
    (recv [_] nil)
    (recv [_ _timeout] nil)
    (send [_ _message] (throw (java.net.SocketException. "nREPL server stopped")))
    java.io.Closeable
    (close [_] nil)))

(defn- transport-factory [transports]
  (fn [socket]
    ;; nREPL constructs/registers transports asynchronously after accept. Own
    ;; them before returning to nREPL, so its delayed registration cannot escape
    ;; shutdown's snapshot. nil permanently closes admission.
    (locking transports
      (if (nil? @transports)
        (do (.close ^java.io.Closeable socket) closed-transport)
        (try
          (let [delegate (transport/bencode socket)
                closed? (atom false)
                tracked (reify
                          transport/Transport
                          (recv [_] (transport/recv delegate))
                          (recv [_ timeout] (transport/recv delegate timeout))
                          (send [this message] (transport/send delegate message) this)
                          java.io.Closeable
                          (close [this]
                            (when (compare-and-set! closed? false true)
                              (try
                                (.close ^java.io.Closeable delegate)
                                (finally
                                  (locking transports
                                    (when (some? @transports)
                                      (swap! transports disj this))))))))]
            (swap! transports conj tracked)
            tracked)
          (catch Throwable t
            (.close ^java.io.Closeable socket)
            (throw t)))))))

(defn- close-transports! [transports]
  (when-let [failure (reduce (fn [failure t]
                               (try
                                 (.close ^java.io.Closeable t)
                                 failure
                                 (catch Throwable e
                                   (if failure
                                     (do (.addSuppressed ^Throwable failure e) failure)
                                     e))))
                             nil transports)]
    (throw failure)))

(defn start!
  "Start configured nREPL and update `status`. Returns an owned resource map,
   or nil when nREPL is disabled. Configuration must already be validated."
  [configured-config public-config connections status]
  (when-let [{:keys [port bind socket]} (:nrepl public-config)]
    (let [context {:configured-config configured-config
                   :config public-config
                   :connections connections}
          transports (atom #{})
          options (cond-> [:handler (handler context transports)
                           :transport-fn (transport-factory transports)]
                    socket (into [:socket socket])
                    (some? port) (into [:port port :bind bind]))
          server (apply nrepl/start-server options)
          endpoint (if socket
                     {:enabled true :transport :unix :socket socket}
                     {:enabled true :transport :tcp :bind bind :port (:port server)})]
      (reset! status endpoint)
      (log/info :datahike/nrepl-started endpoint)
      {:server server :socket socket :status status :transports transports})))

(defn stop! [{:keys [server socket status transports]}]
  (when server
    (let [registered (locking transports
                       (let [registered @transports]
                         (reset! transports nil)
                         registered))]
      (try
        (nrepl/stop-server server)
        (finally
          (try
            (close-transports! registered)
            (finally
              ;; nREPL normally defers unlinking Unix sockets until JVM exit.
              (try
                (when socket
                  (Files/deleteIfExists (Path/of socket (make-array String 0))))
                (finally
                  (when status (reset! status disabled-status))))))))))
  nil)
