(ns datahike.test.http.nrepl-test
  (:require [babashka.http-client :as http]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.http.nrepl :as server-nrepl]
            [datahike.http.server :as server]
            [datahike.test.scratch :as scratch]
            [nrepl.core :as nrepl]
            [nrepl.server :as nrepl-server]
            [nrepl.transport :as transport])
  (:import [java.nio.file Files Path]))

(defn- eval-values [connection code]
  (->> (nrepl/message (nrepl/client connection 3000)
                      {:op "eval" :code code})
       (keep :value)
       vec))

(defn- http-port [^org.eclipse.jetty.server.Server instance]
  (.getLocalPort ^org.eclipse.jetty.server.ServerConnector
   (first (.getConnectors instance))))

(deftest tcp-nrepl-evaluates-with-server-context
  (let [connections (atom {})
        status (server-nrepl/status-atom)
        config {:nrepl {:port 0 :bind "127.0.0.1"}}
        resource (server-nrepl/start! config
                                      {:nrepl (:nrepl config) :token "REDACTED"}
                                      connections status)]
    (try
      (is (= :tcp (:transport @status)))
      (is (pos? (:port @status)))
      (with-open [connection (nrepl/connect :host (:bind @status)
                                            :port (:port @status))]
        (is (= ["3"] (eval-values connection "(+ 1 2)")))
        (is (some #(str/includes? % "REDACTED")
                  (eval-values connection "(datahike.http.repl/config)")))
        (is (= ["[]"]
               (eval-values connection "(datahike.http.repl/loaded-connections)"))))
      (finally
        (server-nrepl/stop! resource)))
    (is (= server-nrepl/disabled-status @status))))

(defn- with-unix-socket-path [f]
  ;; Unix sockets limit the pathname passed to bind/connect, even when the
  ;; filesystem accepts a much longer name. Keep this path relative and short
  ;; regardless of checkout depth or the configured java.io.tmpdir. This fixture
  ;; owns its directory and removes it even if starting the server throws.
  (let [root (Files/createDirectories (Path/of ".scratch" (make-array String 0))
                                      (make-array java.nio.file.attribute.FileAttribute 0))
        directory (Files/createTempDirectory root "nrepl-"
                                             (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (f (str (.resolve directory "nrepl.sock")))
      (finally (scratch/delete-tree! (str directory))))))

(deftest unix-socket-is-usable-and-removed-on-stop
  (with-unix-socket-path
    (fn [socket]
      (let [status (server-nrepl/status-atom)
            resource (server-nrepl/start! {:nrepl {:socket socket}}
                                          {:nrepl {:socket socket}}
                                          (atom {}) status)]
        (try
          (testing "the JDK Unix-domain transport accepts normal nREPL clients"
            (is (= {:enabled true :transport :unix :socket socket} @status))
            (is (Files/exists (Path/of socket (make-array String 0))
                              (make-array java.nio.file.LinkOption 0)))
            (with-open [connection (nrepl/connect :socket socket)]
              (is (= ["42"] (eval-values connection "(* 6 7)")))))
          (finally
            (server-nrepl/stop! resource)))
        (is (not (Files/exists (Path/of socket (make-array String 0))
                               (make-array java.nio.file.LinkOption 0))))))))

(deftest unix-socket-directory-is-removed-on-failure
  (let [directory (atom nil)]
    (is (thrown-with-msg?
         Exception #"startup failed"
         (with-unix-socket-path
           (fn [socket]
             (reset! directory (.getParent (Path/of socket (make-array String 0))))
             (spit (str (.resolve ^Path @directory "partial-startup")) "leftover")
             (throw (ex-info "startup failed" {}))))))
    (is (not (Files/exists ^Path @directory (make-array java.nio.file.LinkOption 0))))))

(deftest standalone-server-owns-nrepl-and-reports-its-resolved-endpoint
  (let [resource (atom nil)
        start-nrepl! server-nrepl/start!
        instance (with-redefs [server-nrepl/start!
                               (fn [& args]
                                 (let [started (apply start-nrepl! args)]
                                   (reset! resource started)
                                   started))]
                   (server/start-server {:host "127.0.0.1"
                                         :port 0
                                         :join? false
                                         :metrics false
                                         :token "test-token"
                                         :nrepl {:port 0}}))
        endpoint (try
                   (let [response (http/request
                                   {:method :get
                                    :uri (str "http://127.0.0.1:" (http-port instance)
                                              "/admin/status")
                                    :headers {"authorization" "token test-token"
                                              "accept" "application/edn"}
                                    :as :stream})]
                     (get-in (edn/read-string (slurp (:body response)))
                             [:node :nrepl]))
                   (catch Throwable t
                     (server/stop-server instance)
                     (throw t)))]
    (try
      (is (= :tcp (:transport endpoint)))
      (is (= endpoint @(:status @resource)))
      (is (not (.isClosed ^java.net.ServerSocket (get-in @resource [:server :server-socket]))))
      (with-open [connection (nrepl/connect :host (:bind endpoint) :port (:port endpoint))]
        (is (= ["11"] (eval-values connection "(+ 5 6)"))))
      (finally
        (server/stop-server instance)))
    ;; Verify the resource this HTTP server owned. Probing its freed port races
    ;; native listener closure and port reuse; the delayed-worker tests below
    ;; separately prove that late connections cannot escape shutdown.
    (is (.isClosed ^java.net.ServerSocket (get-in @resource [:server :server-socket])))
    (is (= server-nrepl/disabled-status @(:status @resource)))))

(deftest shutdown-closes-delayed-transports
  (doseq [phase [:before-transport :after-transport]]
    (testing (name phase)
      (let [ready (promise)
            resume (promise)
            finished (promise)
            start-server nrepl-server/start-server]
        (with-redefs [nrepl-server/start-server
                      (fn [& {:as options}]
                        (let [factory (or (:transport-fn options) transport/bencode)
                              delayed (fn [socket]
                                        (let [pause (fn []
                                                      (deliver ready socket)
                                                      (when (= ::timeout (deref resume 10000 ::timeout))
                                                        (throw (ex-info "Timed out waiting for shutdown" {}))))]
                                          (try
                                            (when (= phase :before-transport) (pause))
                                            (let [t (factory socket)]
                                              (when (= phase :after-transport) (pause))
                                              t)
                                            (finally (deliver finished true)))))]
                          (apply start-server (mapcat identity (assoc options :transport-fn delayed)))))]
          (let [status (server-nrepl/status-atom)
                config {:nrepl {:port 0 :bind "127.0.0.1"}}
                resource (server-nrepl/start! config config (atom {}) status)]
            (try
              (with-open [connection (nrepl/connect :host (:bind @status) :port (:port @status))]
                (let [socket (deref ready 10000 ::timeout)]
                  (is (not= ::timeout socket) "the real accept worker reached the selected race window")
                  (when-not (= ::timeout socket)
                    (is (empty? @(get-in resource [:server :open-transports]))
                        "nREPL has not registered this connection yet")
                    (server-nrepl/stop! resource)
                    (when (= phase :after-transport)
                      (is (.isClosed ^java.net.Socket socket)
                          "our registry closes transports missing from nREPL's snapshot"))
                    (deliver resume true)
                    (is (= true (deref finished 10000 ::timeout)))
                    (is (.isClosed ^java.net.Socket socket)
                        "late transport creation must close the accepted socket")
                    (is (thrown? java.io.IOException (eval-values connection "(+ 1 1)")))
                    (is (= server-nrepl/disabled-status @status)))))
              (finally
                (deliver resume true)
                (server-nrepl/stop! resource)))))))))

(deftest shutdown-rejects-queued-handler-dispatch
  (let [handled (atom 0)
        dispatch (atom nil)
        start-server nrepl-server/start-server]
    (with-redefs [nrepl-server/default-handler (fn [] (fn [_] (swap! handled inc)))
                  nrepl-server/start-server (fn [& {:as options}]
                                              (reset! dispatch (:handler options))
                                              (apply start-server (mapcat identity options)))]
      (let [config {:nrepl {:port 0 :bind "127.0.0.1"}}
            resource (server-nrepl/start! config config (atom {}) (server-nrepl/status-atom))]
        (try
          (@dispatch {:op "eval"})
          (is (= 1 @handled))
          (server-nrepl/stop! resource)
          (@dispatch {:op "eval"})
          (is (= 1 @handled) "a handler queued before stop must not dispatch after stop")
          (finally (server-nrepl/stop! resource)))))))
