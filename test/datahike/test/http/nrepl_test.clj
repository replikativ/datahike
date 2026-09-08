(ns datahike.test.http.nrepl-test
  (:require [babashka.http-client :as http]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.http.nrepl :as server-nrepl]
            [datahike.http.server :as server]
            [datahike.test.scratch :as scratch]
            [nrepl.core :as nrepl])
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
  (let [instance (server/start-server {:host "127.0.0.1"
                                       :port 0
                                       :join? false
                                       :metrics false
                                       :token "test-token"
                                       :nrepl {:port 0}})
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
      (with-open [connection (nrepl/connect :host (:bind endpoint) :port (:port endpoint))]
        (is (= ["11"] (eval-values connection "(+ 5 6)"))))
      (finally
        (server/stop-server instance)))
    (is (thrown? java.net.ConnectException
                 (with-open [connection (nrepl/connect :host (:bind endpoint)
                                                       :port (:port endpoint))]
                   ;; connect constructs a lazy transport; sending proves the
                   ;; server socket was actually closed by stop-server.
                   (eval-values connection "(+ 1 1)"))))))
