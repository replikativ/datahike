(ns datahike.test.http.nrepl-test
  (:require [babashka.http-client :as http]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.http.nrepl :as server-nrepl]
            [datahike.http.server :as server]
            [nrepl.core :as nrepl])
  (:import [java.nio.file Files Path]
           [java.net Socket InetSocketAddress]))

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

(deftest unix-socket-is-usable-and-removed-on-stop
  (let [directory (Files/createTempDirectory "datahike-nrepl-test-"
                                             (make-array java.nio.file.attribute.FileAttribute 0))
        socket (str (.resolve directory "nrepl.sock"))
        status (server-nrepl/status-atom)
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
                           (make-array java.nio.file.LinkOption 0))))))

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
                 (with-open [socket (Socket.)]
                   ;; Test socket closure directly. The nREPL client's lazy
                   ;; response sequence may end without surfacing a transport
                   ;; exception; an empty response is not proof of a listener.
                   (.connect socket (InetSocketAddress. ^String (:bind endpoint)
                                                        (int (:port endpoint)))
                             3000))))))
