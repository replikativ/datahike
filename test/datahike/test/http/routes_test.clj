(ns datahike.test.http.routes-test
  "Datahike's HTTP API mounted inside a host application, under a prefix,
   driven by the real clients — the HTTP client and the remote writer — and
   the contract `routes/handler` makes: one registry for the whole request,
   nothing decoded before the token is checked, bodies capped."
  (:require
   [babashka.http-client :as http]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.connections :as connections]
   [datahike.http.client :as client]
   [datahike.http.routes :as routes]
   [datahike.http.server :as server]
   [datahike.migrate.fs :as fs]
   [datahike.remote :as remote]
   [ring.adapter.jetty :refer [run-jetty]]))

(def ^:private token "securerandompassword")

(defn- host-app
  "A host that owns `/` and hands `/datahike/...` to Datahike — the shape of an
   application embedding the API rather than running the server."
  [datahike-handler]
  (fn [request]
    (if (str/starts-with? (:uri request) "/datahike")
      (datahike-handler request)
      {:status 200 :headers {"content-type" "text/plain"} :body "host"})))

(defn- file-config
  "A file store under a fresh temp dir (konserve refuses a path that exists)."
  [store-id]
  {:store {:backend :file :path (str (fs/temp-dir! "dh-routes-") "/store") :id store-id}
   :keep-history? true
   :schema-flexibility :read})

(defn- root-cause-message [^Throwable e]
  (loop [e e]
    (if-let [c (.getCause e)] (recur c) (ex-message e))))

(deftest embedded-under-a-prefix
  (let [port        23200
        connections (atom {})
        server      (run-jetty (host-app (routes/handler {:token token :dev-mode false :max-body-bytes 4096}
                                                         {:prefix "/datahike" :connections connections}))
                               {:port port :join? false})
        url         (str "http://localhost:" port "/datahike")
        peer        {:backend :datahike-server :url url :token token :format :transit}]
    (try
      (testing "the host still owns everything outside the prefix"
        (is (= "host" (:body (http/get (str "http://localhost:" port "/anything"))))))

      (testing "a request inside the prefix that matches nothing is a 404, not the host's page"
        (is (= 404 (:status (http/get (str url "/no-such-route") {:throw false})))))

      (testing "the gate: no or wrong token is 401 before anything is decoded"
        (doseq [end-point ["q" "transact!-writer" "connect"]
                headers   [{} {"authorization" "token wrong"}]]
          (is (= 401 (:status (http/post (str url "/" end-point)
                                         {:throw   false
                                          :headers (assoc headers "content-type" "application/transit+json")
                                          :body    "this is not transit"})))
              (str end-point " with " (or (get headers "authorization") "no token")))))

      (testing "the gate: a body over :max-body-bytes is 413, with or without Content-Length"
        (let [big (.getBytes (apply str (repeat 10000 "x")))
              hdr {"authorization" (str "token " token) "content-type" "application/transit+json"}]
          (is (= 413 (:status (http/post (str url "/transact") {:throw false :headers hdr :body big}))))
          (is (= 413 (:status (http/post (str url "/transact") {:throw   false :headers hdr
                                                                :body    (java.io.ByteArrayInputStream. big)})))
              "chunked, no Content-Length: the stream itself is capped")))

      (testing "the HTTP client works against the prefixed API, in transit and in CBOR"
        (doseq [fmt [:transit :cbor]]
          (let [cfg  (client/create-database {:store {:backend :memory :id (random-uuid)}
                                              :schema-flexibility :read
                                              :remote-peer (assoc peer :format fmt)})
                conn (client/connect cfg)]
            (client/transact conn [{:name "Ada"}])
            (is (= #{["Ada"]} (client/q '[:find ?n :where [?e :name ?n]] @conn)) (name fmt)))))

      (testing "one registry: the host's connection and the API's are the identical object"
        (let [store-id  (random-uuid)
              cfg       (file-config store-id)
              host-conn (binding [connections/*connections* connections]
                          (d/create-database cfg)
                          (d/connect cfg))
              api-conn  (client/connect (assoc cfg :remote-peer peer))]
          (is (identical? host-conn (get-in @connections [[store-id :db] :conn])))
          (client/transact api-conn [{:name "Grace"}])
          (is (= #{["Grace"]} (d/q '[:find ?n :where [?e :name ?n]] @host-conn))
              "what the client wrote through the API, the host reads on its own connection")
          (is (= 1 (count (filter #(= store-id (ffirst %)) @connections))))
          (is (nil? (get @connections/*connections* [store-id :db]))
              "the process-wide registry was never touched")
          (client/release api-conn)
          (binding [connections/*connections* connections]
            (d/release host-conn)
            (d/delete-database cfg))))

      (testing "the remote writer through the prefix: the server holds one lease, requests borrow it"
        (let [store-id (random-uuid)
              cfg      (assoc (file-config store-id)
                              :writer {:backend :datahike-server :url url :token token})
              conn     (do (d/create-database cfg) (d/connect cfg))]
          (dotimes [i 3] (d/transact conn [{:name (str "Grace-" i)}]))
          (is (= 3 (count (d/q '[:find ?n :where [?e :name ?n]] @conn))))
          (is (= 1 (get-in @connections [[store-id :db] :count]))
              "three transactions, one lease — the count is not per request")
          (let [report (client/request-cbor :post "transact!-writer" peer [(dissoc cfg :writer) [{:name "Ada"}]])]
            (is (= peer (remote/remote-peer (:db-after report)))
                "a database handle in a CBOR response carries the peer it came from"))
          (let [e (try @(d/load-entities conn [[1 :name "x" 1 true]]) (catch Exception e e))]
            (is (str/includes? (str (root-cause-message e)) "not available over the :datahike-server writer")
                "an operation the server does not implement fails by name, not with a 404"))
          (d/release conn)
          (d/delete-database cfg)))
      (finally
        (.stop server)
        (routes/release-all! connections)))))

(deftest prefix-and-gate-without-a-server
  (let [req (fn [uri] {:request-method :post :uri uri :headers {}})]
    (testing "a prefix is normalized: leading and trailing slashes do not matter"
      (doseq [prefix ["/datahike" "datahike" "/datahike/" "datahike/"]]
        (let [h (routes/handler {:token token} {:prefix prefix})]
          (is (= 401 (:status (h (req "/datahike/q")))) (pr-str prefix))
          (is (= 404 (:status (h (req "/q")))) (pr-str prefix)))))
    (testing "no prefix, and an empty or root prefix, mount at /"
      (doseq [prefix [nil "" "/"]]
        (is (= 401 (:status ((routes/handler {:token token} {:prefix prefix}) (req "/q")))) (pr-str prefix))))
    (testing "a handler without a token and without :dev-mode admits nobody"
      (is (= 401 (:status ((routes/handler {}) (req "/q"))))))))

(deftest the-server-app-shares-the-contract
  (let [port   23201
        conns  (atom {})
        server (run-jetty (server/app {:token token :dev-mode false} conns) {:port port :join? false})
        url    (str "http://localhost:" port)]
    (try
      (is (= 200 (:status (http/get (str url "/swagger.json") {:throw false}))) "swagger.json is public")
      (is (contains? #{200 302} (:status (http/get (str url "/") {:throw false :follow-redirects :never})))
          "the swagger UI at / is served outside the router, untouched by the gate")
      (is (= 401 (:status (http/post (str url "/q") {:throw false :body "garbage"}))))
      (finally
        (.stop server)))))
