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
        (doseq [end-point ["transact" "transact!-writer" "connect"]
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
        (routes/release-all! connections)
        (is (empty? (filter (comp :conn val) @connections))
            "release-all! released the server's own leases, in the server's own atom")))))

(deftest prefix-and-gate-without-a-server
  (let [req (fn [uri] {:request-method :post :uri uri :headers {}})]
    (testing "a prefix is normalized: leading and trailing slashes do not matter"
      (doseq [prefix ["/datahike" "datahike" "/datahike/" "datahike/"]]
        (let [h (routes/handler {:token token} {:prefix prefix})]
          (is (= 401 (:status (h (req "/datahike/transact")))) (pr-str prefix))
          (is (= 404 (:status (h (req "/transact")))) (pr-str prefix)))))
    (testing "no prefix, and an empty or root prefix, mount at /"
      (doseq [prefix [nil "" "/"]]
        (is (= 401 (:status ((routes/handler {:token token} {:prefix prefix}) (req "/transact")))) (pr-str prefix))))
    (testing "a validator that reads the body neither defeats the cap nor starves the route"
      (let [seen (atom nil)
            h    (routes/handler {:max-body-bytes 4
                                  :validator (fn [req] (reset! seen (count (.readAllBytes ^java.io.InputStream (:body req)))) {:sub "v"})})
            post (fn [s] (h {:request-method :post :uri "/transact" :headers {"content-type" "application/edn"}
                             :body (java.io.ByteArrayInputStream. (.getBytes ^String s))}))]
        (is (= 413 (:status (post "0123456789"))))
        (is (nil? @seen) "over the cap, no validator ran")
        (is (not= 401 (:status (post "[1]"))))
        (is (= 3 @seen) "the validator saw the whole (buffered) body")))
    (testing "databases are found nested in query inputs and inside entities, not in tx-data"
      (let [cfg  {:store {:backend :memory :id (random-uuid)} :schema-flexibility :read}
            conn (do (d/create-database cfg) (d/connect cfg))
            db   @conn
            id   {:store-id (get-in cfg [:store :id]) :branch :db}]
        (try
          (is (= [id] (routes/databases [[db]])))
          (is (= [id] (routes/databases [{:query '[:find ?e :in $ :where [?e]] :args [db]}])))
          (is (= [id] (routes/databases [(d/history db)])))
          (is (= [id] (routes/databases [(d/entity db 1)])))
          (is (= [id] (routes/databases [conn [{:db db}]])))
          (is (= [id] (routes/databases [{:conn conn :tx-data [{:db db}]}])))
          (let [cfg2  {:store {:backend :memory :id (random-uuid)} :schema-flexibility :read}
                conn2 (do (d/create-database cfg2) (d/connect cfg2))
                id2   {:store-id (get-in cfg2 [:store :id]) :branch :db}]
            (try
              (is (= #{id id2} (set (routes/databases [conn [[:db.fn/call :installed-fn conn2]]])))
                  "a handle for another database inside transaction data is part of the call")
              (finally (d/release conn2) (d/delete-database cfg2))))
          (finally (d/release conn) (d/delete-database cfg)))))
    (testing "HEAD is not a way around the gate: routes have no HEAD, so reitit answers 405"
      (is (= 405 (:status ((routes/handler {:token token}) {:request-method :head :uri "/q" :headers {}})))))
    (testing "a handler without a token and without :dev-mode admits nobody"
      (is (= 401 (:status ((routes/handler {}) (req "/transact"))))))))

(deftest the-server-app-shares-the-contract
  (let [port   23201
        conns  (atom {})
        server (run-jetty (server/app {:token token :dev-mode false} conns) {:port port :join? false})
        url    (str "http://localhost:" port)]
    (try
      (is (= 200 (:status (http/get (str url "/swagger.json") {:throw false}))) "swagger.json is public")
      (is (contains? #{200 302} (:status (http/get (str url "/") {:throw false :follow-redirects :never})))
          "the swagger UI at / is served outside the router, untouched by the gate")
      (is (= 401 (:status (http/post (str url "/transact") {:throw false :body "garbage"}))))
      (is (= 200 (:status (http/request {:method :options :uri (str url "/q") :throw false
                                         :headers {"origin" "http://localhost:8080"
                                                   "access-control-request-method" "GET"}})))
          "a CORS preflight carries no token and must reach the CORS middleware, not the gate")
      (is (= 405 (:status (http/post (str url "/q") {:throw false
                                                     :headers {"authorization" (str "token " token)}
                                                     :body "garbage"})))
          "a wrong method is reitit's 405, whoever asks")
      (finally
        (.stop server)))))

(defn- error-of [f]
  (try (f) nil (catch Exception e e)))

(deftest the-gate-caps-and-redacts
  (let [port   23203
        conns  (atom {})
        server (run-jetty (server/app {:token token :dev-mode false :max-body-bytes 4096} conns)
                          {:port port :join? false})
        url    (str "http://localhost:" port)
        big    (byte-array 10000 (byte 32))
        auth   {"authorization" (str "token " token)}]
    (try
      (testing "a public route's body is capped too"
        (is (= 413 (:status (http/request {:method :get :uri (str url "/swagger.json") :throw false :body big})))))
      (testing "a body the decoder would read only the first form of is measured in full"
        (is (= 413 (:status (http/post (str url "/transact")
                                       {:throw   false
                                        :headers (assoc auth "content-type" "application/edn")
                                        :body    (java.io.ByteArrayInputStream.
                                                  (.getBytes (str "[1]" (apply str (repeat 10000 " ")))))})))))
      (testing "an unsupported content type over the limit is 413, not 400"
        (is (= 413 (:status (http/post (str url "/transact")
                                       {:throw false :headers (assoc auth "content-type" "text/plain") :body big})))))
      (testing "the gate's own responses carry CORS headers"
        (let [r (http/post (str url "/transact") {:throw false :headers {"origin" "http://localhost"} :body "x"})]
          (is (= 401 (:status r)))
          (is (= "http://localhost" (get-in r [:headers "access-control-allow-origin"])))))
      (testing "an error body never carries a credential"
        (let [peer {:backend :datahike-server :url url :token token}
              e    (error-of #(client/connect {:store {:backend :file :path "/nonexistent/dh-routes-test"
                                                       :id (random-uuid) :password "s3cret-pw"}
                                               :remote-peer peer}))]
          (is (some? e))
          (is (not (str/includes? (str (ex-message e) (pr-str (ex-data e))) "s3cret-pw")))))
      (finally
        (.stop server)
        (routes/release-all! conns)))))

(deftest leases-and-concurrency
  (let [port        23204
        connections (atom {})
        h           (routes/handler {:token token} {:connections connections})
        server      (run-jetty h {:port port :join? false})
        url         (str "http://localhost:" port)
        peer        {:backend :datahike-server :url url :token token}
        store-id    (random-uuid)
        cfg         (assoc (file-config store-id)
                           :writer {:backend :datahike-server :url url :token token})
        conn        (do (d/create-database cfg) (d/connect cfg))]
    (try
      (testing "concurrent first transactions: every one succeeds, one connection, one base lease"
        (let [go (promise)
              fs (doall (repeatedly 16 #(future @go (d/transact conn [{:name (str (random-uuid))}]))))]
          (deliver go true)
          (is (every? some? (map (comp :db-after deref) fs)))
          (is (= 16 (count (d/q '[:find ?n :where [?e :name ?n]] @conn))))
          (is (= 1 (get-in @connections [[store-id :db] :count])))))
      (testing "decoding a database handle takes no lease"
        (let [api-conn (client/connect (assoc (dissoc cfg :writer) :remote-peer peer))
              before   (get-in @connections [[store-id :db] :count])]
          (dotimes [_ 5] (client/q '[:find ?n :where [?e :name ?n]] @api-conn))
          (is (= before (get-in @connections [[store-id :db] :count])))
          (client/release api-conn)))
      (testing "two releases racing on one granted lease give back one"
        (let [api-conn (client/connect (assoc (dissoc cfg :writer) :remote-peer peer))]
          (dotimes [_ 50]
            (client/connect (assoc (dissoc cfg :writer) :remote-peer peer))
            (let [fs (doall (repeatedly 2 #(future (client/release api-conn))))]
              (run! deref fs))
            (is (= 1 (get-in @connections [[store-id :db] :count]))))
          (client/release api-conn)))
      (testing "deleting a database whose config names this server as its writer does not deadlock"
        (let [cfg2 (assoc (file-config (random-uuid))
                          :writer {:backend :datahike-server :url url :token token}
                          :remote-peer peer)]
          (client/create-database cfg2)
          (is (= true (client/database-exists? cfg2)))
          (is (nil? (deref (future (client/delete-database cfg2)) 20000 ::timeout)))
          (is (false? (client/database-exists? cfg2)))))
      (testing "a rejected writer operation leaves the writer usable"
        (is (instance? Throwable (try @(d/load-entities conn [[1 :name "x" 1 true]]) (catch Exception e e))))
        (is (some? (:db-after (d/transact conn [{:name "after"}])))))
      (testing "a client's releases never close the shared connection"
        (let [api-conn (client/connect (assoc (dissoc cfg :writer) :remote-peer peer))]
          (client/release api-conn true)
          (is (some? (get-in @connections [[store-id :db] :conn]))
              "release-all? from a client is ignored: the base lease stays")
          (dotimes [_ 5] (client/release api-conn))
          (is (= 1 (get-in @connections [[store-id :db] :count]))
              "releasing more often than connecting gives back nothing the caller was not granted")))
      (testing "release-all! empties the registry it is given, handler or atom"
        (routes/release-all! h)
        (is (empty? (filter (comp :conn val) @connections))))
      (finally
        ;; Deletion goes through the remote writer, so before the server stops.
        (d/release conn)
        (d/delete-database cfg)
        (.stop server)))))

(deftest validator-and-authorize
  (let [port      23205
        validator (fn [request]
                    (case (some->> (get-in request [:headers "authorization"]) (re-find #"token (.+)") second)
                      "alice-token" {:sub "alice"}
                      "bob-token"   {:sub "bob"}
                      nil))
        h         (routes/handler {:validator validator
                                   :authorize (fn [{:keys [op principal]}]
                                                (or (= "alice" (:sub principal)) (= :read op)))})
        server    (run-jetty h {:port port :join? false})
        url       (str "http://localhost:" port)
        peer      (fn [t] {:backend :datahike-server :url url :token t})
        cfg       {:store {:backend :memory :id (random-uuid)} :schema-flexibility :read}]
    (try
      (let [alice (client/connect (client/create-database (assoc cfg :remote-peer (peer "alice-token"))))]
        (client/transact alice [{:name "Ada"}])
        (testing "bob may read but not write"
          (let [bob (client/connect (assoc cfg :remote-peer (peer "bob-token")))
                e   (error-of #(client/transact bob [{:name "x"}]))]
            (is (= #{["Ada"]} (client/q '[:find ?n :where [?e :name ?n]] @bob)))
            (is (= :datahike.http/forbidden (:type (ex-data e))))
            (is (= :transact (:op (ex-data e))))))
        (testing "a validator principal may not claim the token's subject"
          (let [h2 (routes/handler {:token token :validator (fn [_] {:sub "root"})})]
            (is (= 401 (:status (h2 {:request-method :post :uri "/transact" :headers {"authorization" "token whatever"}}))))
            (is (not= 401 (:status (h2 {:request-method :post :uri "/transact"
                                        :headers {"authorization" (str "token " token)}
                                        :body (java.io.ByteArrayInputStream. (byte-array 0))})))
                "the token itself still authenticates (and then fails coercion further in)")))
        (testing "a token no validator accepts is 401"
          (is (= 401 (:status (http/post (str url "/transact")
                                         {:throw false :headers {"authorization" "token nobody"} :body "x"}))))))
      (finally
        (routes/release-all! h)
        (.stop server)))))
