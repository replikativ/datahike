(ns datahike.test.http.kabel-test
  "The Kabel listener's authorization: remote calls and sync subscriptions ask
   the same `:authorize` policy the HTTP routes do."
  (:require
   [babashka.http-client :as http]
   [clojure.core.async :refer [<!! timeout alts!! chan put!]]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.http.kabel :as kabel]
   [datahike.http.client :as http-client]
   [datahike.http.permissions :as permissions]
   [datahike.http.server :as http-server]
   [datahike.api :as d]
   [datahike.kabel.handlers :as handlers]
   [datahike.kabel.tx-broadcast :as tx-broadcast]
   [datahike.json :as json]
   [datahike.migrate.fs :as fs]
   [datahike.kabel.cbor-handlers :as cbor]
   [kabel.pubsub :as pubsub]
   [kabel.auth.jwt :as jwt]
   [kabel.auth.websocket :as auth]
   [kabel.peer :as peer]
   [kabel.remote :as remote]
   [konserve-sync.core :as sync]
   [jsonista.core :as j]
   [superv.async :refer [S <??]])
  (:import [java.io BufferedReader InputStreamReader]))

(def ^:private secret "kabel-listener-test-secret")
(def ^:private store-a #uuid "aaaaaaaa-1111-1111-1111-111111111111")
(def ^:private store-b #uuid "bbbbbbbb-2222-2222-2222-222222222222")

(defn- policy
  "alice may transact and read store-a, and call the host's own `app/ping`;
   nobody else anything."
  [{:keys [op principal db fn-name]}]
  (and (= "alice" (:sub principal))
       (if (= op :invoke)
         (= 'app/ping fn-name)
         (and (= (str store-a) (str (:store-id db)))
              (contains? #{:transact :read} op)))))

(defn- decided
  "The gate as a plain predicate: since kabel 0.3.134 the listener's gates
   decide on a thread and answer with a channel."
  [gate]
  (fn [ctx] (<!! (gate ctx))))

(deftest gates-map-remote-calls-and-topics-onto-the-policy
  (let [config {:authorize policy}
        remote-gate (decided (kabel/authorize-remote config))
        sync-gate (decided (kabel/authorize-sync config))
        alice {:sub "alice"} bob {:sub "bob"}]
    (testing "dispatch is a transact on the store the call names"
      (is (remote-gate {:principal alice :fn-name 'datahike.kabel/dispatch
                        :arg-map {:store-id store-a :arg-map {:op 'transact}}}))
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id store-b :arg-map {:op 'transact}}})))
      (is (not (remote-gate {:principal bob :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id store-a :arg-map {:op 'transact}}}))))
    (testing "gc is administration, creation and deletion their own ops"
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id store-a :arg-map {:op 'gc-storage!}}})))
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/create-database
                             :arg-map {:config {:store {:id store-a}}}})))
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/delete-database
                             :arg-map {:config {:store {:id store-a}}}}))))
    (testing "the string spelling of a Datahike function is gated as that function, and no other name under it passes"
      (is (not (remote-gate {:principal alice :fn-name "datahike.kabel/dispatch"
                             :arg-map {:store-id store-b :arg-map {:op 'transact}}})))
      (is (remote-gate {:principal alice :fn-name "datahike.kabel/dispatch"
                        :arg-map {:store-id store-a :arg-map {:op 'transact}}}))
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/anything :arg-map {}})))
      (is (not (remote-gate {:principal alice :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id "not-a-uuid" :arg-map {:op 'transact}}}))
          "a store id that is not a UUID reaches no database"))
    (testing "a broadcast topic is a read of the store behind it"
      (is (sync-gate {:op :subscribe :principal alice
                      :topic (keyword "tx-report" (str "scope-" store-a))}))
      (is (not (sync-gate {:op :subscribe :principal alice
                           :topic (keyword "tx-report" (str "scope-" store-b))})))
      (is (not (sync-gate {:op :subscribe :principal alice :topic :tx-report/scope-junk}))))
    (testing "a function the host registered is asked for as an :invoke; a missing principal is refused"
      (is (remote-gate {:principal alice :fn-name 'app/ping :arg-map {}}))
      (is (not (remote-gate {:principal alice :fn-name 'other/fn :arg-map {}})))
      (is (not (remote-gate {:principal bob :fn-name 'app/ping :arg-map {}})))
      (is (not (remote-gate {:principal nil :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id store-a}}))))
    (testing "subscribing to a store's topic reads it; publishing into the server never passes"
      (is (sync-gate {:op :subscribe :principal alice :topic store-a}))
      (is (not (sync-gate {:op :subscribe :principal alice :topic store-b})))
      (is (not (sync-gate {:op :subscribe :principal bob :topic store-a})))
      (is (not (sync-gate {:op :publish :principal alice :topic store-a}))))
    (testing "without a policy every authenticated principal passes, nobody anonymous"
      (let [open-gate (decided (kabel/authorize-remote {}))]
        (is (open-gate {:principal bob :fn-name 'datahike.kabel/dispatch :arg-map {:store-id store-b}}))
        (is (not (open-gate {:principal nil :fn-name 'datahike.kabel/dispatch :arg-map {:store-id store-b}})))))))

(defn- free-port []
  (with-open [socket (java.net.ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- token-for [sub]
  (jwt/sign-hs256 secret {:sub sub :exp (+ (quot (System/currentTimeMillis) 1000) 300)}))

(defn- client [id token]
  (peer/client-peer S id
                    (comp remote/middleware
                          (auth/auth-middleware {:authenticate {:token token} :permissive true}))
                    cbor/datahike-cbor-middleware))

(defn- refusal
  "The `:type` of the error `invoke` yields, within two seconds."
  [ch]
  (let [[v _] (alts!! [ch (timeout 2000)])]
    (if (instance? Throwable v) (:type (ex-data v)) v)))

(defn- local-port [^org.eclipse.jetty.server.Server instance]
  (.getLocalPort ^org.eclipse.jetty.server.ServerConnector
   (first (.getConnectors instance))))

(defn- await-subscribed!
  "Wait until the server has registered this peer's subscription to `topic`.

   `subscribe-tx-reports!` settles when the request has gone out, not when the
   far end serves it, so a write issued immediately after could be published
   to nobody and the test would wait for an event that was never sent."
  [peer topic]
  (is (loop [attempts 200]
        (cond
          (:handshake-complete? (pubsub/subscription peer topic)) true
          (zero? attempts) false
          :else (do (Thread/sleep 25) (recur (dec attempts)))))
      (str "subscription to " topic " became ready")))

(defn- next-report
  ([events] (next-report events 5000))
  ([events timeout-ms]
   (let [[value port] (alts!! [events (timeout timeout-ms)])]
     (when (= port events) value))))

(defn- open-stream [url store-id]
  (let [response (http/get (str url "/listen?store=" store-id)
                           {:headers {"authorization" "token http-kabel-token"}
                            :as :stream})]
    (is (= 200 (:status response))
        "a Kabel-reopened store can be listened to before an HTTP connect")
    (assoc response :reader
           (BufferedReader. (InputStreamReader. (:body response) "UTF-8")))))

(defn- read-stream-event [stream]
  (let [result (future
                 (loop [event nil data nil]
                   (let [line (.readLine ^BufferedReader (:reader stream))]
                     (cond
                       (nil? line) {:event :eof}
                       (str/starts-with? line "event: ")
                       (recur (keyword (subs line 7)) data)
                       (str/starts-with? line "data: ")
                       (recur event (j/read-value (subs line 6) json/mapper))
                       (and (str/blank? line) event) {:event event :data data}
                       :else (recur event data)))))]
    (if (= ::timeout (deref result 5000 ::timeout))
      (do (future-cancel result) ::timeout)
      @result)))

(deftest standalone-broadcasts-http-and-kabel-writes-once
  (let [http-port (free-port)
        kabel-port (free-port)
        server-id (random-uuid)
        client-id (random-uuid)
        store-id (random-uuid)
        root (str (fs/temp-dir! "dh-http-kabel-broadcast-") "/databases")
        store-config {:store {:backend :file
                              :path (str root "/" store-id)
                              :id store-id}
                      :schema-flexibility :read
                      :keep-history? false}
        _ (d/create-database store-config)
        server (http-server/start-server
                {:host "127.0.0.1"
                 :port http-port
                 :join? false
                 :metrics false
                 :token "http-kabel-token"
                 :kabel {:host "127.0.0.1"
                         :port kabel-port
                         :peer-id server-id
                         :jwt {:alg :HS256 :secret secret}
                         :store {:backend :file :path root}}})
        peer (peer/client-peer
              S client-id
              (comp (sync/client-middleware)
                    remote/middleware
                    (auth/auth-middleware
                     {:authenticate {:token (token-for "alice")}
                      :permissive true}))
              cbor/datahike-cbor-middleware)
        reports (chan 8)]
    (try
      (remote/serve peer)
      (<?? S (remote/connect S peer (str "ws://127.0.0.1:" kabel-port)))
      (<?? S (tx-broadcast/subscribe-tx-reports! peer store-id
                                                 #(put! reports %)))
      (await-subscribed! peer (tx-broadcast/tx-report-topic store-id))
      (let [http-peer {:backend :datahike-server
                       :url (str "http://127.0.0.1:" (local-port server))
                       :token "http-kabel-token"
                       :format :cbor}
            stream (open-stream (:url http-peer) store-id)
            _ (is (= :resync (:event (read-stream-event stream))))
            host-conn (handlers/get-connection-for-store store-id)
            host-report (d/transact host-conn [{:source "host-before-http-connect"}])
            host-topic-event (next-report reports)
            host-sse-event (read-stream-event stream)
            _ (is (= (get-in host-report [:db-after :meta :datahike/commit-id])
                     (get-in host-topic-event [:tx-report :db-after :meta :datahike/commit-id])
                     (get-in host-sse-event [:data :commit-id]))
                  "a host write on a Kabel-reopened store reaches both transports before HTTP connects")
            http-conn (http-client/connect (assoc store-config :remote-peer http-peer))]
        (try
          (let [http-result (http-client/transact http-conn [{:source "http"}])
                event (next-report reports)
                sse-event (read-stream-event stream)]
            (is (= (get-in http-result [:db-after :commit-id])
                   (get-in event [:tx-report :db-after :meta :datahike/commit-id])))
            (is (= (get-in http-result [:db-after :commit-id])
                   (get-in sse-event [:data :commit-id]))
                "an HTTP write reaches SSE as well as Kabel")
            (is (nil? (next-report reports 750)) "the HTTP commit is published once"))
          (let [result (<?? S (remote/invoke
                               peer server-id 'datahike.kabel/dispatch
                               {:store-id store-id
                                :branch :db
                                :request-id (random-uuid)
                                :arg-map {:op 'transact!
                                          :args [[{:source "kabel"}]]}}))
                event (next-report reports)
                sse-event (read-stream-event stream)]
            (is (= (get-in result [:db-after :meta :datahike/commit-id])
                   (get-in event [:tx-report :db-after :meta :datahike/commit-id])))
            (is (= (get-in result [:db-after :meta :datahike/commit-id])
                   (get-in sse-event [:data :commit-id]))
                "a Kabel write reaches SSE")
            (is (nil? (next-report reports 750)) "the Kabel commit is published once"))
          (finally
            (try (http-client/release http-conn)
                 (catch Throwable _))))
        (let [host-report (d/transact host-conn [{:source "host-after-http-release"}])
              topic-event (next-report reports)
              sse-event (read-stream-event stream)]
          (is (= (get-in host-report [:db-after :meta :datahike/commit-id])
                 (get-in topic-event [:tx-report :db-after :meta :datahike/commit-id])
                 (get-in sse-event [:data :commit-id]))
              "the registry listener survives the last HTTP release"))
        (let [late-conn (http-client/connect (assoc store-config :remote-peer http-peer))]
          (<?? S (remote/invoke peer server-id 'datahike.kabel/delete-database
                                {:config {:store {:id store-id}}}))
          (<?? S (remote/invoke peer server-id 'datahike.kabel/create-database
                                {:config {:store {:id store-id}
                                          :schema-flexibility :read}}))
          (http-client/release late-conn)
          (let [result (<?? S (remote/invoke
                               peer server-id 'datahike.kabel/dispatch
                               {:store-id store-id
                                :branch :db
                                :request-id (random-uuid)
                                :arg-map {:op 'transact!
                                          :args [[{:source "after-late-release"}]]}}))]
            (is (map? result)
                "a late HTTP release from before Kabel deletion does not release the recreated connection"))
          (http-client/delete-database (assoc store-config :remote-peer http-peer))
          (let [error (next-report
                       (remote/invoke peer server-id 'datahike.kabel/dispatch
                                      {:store-id store-id
                                       :branch :db
                                       :request-id (random-uuid)
                                       :arg-map {:op 'transact!
                                                 :args [[{:source "after-http-delete"}]]}}))]
            (is (instance? Throwable error))
            (is (str/includes? (ex-message error) "not registered")
                "HTTP deletion removes the Kabel dispatch registration")))
        (is (= :deleted (:event (read-stream-event stream))))
        (is (= :eof (:event (read-stream-event stream)))
            "Kabel deletion terminates the SSE stream")
        (.close ^BufferedReader (:reader stream)))
      (finally
        (try (<?? S (tx-broadcast/unsubscribe-tx-reports! peer store-id))
             (catch Throwable _))
        (<?? S (peer/stop peer))
        (http-server/stop-server server)
        (when (d/database-exists? store-config)
          (d/delete-database store-config))))))

(deftest listener-refuses-through-the-policy
  (let [port (free-port)
        config {:host "127.0.0.1"
                :authorize policy
                :kabel {:port port :jwt {:alg :HS256 :secret secret} :store {:backend :memory}}}
        resource (kabel/start! config)
        server-id (:peer-id resource)
        url (str "ws://127.0.0.1:" port)
        dispatch (fn [peer store-id]
                   (remote/invoke peer server-id 'datahike.kabel/dispatch
                                  {:store-id store-id :branch :db :request-id (random-uuid)
                                   :arg-map {:op 'transact :args []}}))]
    (try
      (testing "bob is authenticated but not authorized"
        (let [bob (client (random-uuid) (token-for "bob"))]
          (remote/serve bob)
          (<?? S (remote/connect S bob url))
          (is (= :kabel.remote/not-authorized (refusal (dispatch bob store-a))))
          (<?? S (peer/stop bob))))
      (testing "alice passes the gate and reaches the handler"
        (let [alice (client (random-uuid) (token-for "alice"))]
          (remote/serve alice)
          (<?? S (remote/connect S alice url))
          ;; No database is registered under store-a, so the handler itself
          ;; refuses; that the refusal is the handler's is the point.
          (is (not (contains? #{:kabel.remote/not-authorized :kabel.remote/authentication-required nil}
                              (refusal (dispatch alice store-a)))))
          (<?? S (peer/stop alice))))
      (testing "an unauthenticated peer is asked to authenticate"
        (let [anon (peer/client-peer S (random-uuid) remote/middleware cbor/datahike-cbor-middleware)]
          (remote/serve anon)
          (<?? S (remote/connect S anon url))
          (is (= :kabel.remote/authentication-required (refusal (dispatch anon store-a))))
          (<?? S (peer/stop anon))))
      (finally
        (kabel/stop! resource)))))

(deftest listener-create-database-policy
  (testing "a backend outside the policy is refused with the policy's type"
    (let [port (free-port)
          resource (kabel/start!
                    {:host "127.0.0.1"
                     :create-database {:backends #{:memory}}
                     :kabel {:port port :jwt {:alg :HS256 :secret secret}
                             :store {:backend :file
                                     :path (str (fs/temp-dir! "dh-kabel-refused-") "/listener")}}})
          peer (client (random-uuid) (token-for "alice"))]
      (try
        (remote/serve peer)
        (<?? S (remote/connect S peer (str "ws://127.0.0.1:" port)))
        (is (= :datahike.http/store-refused
               (refusal
                (remote/invoke peer (:peer-id resource) 'datahike.kabel/create-database
                               {:config {:store {:backend :file :id (random-uuid)}}}))))
        (finally
          (<?? S (peer/stop peer))
          (kabel/stop! resource)))))
  (testing "a pinned store root overrides the listener store and keeps the client id"
    (let [port (free-port)
          store-id (random-uuid)
          root (str (fs/temp-dir! "dh-kabel-policy-root-") "/databases")
          db-config {:store {:backend :file :path (str root "/" store-id) :id store-id}}
          resource (kabel/start!
                    {:host "127.0.0.1"
                     :create-database {:store {:backend :file :path root}}
                     :kabel {:port port :jwt {:alg :HS256 :secret secret}
                             :store {:backend :memory}}})
          peer (client (random-uuid) (token-for "alice"))]
      (try
        (remote/serve peer)
        (<?? S (remote/connect S peer (str "ws://127.0.0.1:" port)))
        (<?? S (remote/invoke peer (:peer-id resource) 'datahike.kabel/create-database
                              {:config {:store {:backend :memory :id store-id}}}))
        (is (d/database-exists? db-config))
        (finally
          (<?? S (peer/stop peer))
          (kabel/stop! resource)
          (when (d/database-exists? db-config)
            (d/delete-database db-config)))))))

(deftest host-policy-composes-over-the-permission-graph
  (let [config (permissions/configure
                {:system-db {:store {:backend :memory :id (random-uuid)}}
                 :authorize (fn [{:keys [op fn-name principal default]}]
                              (if (= op :invoke)
                                (and (= 'app/ping fn-name) (= "alice" (:sub principal)))
                                (default)))})
        gate (decided (kabel/authorize-remote config))]
    (try
      (testing "the host rules on its own remote functions"
        (is (gate {:principal {:sub "alice"} :fn-name 'app/ping :arg-map {}}))
        (is (not (gate {:principal {:sub "bob"} :fn-name 'app/ping :arg-map {}})))
        (is (not (gate {:principal {:sub "alice"} :fn-name 'app/other :arg-map {}}))))
      (testing "everything else falls back to the graph: root is admin, alice holds nothing"
        (is (gate {:principal {:sub "root"} :fn-name 'datahike.kabel/dispatch
                   :arg-map {:store-id store-a :arg-map {:op 'transact}}}))
        (is (not (gate {:principal {:sub "alice"} :fn-name 'datahike.kabel/dispatch
                        :arg-map {:store-id store-a :arg-map {:op 'transact}}}))))
      (finally
        (permissions/close! config)))))

(deftest listener-reopens-its-databases-on-start
  (let [root (str (fs/temp-dir! "dh-kabel-reopen-") "/databases")
        store-id (random-uuid)
        port (free-port)]
    (d/create-database {:store {:backend :file :path (str root "/" store-id) :id store-id}
                        :schema-flexibility :write :keep-history? false})
    (let [resource (kabel/start! {:host "127.0.0.1"
                                  :kabel {:port port :jwt {:alg :HS256 :secret secret}
                                          :store {:backend :file :path root}}})]
      (try
        (is (some? (handlers/get-connection-for-store store-id :db))
            "a database from an earlier run is served again")
        (finally
          (kabel/stop! resource))))))
