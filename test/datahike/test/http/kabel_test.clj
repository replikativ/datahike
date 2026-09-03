(ns datahike.test.http.kabel-test
  "The Kabel listener's authorization: remote calls and sync subscriptions ask
   the same `:authorize` policy the HTTP routes do."
  (:require
   [clojure.core.async :refer [<!! timeout alts!!]]
   [clojure.test :refer [deftest is testing]]
   [datahike.http.kabel :as kabel]
   [datahike.kabel.cbor-handlers :as cbor]
   [kabel.auth.jwt :as jwt]
   [kabel.auth.websocket :as auth]
   [kabel.peer :as peer]
   [kabel.remote :as remote]
   [superv.async :refer [S <??]]))

(def ^:private secret "kabel-listener-test-secret")
(def ^:private store-a #uuid "aaaaaaaa-1111-1111-1111-111111111111")
(def ^:private store-b #uuid "bbbbbbbb-2222-2222-2222-222222222222")

(defn- policy
  "alice may transact and read store-a; nobody else anything."
  [{:keys [op principal db]}]
  (and (= "alice" (:sub principal))
       (= (str store-a) (str (:store-id db)))
       (contains? #{:transact :read} op)))

(deftest gates-map-remote-calls-and-topics-onto-the-policy
  (let [config {:authorize policy}
        remote-gate (kabel/authorize-remote config)
        sync-gate (kabel/authorize-sync config)
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
    (testing "an unknown remote name and a missing principal are refused"
      (is (not (remote-gate {:principal alice :fn-name 'other/fn :arg-map {}})))
      (is (not (remote-gate {:principal nil :fn-name 'datahike.kabel/dispatch
                             :arg-map {:store-id store-a}}))))
    (testing "subscribing to a store's topic reads it; publishing into the server never passes"
      (is (sync-gate {:op :subscribe :principal alice :topic store-a}))
      (is (not (sync-gate {:op :subscribe :principal alice :topic store-b})))
      (is (not (sync-gate {:op :subscribe :principal bob :topic store-a})))
      (is (not (sync-gate {:op :publish :principal alice :topic store-a}))))
    (testing "without a policy every authenticated principal passes, nobody anonymous"
      (let [open-gate (kabel/authorize-remote {})]
        (is (open-gate {:principal bob :fn-name 'datahike.kabel/dispatch :arg-map {:store-id store-b}}))
        (is (not (open-gate {:principal nil :fn-name 'datahike.kabel/dispatch :arg-map {:store-id store-b}})))))))

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

(deftest listener-refuses-through-the-policy
  (let [port 47391
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
