(ns datahike.test.http.headers-test
  "A remote peer's `:headers-fn` adds headers to every request, which is how a
   client reaches a server behind a gateway that wants its own credential."
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.client :as api]
   [datahike.http.server :refer [start-server stop-server]]))

(deftest request-headers-precedence
  (testing "no peer options: the base headers, lower-cased"
    (is (= {"accept" "x"} (api/request-headers {} {:accept "x"}))))
  (testing ":headers-fn adds headers; names are case-insensitive"
    (is (= {"x-gateway" "g" "accept" "x"}
           (api/request-headers {:headers-fn (fn [] {"X-Gateway" "g"})}
                                {:accept "x"}))))
  (testing "the request's own headers win over :headers-fn"
    (is (= {"accept" "x"}
           (api/request-headers {:headers-fn (fn [] {"Accept" "other"})}
                                {"accept" "x"}))))
  (testing ":token keeps authorization to itself"
    (is (= {"authorization" "token t"}
           (api/request-headers {:token "t"
                                 :headers-fn (fn [] {"Authorization" "Bearer g"})}
                                {}))))
  (testing "without :token, :headers-fn may set authorization"
    (is (= {"authorization" "Bearer g"}
           (api/request-headers {:headers-fn (fn [] {:authorization "Bearer g"})}
                                {})))))

(deftest headers-fn-runs-on-every-request
  (let [calls (atom 0)
        peer  {:headers-fn (fn [] (swap! calls inc) {"x-n" (str @calls)})}]
    (is (= {"x-n" "1"} (api/request-headers peer {})))
    (is (= {"x-n" "2"} (api/request-headers peer {})))))

(deftest headers-fn-reaches-the-server
  (let [seen   (atom [])
        secret "gateway-secret"
        ;; A server that trusts a header, as a gateway's credential, and has no
        ;; shared token at all.
        server (start-server {:port       0
                              :host       "127.0.0.1"
                              :join?      false
                              :validator  (fn [request]
                                            (swap! seen conj (get-in request [:headers "x-gateway-auth"]))
                                            (when (= secret (get-in request [:headers "x-gateway-auth"]))
                                              {:sub "gateway"}))})
        port   (.getLocalPort ^org.eclipse.jetty.server.ServerConnector
                (first (.getConnectors ^org.eclipse.jetty.server.Server server)))
        url    (str "http://127.0.0.1:" port)
        store  {:backend :memory :id #uuid "de110000-0000-0000-0000-0000000000a1"}]
    (try
      (doseq [fmt [:cbor :edn :transit :json]]
        (testing (str "format " fmt)
          (let [calls  (atom 0)
                peer   {:backend    :datahike-server
                        :url        url
                        :format     fmt
                        :headers-fn (fn [] (swap! calls inc) {"X-Gateway-Auth" secret})}
                cfg    (assoc {:store store :schema-flexibility :read} :remote-peer peer)
                created (api/create-database cfg)
                conn    (api/connect created)]
            (try
              (is (map? created) "a function in the config is not sent to the server")
              (api/transact conn [{:name "Peter" :age 42}])
              (is (= #{["Peter"]}
                     (api/q '[:find ?n :where [?e :name ?n]] @conn)))
              (is (< 2 @calls) "the function ran for each request")
              (finally
                (api/release conn)
                (api/delete-database created)))))
        ;; The first accepted request cannot be a stale credential.
        (is (every? #{secret nil} @seen)))
      (testing "a peer without the header is refused"
        (let [error (try
                      (api/create-database {:store store
                                            :remote-peer {:backend :datahike-server
                                                          :url url}})
                      nil
                      (catch Exception e e))]
          (is (some? error))
          (is (= 401 (:status (ex-data error))))))
      (finally
        (stop-server server)))))
