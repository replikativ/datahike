(ns datahike.test.http.routes-test
  "Datahike's HTTP API mounted inside a host application, under a prefix,
   driven by the real clients — the HTTP client and the remote writer."
  (:require
   [babashka.http-client :as http]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.http.client :as client]
   [datahike.http.routes :as routes]
   [datahike.migrate.fs :as fs]
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

(deftest embedded-under-a-prefix
  (let [port        23200
        connections (atom {})
        server      (run-jetty (host-app (routes/handler {:token token :dev-mode false}
                                                         {:prefix "/datahike" :connections connections}))
                               {:port port :join? false})
        url         (str "http://localhost:" port "/datahike")]
    (try
      (testing "the host still owns everything outside the prefix"
        (is (= "host" (:body (http/get (str "http://localhost:" port "/anything"))))))

      (testing "a request inside the prefix that matches nothing is a 404, not the host's page"
        (is (= 404 (:status (http/get (str url "/no-such-route") {:throw false})))))

      (testing "the HTTP client works against the prefixed API"
        (let [peer {:backend :datahike-server :url url :token token :format :transit}
              cfg  (client/create-database {:store {:backend :memory
                                                    :id #uuid "de110000-0000-0000-0000-00000000e0b1"}
                                            :schema-flexibility :read
                                            :remote-peer peer})
              conn (client/connect cfg)]
          (client/transact conn [{:name "Ada"}])
          (is (= #{["Ada"]} (client/q '[:find ?n :where [?e :name ?n]] @conn)))))

      (testing "the remote writer works through the prefix, and connects inside the host's registry"
        (let [store-id #uuid "17100000-0000-0000-0000-00000000e0b2"
              ;; A child of a fresh temp dir: konserve refuses a path that exists.
              cfg      {:store {:backend :file :path (str (fs/temp-dir! "dh-routes-") "/store") :id store-id}
                        :keep-history? true
                        :schema-flexibility :read
                        :writer {:backend :datahike-server :url url :token token}}
              conn     (do (d/create-database cfg) (d/connect cfg))]
          (d/transact conn [{:name "Grace"}])
          (is (= #{["Grace"]} (d/q '[:find ?n :where [?e :name ?n]] @conn)))
          (is (some? (get-in @connections [[store-id :db] :conn]))
              "the writer route connected in the atom the host passed in — shared, not private")
          (d/release conn)
          (d/delete-database cfg)))
      (finally
        (.stop server)))))
