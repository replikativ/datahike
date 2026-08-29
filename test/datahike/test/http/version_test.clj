(ns datahike.test.http.version-test
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [datahike.http.server :as server]
            [datahike.tools :as tools]))

(def token "version-test-token")

(defn- get-version [handler authorization]
  (handler {:request-method :get
            :uri            "/version"
            :headers        (cond-> {"accept" "application/edn"}
                              authorization
                              (assoc "authorization" authorization))}))

(deftest version-is-authenticated-content-negotiated-operational-metadata
  (let [config  {:token token
                 :port  8080
                 :database {:store {:backend  :jdbc
                                    :user     "datahike"
                                    :password "database-secret"}}
                 :nested {:access-key "access-secret"
                          :secret-key "secret-secret"}}
        handler (with-redefs [tools/datahike-version "1.2.3"
                              tools/datahike-git-sha "0123456789abcdef0123456789abcdef01234567"
                              tools/konserve-version "9.8.7"]
                  (server/app config (atom {})))]
    (is (= 401 (:status (get-version handler nil)))
        "build metadata and deployment topology are not public")
    (with-redefs [tools/datahike-version "1.2.3"
                  tools/datahike-git-sha "0123456789abcdef0123456789abcdef01234567"
                  tools/konserve-version "9.8.7"]
      (let [response (get-version handler (str "token " token))
            info     (edn/read-string (slurp (:body response)))]
        (is (= 200 (:status response)))
        (is (= "application/edn; charset=utf-8"
               (get-in response [:headers "Content-Type"])))
        (is (= "1.2.3" (:datahike-version info)))
        (is (= "0123456789abcdef0123456789abcdef01234567" (:git-sha info)))
        (is (= "9.8.7" (:konserve-version info)))
        (is (set/subset? #{:dynamodb :file :jdbc :memory :redis :s3 :tiered}
                         (set (:konserve-backends info))))
        (is (not (some #{:default} (:konserve-backends info))))
        (testing "configuration remains useful without exposing credentials"
          (is (= 8080 (get-in info [:config :port])))
          (is (= "REDACTED" (get-in info [:config :token])))
          (is (= "REDACTED" (get-in info [:config :database :store :password])))
          (is (= "REDACTED" (get-in info [:config :nested :access-key])))
          (is (= "REDACTED" (get-in info [:config :nested :secret-key]))))))))
