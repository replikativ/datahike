(ns datahike.test.http.admin-test
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.http.server :as server]
   [datahike.http.system :as system]))

(defn- request [handler uri & [headers]]
  (handler {:request-method :get
            :uri uri
            :headers (or headers {})}))

(deftest admin-is-a-public-shell-over-authenticated-data-apis
  (let [token   "admin-page-test-token"
        handler (server/app {:token token
                             :system-db {:store {:backend :memory}}}
                            (atom {}))]
    (try
      (testing "the shell is directly navigable without putting a token in the URL"
        (let [response (request handler "/")
              body     (slurp (:body response))]
          (is (= 200 (:status response)))
          (is (= "text/html; charset=utf-8"
                 (get-in response [:headers "content-type"])))
          (is (str/includes? (get-in response [:headers "content-security-policy"])
                             "default-src 'none'"))
          (is (str/includes? body "https://datahike.io"))
          (is (str/includes? body "https://github.com/replikativ/datahike/blob/main/doc/README.md"))
          (is (str/includes? body "Datahike"))
          (is (not (str/includes? body token)))))
      (is (= 200 (:status (request handler "/admin"))))
      (is (= 308 (:status (request handler "/admin/"))))

      (testing "the established Datahike logo is served locally"
        (let [response (request handler "/admin/datahike-logo.svg")]
          (is (= 200 (:status response)))
          (is (= "image/svg+xml" (get-in response [:headers "content-type"])))
          (is (str/starts-with? (slurp (:body response)) "<svg"))))

      (testing "the script uses tab-scoped token storage and the existing APIs"
        (let [response (request handler "/admin/app.js"
                                {"content-type" "application/json"})
              body     (slurp (:body response))]
          (is (= 200 (:status response)))
          (is (= "text/javascript; charset=utf-8"
                 (get-in response [:headers "content-type"])))
          (is (= "no-store" (get-in response [:headers "cache-control"])))
          (is (str/includes? body "sessionStorage"))
          (is (str/includes? body "Authorization"))
          (is (str/includes? body "taggedValue"))
          (is (str/includes? body "request(`/admin/status?${params}`)"))
          (is (not (str/includes? body "innerHTML")))))

      (testing "static visibility does not bypass API authentication"
        (is (= 401 (:status (request handler "/version"))))
        (is (= 401 (:status (request handler "/databases"))))
        (is (= 401 (:status (request handler "/admin/status"))))
        (is (= 200 (:status (request handler "/version"
                                     {"authorization" (str "token " token)})))))

      (testing "status is authenticated and does not require opening a catalog database"
        (let [response (request handler "/admin/status"
                                {"authorization" (str "token " token)
                                 "accept" "application/edn"})
              body     (edn/read-string (slurp (:body response)))]
          (is (= 200 (:status response)))
          (is (map? (:node body)))
          (is (= {:enabled false} (get-in body [:node :nrepl])))
          (is (= [] (:databases body)))))
      (finally
        (system/close! (:datahike.http.server/config (meta handler)))))))
