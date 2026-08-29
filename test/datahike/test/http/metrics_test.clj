(ns datahike.test.http.metrics-test
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.http.routes :as routes]
            [datahike.http.server :as server]
            [datahike.http.system :as system]
            [datahike.metrics :as dhm]
            [konserve.metrics :as konserve-metrics]
            [replikativ.metrics :as metrics]))

(def token "metrics-test-token")

(defn- reset-registry [f]
  (metrics/reset!)
  (dhm/describe!)
  (try (f)
       (finally
         (metrics/reset!)
         (dhm/describe!))))

(use-fixtures :each reset-registry)

(defn- series-value [metric labels]
  (get-in (metrics/snapshot) [metric :series labels]))

(deftest the-gate-records-duration-status-and-rejection-reason
  (let [handler (routes/handler {:token token :max-body-bytes 4})
        post    (fn [headers body]
                  (handler {:request-method :post
                            :uri            "/transact"
                            :headers        headers
                            :body           body}))]
    (is (= 401 (:status (post {} nil))))
    (is (= 413 (:status (post {"authorization" (str "token " token)}
                              (java.io.ByteArrayInputStream. (.getBytes "too large"))))))
    (is (= 403 (:status (routes/authorize {:authorize (constantly false)}
                                          {:datahike/principal {:sub "nobody"}}
                                          :read []))))
    (testing "the request duration is observed once at the gate, including rejections"
      (is (= 1 (:count (series-value :datahike_http_request_seconds
                                     {:op "transact" :status "401"}))))
      (is (= 1 (:count (series-value :datahike_http_request_seconds
                                     {:op "transact" :status "413"})))))
    (testing "operators can distinguish authentication, authorization, and capacity"
      (is (= 1 (series-value :datahike_http_rejected_total {:reason "unauthorized"})))
      (is (= 1 (series-value :datahike_http_rejected_total {:reason "too-large"})))
      (is (= 1 (series-value :datahike_http_rejected_total {:reason "forbidden"}))))))

(defn- get-metrics [handler headers]
  (handler {:request-method :get :uri "/prometheus" :headers headers}))

(deftest prometheus-exposition-is-protected-plain-text
  (let [store-id    #uuid "5dcf9ca4-a6b8-4e16-a159-a6c05930be82"
        connections (atom {[store-id :db] {:conn ::connection :count 3}})
        handler     (server/app {:token token} connections)]
    (is (= 401 (:status (get-metrics handler {})))
        "metrics disclose database ids and traffic shape, so authentication is the default")
    (let [response (get-metrics handler {"authorization" (str "token " token)})
          body     (slurp (:body response))]
      (is (= 200 (:status response)))
      (is (= "text/plain; version=0.0.4; charset=utf-8"
             (get-in response [:headers "content-type"])))
      (is (str/includes? body "datahike_http_request_seconds_bucket"))
      (is (str/includes? body "datahike_query_sample_every 256"))
      (is (str/includes? body "jvm_memory_used_bytes"))
      (is (str/includes? body
                         (str "datahike_connections{branch=\"db\",database=\"" store-id "\"} 3")))
      (is (= 1 (:count (series-value :datahike_http_request_seconds
                                     {:op "metrics" :status "200"})))))))

(deftest metrics-can-be-public-or-disabled-explicitly
  (let [public   (server/app {:metrics {:public? true}} (atom {}))
        disabled (server/app {:token token :metrics false} (atom {}))]
    (is (= 200 (:status (get-metrics public {}))))
    (is (= 404 (:status (get-metrics disabled {"authorization" (str "token " token)}))))))

(deftest server-instances-share-one-process-sink-until-the-last-stops
  (let [before @konserve-metrics/sinks
        first-server  (atom nil)
        second-server (atom nil)
        disabled      (atom nil)]
    (try
      (reset! first-server (server/start-server {:port 0 :join? false :token token}))
      (let [after-first @konserve-metrics/sinks
            added       (set/difference (set (keys after-first)) (set (keys before)))]
        (is (= 1 (count added)))
        (reset! second-server (server/start-server {:port 0 :join? false :token token}))
        (is (= after-first @konserve-metrics/sinks)
            "a second server neither replaces nor duplicates the process sink")
        (server/stop-server @first-server)
        (reset! first-server nil)
        (is (= after-first @konserve-metrics/sinks)
            "stopping one server leaves metrics active for the other")
        (reset! disabled (server/start-server {:port 0 :join? false :token token :metrics false}))
        (is (= after-first @konserve-metrics/sinks)
            "a metrics-disabled server takes no sink lease")
        (server/stop-server @second-server)
        (reset! second-server nil)
        (is (= before @konserve-metrics/sinks)
            "the last metrics-enabled server releases the sink"))
      (finally
        (when-let [instance @first-server] (server/stop-server instance))
        (when-let [instance @second-server] (server/stop-server instance))
        (when-let [instance @disabled] (server/stop-server instance))))))

(deftest shutdown-releases-the-process-sink-when-system-cleanup-fails
  (let [before   @konserve-metrics/sinks
        instance (server/start-server {:port 0 :join? false :token token})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"forced system cleanup failure"
                          (with-redefs [system/close!
                                        (fn [_]
                                          (throw (ex-info "forced system cleanup failure" {})))]
                            (server/stop-server instance))))
    (is (= before @konserve-metrics/sinks)
        "permission cleanup failure does not strand the process metrics sink")))
