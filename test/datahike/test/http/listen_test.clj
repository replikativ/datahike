(ns datahike.test.http.listen-test
  "The authenticated SSE change stream for thin HTTP clients."
  (:require
   [babashka.http-client :as http]
   [clojure.core.async :as async]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.connections :refer [*connections*]]
   [datahike.http.client :as client]
   [datahike.http.routes :as routes]
   [datahike.json :as json]
   [jsonista.core :as j]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.core.protocols :as protocols])
  (:import
   [java.io BufferedReader ByteArrayOutputStream InputStreamReader OutputStream]
   [java.net ServerSocket URLEncoder]))

(def ^:private token "listen-test-token")

(defn- free-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- encode [x]
  (URLEncoder/encode (str x) "UTF-8"))

(defn- listen-url [url store-id & [{:keys [branch since]}]]
  (str url "/listen?store=" store-id
       (when branch (str "&branch=" (encode branch)))
       (when since (str "&since=" since))))

(defn- open-stream [url store-id opts]
  (let [response (http/get (listen-url url store-id opts)
                           {:headers {"authorization" (str "token " token)}
                            :as :stream})]
    (is (= 200 (:status response)))
    (is (str/starts-with? (get-in response [:headers "content-type"] "")
                          "text/event-stream"))
    (assoc response :reader (BufferedReader. (InputStreamReader. (:body response) "UTF-8")))))

(defn- read-event
  ([stream] (read-event stream 5000))
  ([stream timeout-ms]
   (let [result (future
                  (loop [event nil data nil]
                    (let [line (.readLine ^BufferedReader (:reader stream))]
                      (cond
                        (nil? line) {:event :eof}
                        (str/starts-with? line "event: ")
                        (recur (keyword (subs line 7)) data)
                        (str/starts-with? line "data: ")
                        (recur event (j/read-value (subs line 6) json/mapper))
                        (and (str/blank? line) event)
                        {:event event :data data}
                        :else (recur event data)))))]
     (if (= ::timeout (deref result timeout-ms ::timeout))
       (do (future-cancel result) ::timeout)
       @result))))

(defn- close-stream! [stream]
  (.close ^BufferedReader (:reader stream)))

(deftest authentication-and-authorization-run-before-listening
  (let [port (free-port)
        store-id (random-uuid)
        app (routes/handler {:token token
                             :authorize (constantly false)})
        server (run-jetty app {:host "127.0.0.1" :port port :join? false})
        url (str "http://127.0.0.1:" port)]
    (try
      (is (= 401 (:status (http/get (listen-url url store-id nil)
                                    {:throw false}))))
      (let [response (http/get (listen-url url store-id nil)
                               {:throw false
                                :headers {"authorization" (str "token " token)}})]
        (is (= 403 (:status response)))
        (is (= :datahike.http/forbidden
               (get-in (j/read-value (:body response) json/mapper) [:ex-data :type]))))
      (finally
        (.stop server)
        (routes/release-all! app)))))

(deftest listeners-see-resync-reports-and-deletion
  (let [port (free-port)
        store-id (random-uuid)
        cfg {:store {:backend :memory :id store-id} :schema-flexibility :read}
        connections (atom {})
        app (routes/handler {:token token} {:connections connections})
        server (run-jetty app {:host "127.0.0.1" :port port :join? false})
        url (str "http://127.0.0.1:" port)
        remote-peer {:backend :datahike-server :url url :token token :format :cbor}]
    (binding [*connections* connections]
      (d/create-database cfg))
    (let [conn (client/connect (assoc cfg :remote-peer remote-peer))
          current (:commit-id @conn)
          current-stream (open-stream url store-id {:since current})
          stale-stream (open-stream url store-id nil)]
      (try
        (testing "an absent since resyncs, while an equal since waits for a report"
          (is (= :resync (:event (read-event stale-stream)))))
        (testing "one HTTP transaction reaches both listeners once with datoms"
          (let [report (client/transact conn [{:name "Ada"}])
                first-current (read-event current-stream)
                first-stale (read-event stale-stream)]
            (doseq [event [first-current first-stale]]
              (is (= :report (:event event)))
              (is (= (:commit-id (:db-after report)) (get-in event [:data :commit-id])))
              (is (seq (get-in event [:data :tx-data]))))
            (let [second-report (client/transact conn [{:name "Grace"}])]
              (doseq [stream [current-stream stale-stream]]
                (let [event (read-event stream)]
                  (is (= :report (:event event)))
                  (is (= (:commit-id (:db-after second-report))
                         (get-in event [:data :commit-id]))))))))
        (testing "HTTP load-entities reaches the report bus"
          (let [report (client/load-entities conn [[100 :loaded true 536870913 true]])]
            (doseq [stream [current-stream stale-stream]]
              (let [event (read-event stream)]
                (is (= :report (:event event)))
                (is (= (:commit-id (:db-after report))
                       (get-in event [:data :commit-id])))))))
        (testing "deletion is terminal"
          (try (client/release conn)
               (catch Throwable _))
          (client/delete-database (assoc cfg :remote-peer remote-peer))
          (doseq [stream [current-stream stale-stream]]
            (is (= :deleted (:event (read-event stream))))
            (is (= :eof (:event (read-event stream))))))
        (finally
          (close-stream! current-stream)
          (close-stream! stale-stream)
          (.stop server)
          (routes/release-all! app))))))

(deftest deleting-a-branch-terminates-its-stream
  (let [port (free-port)
        store-id (random-uuid)
        cfg {:store {:backend :memory :id store-id} :schema-flexibility :read}
        connections (atom {})
        app (routes/handler {:token token} {:connections connections})
        server (run-jetty app {:host "127.0.0.1" :port port :join? false})
        url (str "http://127.0.0.1:" port)
        remote-peer {:backend :datahike-server :url url :token token :format :cbor}]
    (binding [*connections* connections]
      (d/create-database cfg))
    (let [main (client/connect (assoc cfg :remote-peer remote-peer))]
      (try
        (client/branch! main :db :feature)
        (let [branch (client/connect (assoc cfg :branch :feature :remote-peer remote-peer))
              stream (open-stream url store-id {:branch :feature
                                                :since (:commit-id @branch)})]
          (try
            (client/delete-branch! main :feature)
            (is (= :deleted (:event (read-event stream))))
            (is (= :eof (:event (read-event stream))))
            (finally
              (close-stream! stream)
              (try (client/release branch) (catch Throwable _)))))
        (finally
          (try (client/release main) (catch Throwable _))
          (try (client/delete-database (assoc cfg :remote-peer remote-peer))
               (catch Throwable _))
          (.stop server)
          (routes/release-all! app))))))

(deftest a-slow-writer-is-coalesced
  (let [store-id (random-uuid)
        cfg {:store {:backend :memory :id store-id} :schema-flexibility :read}
        connections (atom {})
        app (routes/handler {:token token} {:connections connections})
        conn (binding [*connections* connections]
               (d/create-database cfg)
               (d/connect cfg))
        head (d/commit-id @conn)
        response (app {:request-method :get
                       :uri "/listen"
                       :query-string (str "store=" store-id "&since=" head)
                       :headers {"authorization" (str "token " token)}})
        entered (promise)
        proceed (promise)
        bytes (ByteArrayOutputStream.)
        blocked-output
        (proxy [OutputStream] []
          (write
            ([b]
             (deliver entered true)
             @proceed
             (.write bytes (int b)))
            ([b off len]
             (deliver entered true)
             @proceed
             (.write bytes b off len))))
        writing (future
                  (protocols/write-body-to-stream (:body response) response blocked-output))]
    (try
      (d/transact conn [{:n 0}])
      (is (= true (deref entered 5000 ::timeout)))
      (doseq [n (range 1 25)]
        (d/transact conn [{:n n}]))
      (deliver proceed true)
      (loop [attempts 100]
        (when (and (pos? attempts)
                   (not (str/includes? (.toString bytes "UTF-8") "event: coalesced")))
          (Thread/sleep 10)
          (recur (dec attempts))))
      (is (str/includes? (.toString bytes "UTF-8") "event: coalesced"))
      (finally
        (doseq [subscriber (mapcat val @(-> app meta ::routes/state :subscribers))]
          (async/close! (:channel subscriber)))
        (deliver proceed true)
        (deref writing 5000 nil)
        (binding [*connections* connections]
          (d/release conn)
          (d/delete-database cfg))
        (routes/release-all! app)))))
