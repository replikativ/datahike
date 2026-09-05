(ns datahike.test.remote-test
  "The thin HTTP client against a live server, on Node.js: the ClojureScript
   API (`datahike.http.client`) and the JavaScript boundary
   (`datahike.js.remote`). `bb node-remote-test` starts the server and
   points DATAHIKE_REMOTE_URL / DATAHIKE_REMOTE_TOKEN at it; without them the
   suite reports itself skipped."
  (:require [cljs.test :refer [deftest is async] :as t]
            [datahike.http.client :as client]
            [datahike.remote :as remote]
            [datahike.js.remote :as js-remote]
            [cljs.core.async :refer [<! go] :as core-async]))

(def ^:private url (some-> js/process .-env .-DATAHIKE_REMOTE_URL))
(def ^:private token (some-> js/process .-env .-DATAHIKE_REMOTE_TOKEN))
(def ^:private peer {:backend :datahike-server :url url :token token})

(defn- <ok
  "Take from a client channel; an exception delivered there is thrown."
  [ch]
  (go (let [v (<! ch)]
        (if (instance? js/Error v) (throw v) v))))

(defn- <event
  "Take one listener event or fail so a broken stream cannot hang the suite."
  [events]
  (let [out (core-async/promise-chan)
        pending? (atom true)
        timeout (js/setTimeout
                 #(when (compare-and-set! pending? true false)
                    (core-async/put! out (js/Error. "Timed out waiting for a change event")))
                 5000)]
    (core-async/take! events
                      (fn [value]
                        (when (compare-and-set! pending? true false)
                          (js/clearTimeout timeout)
                          (core-async/put! out value))))
    out))

(defn- config []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :read
   :remote-peer peer})

(deftest listen-json-and-crlf-chunks
  (let [decoded (client/decode-listen-json
                 #js ["!set" #js [#js ["!kw" "one"]
                                  #js ["!sym" "two"]]])
        [frames pending] (client/split-listen-chunk
                          "" "event: report\r\ndata: {\"max-tx\":1}\r")
        [frames-2 pending-2] (client/split-listen-chunk pending "\n\r\n")]
    (is (= #{:one 'two} decoded) "all collection element tags are decoded")
    (is (empty? frames) "a trailing CR is retained rather than becoming a frame end")
    (is (= ["event: report\ndata: {\"max-tx\":1}"] frames-2)
        "a CR/LF split across chunks produces exactly one frame")
    (is (empty? pending-2))))

(deftest cljs-api-against-the-server
  (async done
         (go
           (try
             (let [cfg  (<! (<ok (client/create-database (config))))
                   _    (is (= peer (:remote-peer cfg)) "the caller's peer goes back on the configuration")
                   conn (<! (<ok (client/connect cfg)))
                   _    (is (instance? remote/RemoteConnection conn))
                   rep  (<! (<ok (client/transact conn [{:name "Ada" :age 36} {:name "Grace" :age 45}])))
                   _    (is (instance? remote/RemoteDB (:db-after rep)) "a report carries database handles")
                   _    (is (= 2 (count (filter #(= :name (:a %)) (:tx-data rep)))) "datoms come back as datoms")
                   db   (<! (<ok (client/db conn)))
                   _    (is (instance? remote/RemoteDB db))
                   _    (is (= peer (remote/remote-peer db)) "a handle knows its peer")
                   small (<! (<ok (client/q '[:find ?n :where [?e :name ?n]] db)))
                   _    (is (= #{["Ada"] ["Grace"]} small) "a small read, sent as a GET with its arguments in the URL")
                   ages (vec (range 3000))
                   big  (<! (<ok (client/q '[:find ?n :in $ [?a ...] :where [?e :age ?a] [?e :name ?n]] db ages)))
                   _    (is (= #{["Ada"] ["Grace"]} big) "a large read, sent as a POST")
                   e    (<! (<ok (client/q '[:find ?e . :where [?e :name "Ada"]] db)))
                   pulled (<! (<ok (client/pull db '[:name :age] e)))
                   _    (is (= {:name "Ada" :age 36} pulled))
                   _    (<! (client/release conn))
                   _    (<! (<ok (client/delete-database cfg)))]
               (is (false? (<! (<ok (client/database-exists? cfg)))) "deleted"))
             (catch :default e
               (is false (str "unexpected: " (ex-message e) " " (pr-str (ex-data e))))))
           (done))))

(deftest cljs-change-listener
  (async done
         (go
           (try
             (let [cfg (<! (<ok (client/create-database (config))))
                   conn (<! (<ok (client/connect cfg)))
                   events (core-async/chan 8)
                   key (client/listen conn :remote-test #(core-async/put! events %))
                   resync (<! (<ok (<event events)))
                   _ (is (= :remote-test key) "an explicit listener key is returned")
                   _ (is (:resync resync) "a listener without since starts with a resync")
                   _ (is (instance? remote/RemoteDB (:db-after resync)))
                   _ (<! (<ok (client/transact conn [{:db/id "Serg"
                                                      :listener/value :received
                                                      :listener/symbol 'tagged}])))
                   report (<! (<ok (<event events)))
                   _ (is (not (:resync report)) "the next event is a transaction report")
                   _ (is (= [:received] (mapv :v (filter #(= :listener/value (:a %))
                                                         (:tx-data report))))
                         "tagged JSON datoms and keywords are decoded")
                   _ (is (number? (get (:tempids report) "Serg"))
                         "string tempid keys remain strings")
                   _ (is (= ['tagged]
                            (mapv :v (filter #(= :listener/symbol (:a %))
                                             (:tx-data report))))
                         "tagged JSON symbols are decoded")
                   db (<! (<ok (client/db conn)))
                   _ (is (= (:commit-id report) (:commit-id (:db-after report)) (:commit-id db))
                         "the event handle is the new head")
                   _ (client/unlisten conn key)
                   _ (<! (<ok (client/transact conn [{:listener/value :after-unlisten}])))
                   [_ port] (core-async/alts! [events (core-async/timeout 300)])
                   _ (is (not= port events) "unlisten stops delivery")
                   failures (core-async/chan 2)
                   bad-conn (assoc conn :remote-peer (assoc peer :token "wrong"))
                   _ (client/listen bad-conn :permanent-failure #(core-async/put! failures %))
                   failure (<! (<ok (<event failures)))
                   _ (is (= 401 (:status failure)))
                   _ (is (instance? ExceptionInfo (:error failure))
                         "a permanent HTTP status reaches the application as ex-info")
                   [_ retry-port] (core-async/alts! [failures (core-async/timeout 800)])
                   _ (is (not= retry-port failures)
                         "a permanent HTTP status stops instead of reconnecting")
                   _ (<! (client/release conn))
                   _ (<! (<ok (client/delete-database cfg)))]
               (is true))
             (catch :default e
               (is false (str "unexpected: " (ex-message e) " " (pr-str (ex-data e))))))
           (done))))

(deftest javascript-boundary-against-the-server
  (async done
    ;; As in the JavaScript docs: the configuration object holding the uuid()
    ;; value is what connect and deleteDatabase take; the echoed copy renders
    ;; its id as a plain string, like every uuid at the JavaScript boundary.
         (let [cfg #js {:store #js {:backend ":memory" :id (js-remote/randomUuid)}
                        :schema-flexibility ":read"
                        :remote-peer #js {:backend ":datahike-server" :url url :token token}}]
           (-> (js-remote/createDatabase cfg)
               (.then (fn [echoed]
                        (is (some? (aget echoed "store")) "a configuration comes back as a JavaScript object")
                        (.then (js-remote/connect cfg)
                               (fn [conn]
                                 (-> (js-remote/transact conn #js [#js {:name "Linus"}])
                                     (.then (fn [report]
                                              (is (some? (aget report "db-after")) "a report is a JavaScript object with handles inside")
                                              (js-remote/db conn)))
                                     (.then (fn [db] (js-remote/q "[:find ?n :where [?e :name ?n]]" db)))
                                     (.then (fn [result]
                                              (is (= [["Linus"]] (js->clj result)) "results are JavaScript values")
                                              (let [phase (atom :resync)
                                                    key (atom nil)
                                                    report-promise
                                                    (js/Promise.
                                                     (fn [resolve reject]
                                                       (let [timeout (js/setTimeout
                                                                      #(do
                                                                         (when @key
                                                                           (js-remote/unlisten conn @key))
                                                                         (reject (js/Error. "Timed out waiting for a JavaScript change event")))
                                                                      5000)]
                                                         (reset! key
                                                                 (js-remote/listen
                                                                  conn
                                                                  (fn [report]
                                                                    (case @phase
                                                                      :resync
                                                                      (do
                                                                        (is (object? report) "a listener receives a JavaScript object")
                                                                        (is (true? (aget report "resync")))
                                                                        (reset! phase :report)
                                                                        (-> (js-remote/transact
                                                                             conn #js [#js {:name "Listener"}])
                                                                            (.catch reject)))

                                                                      :report
                                                                      (do
                                                                        (js/clearTimeout timeout)
                                                                        (resolve report)))))))))]
                                                (.then report-promise
                                                       (fn [report]
                                                         (is (some? (aget report "db-after"))
                                                             "a JavaScript change report carries a handle")
                                                         (js-remote/unlisten conn @key)
                                                         (js-remote/release conn))))))
                                     (.then (fn [_] (js-remote/deleteDatabase cfg))))))))
               (.catch (fn [e] (is false (str "unexpected: " e))))
               (.finally done)))))

(defn -main [& _]
  (if (and url token)
    (do (println "thin client tests against" url)
        (t/run-tests 'datahike.test.remote-test))
    (println "DATAHIKE_REMOTE_URL / DATAHIKE_REMOTE_TOKEN not set; thin client tests skipped")))

(defmethod t/report [:cljs.test/default :end-run-tests] [m]
  (when-not (t/successful? m)
    (set! (.-exitCode js/process) 1)))

(set! *main-cli-fn* -main)
