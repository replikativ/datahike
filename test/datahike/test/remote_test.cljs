(ns datahike.test.remote-test
  "The thin HTTP client against a live server, on Node.js: the ClojureScript
   API (`datahike.http.client`) and the JavaScript boundary
   (`datahike.js.remote`). `bb node-remote-test` starts the server and
   points DATAHIKE_REMOTE_URL / DATAHIKE_REMOTE_TOKEN at it; without them the
   suite reports itself skipped."
  (:require [cljs.test :refer [deftest is testing async] :as t]
            [datahike.http.client :as client]
            [datahike.remote :as remote]
            [datahike.js.remote :as js-remote]
            [cljs.core.async :refer [<! go]]
            [cljs.nodejs :as nodejs]))

(def ^:private url (some-> js/process .-env .-DATAHIKE_REMOTE_URL))
(def ^:private token (some-> js/process .-env .-DATAHIKE_REMOTE_TOKEN))
(def ^:private peer {:backend :datahike-server :url url :token token})

(defn- <ok
  "Take from a client channel; an exception delivered there is thrown."
  [ch]
  (go (let [v (<! ch)]
        (if (instance? js/Error v) (throw v) v))))

(defn- config []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :read
   :remote-peer peer})

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
                                              (js-remote/release conn)))
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
