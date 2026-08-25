(ns datahike.test.s3-concurrency-runner
  "End-to-end CLJS validation for independent Datahike writers over a tiered
   memory/S3 store. Run against MinIO with `bb cljs-s3-concurrency-test`.

   The orchestrating task starts two separate Node processes. Separate processes
   matter: Datahike intentionally reuses one writer per store/branch inside a
   runtime, so two connections in one process would not test global fencing."
  (:require [cljs.core.async :refer [<!]]
            [datahike.api :as d]
            [datahike.connector :as connector]
            [konserve-s3.core]
            [taoensso.trove :as trove]
            [taoensso.trove.console :as trove-console])
  (:require-macros [cljs.core.async :refer [go]]))

(def store-id #uuid "8f487c89-5ca7-4ae8-bd9d-ae8e00d59531")

(trove/set-log-fn! (trove-console/get-log-fn {:min-level :warn}))

(defn- env [name fallback]
  (or (aget (.-env js/process) name) fallback))

(defn- config []
  (let [s3 {:backend :s3
            :id store-id
            :endpoint (env "S3_ENDPOINT" "http://localhost:9000")
            :bucket (env "S3_BUCKET" "datahike-test")
            :access-key (env "S3_ACCESS_KEY" "minioadmin")
            :secret (env "S3_SECRET" "minioadmin")
            :region (env "S3_REGION" "us-east-1")
            :path-style? (not= "false" (env "S3_PATH_STYLE" "true"))}]
    {:store {:backend :tiered
             :id store-id
             :frontend-config {:backend :memory :id store-id}
             :backend-config s3
             :write-policy :write-through
             :read-policy :frontend-first}
     :writer {:backend :self
              :writer-ownership :shared
              :require-fencing :global
              ;; Expose conflicts so this runner can prove they happened and
              ;; retry explicitly from a freshly hydrated branch head.
              :head-conflict-retries 0
              :head-conflict-backoff-ms 0}
     :schema-flexibility :write
     :keep-history? false
     :initial-tx [{:db/ident :s3-test/id
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique :db.unique/identity}]
     ;; Force enough tree structure that the second writer must hydrate index
     ;; nodes absent from its memory frontend; an inlined one-node DB would not
     ;; exercise the mismatch that prompted this test.
     :index-config {:branching-factor 8}}))

(defn- fail! [e]
  (js/console.error (or (.-stack e) (ex-message e) (pr-str e)))
  (.exit js/process 1))

(defn- succeed! [message]
  (js/console.log message)
  (.exit js/process 0))

(defn- value! [x]
  (if (instance? js/Error x) (throw x) x))

(defn- init! []
  (go
    (try
      ;; A previous interrupted local run may have left the fixed test store.
      (value! (<! (d/delete-database (config))))
      (value! (<! (d/create-database (config))))
      (succeed! "S3_INIT_OK")
      (catch :default e (fail! e)))))

(defn- transact-one! [conn worker i conflicts]
  (go
    (loop []
      (let [result (<! (d/transact! conn [{:s3-test/id (str worker "-" i)}]))]
        (if (= :datahike/head-conflict (:type (ex-data result)))
          (do (swap! conflicts inc)
              (recur))
          result)))))

(defn- worker! [worker n]
  (go
    (try
      (let [conn (value! (<! (d/connect (config) {:sync? false})))
            conflicts (atom 0)]
        (dotimes [i n]
          (value! (<! (transact-one! conn worker i conflicts))))
        (d/release conn true)
        (succeed! (str "S3_WORKER_OK " worker " " @conflicts)))
      (catch :default e (fail! e)))))

(defn- verify! [expected]
  (go
    (try
      (let [conn (value! (<! (d/connect (config) {:sync? false})))
            ;; This is the async acquisition boundary used by the npm `db()`
            ;; binding. The returned immutable DB is queried synchronously.
            db (value! (<! (connector/db-async conn)))
            ids (d/q '[:find [?id ...] :where [_ :s3-test/id ?id]] db)]
        (when-not (= expected (count ids))
          (throw (ex-info "Concurrent S3 writers lost or duplicated transactions."
                          {:expected expected :actual (count ids) :ids ids})))
        (d/release conn true)
        (value! (<! (d/delete-database (config))))
        (succeed! (str "S3_VERIFY_OK " (count ids))))
      (catch :default e (fail! e)))))

(defn -main [& args]
  (let [[mode arg1 arg2] args
        _operation (case mode
                     "init" (init!)
                     "worker" (worker! arg1 (js/parseInt arg2 10))
                     "verify" (verify! (js/parseInt arg1 10))
                     (throw (js/Error. (str "Unknown mode: " mode))))]
    nil))

(set! *main-cli-fn* -main)
