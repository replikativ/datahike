(ns datahike.compat
  (:require [datahike.api :as d]
            [datahike.tools :as dt]
            [datahike.migrate :as dm]
            [wanderung.core :as w]
            [taoensso.timbre :as t]
            [clojure.string :as s])
  (:import [java.util Date]))

(def compat-version 0)

(def default-schema [{:db/ident       :page/title
                      :db/cardinality :db.cardinality/one
                      :db/unique :db.unique/identity
                      :db/valueType   :db.type/string}
                     {:db/ident :page/counter
                      :db/cardinality :db.cardinality/one
                      :db/valueType :db.type/long}
                     {:db/ident :page/text
                      :db/cardinality :db.cardinality/one
                      :db/valueType :db.type/string}
                     {:db/valueType :db.type/keyword
                      :db/ident :page/tags
                      :db/cardinality :db.cardinality/many}])

(def default-size 1000)

(defn default-cfg [target-folder version]
  {:store  {:backend :file
            :path (format "%s/datahike_compat_%s" target-folder version)}
   :keep-history? true
   :schema-flexibility :write
   :initial-tx default-schema})

(defn default-conn-cfg [target-folder version]
  {:store  {:backend :file
            :path (format "%s/datahike_compat_%s" target-folder version)}
   :keep-history? true
   :schema-flexibility :write
   :initial-tx default-schema})

(defn write-db
  ([]
   (write-db nil))
  ([{:keys [config size target-folder]
     :or {size default-size}
     :as opts}]
   (let [target-folder (or target-folder (System/getProperty "java.io.tmpdir"))
         version (or (dt/get-version 'io.replikativ/datahike) "development")
         config (or config (default-cfg target-folder version))]
     (t/set-level! :info)
     (t/info "Opts" opts)
     (t/info "Setting up database" config)
     (d/delete-database config)
     (d/create-database config)

     (let [conn (d/connect config)
           tx-size 10
           tx-count (-> (/ size tx-size)
                        Math/ceil
                        long)
           export-file (format "%s/datahike_%s.nippy" target-folder version)]
       (t/info "Transaction data of size" size)
       (t/info (:meta @conn))
       (time (doseq [i (range tx-count)]
               (let [from (* tx-size i)
                     to (* tx-size (inc i))
                     tx-data (mapv (fn [j]
                                     {:page/title (str "T" j)
                                      :page/counter j
                                      :page/text (format "TXT%sTXT" j)
                                      :page/tags [:tag1 :tag2 :tag3]})
                                   (range from (if (< size to) size to)))]
                 (d/transact conn {:tx-data tx-data}))))
       (t/info "Exporting database to" export-file)
       (w/migrate (merge config
                         {:wanderung/type :datahike})
                  {:wanderung/type :nippy
                   :filename export-file})
       #_(let [export-input (cond
                            (= version "development") conn
                            (->> (s/split version #"\.")
                                 last
                                 Long/parseLong
                                 (> 1489)) @conn
                            :else conn)]
         (dm/export-db export-input export-file))
       (t/info "Done")))))

(defn check-db-data [conn]
  (let [db-data (->> (d/q '[:find [(pull ?e [:page/title :page/text :page/counter :page/tags]) ...]
                            :where
                            [?e :page/title _]]
                          @conn)
                     (into #{}))
        expected-data (->> (range default-size)
                           (mapv (fn [i] {:page/title (str "T" i)
                                          :page/counter i
                                          :page/text (format "TXT%sTXT" i)
                                          :page/tags [:tag1 :tag2 :tag3]}))
                           (into #{}))]
    (= expected-data db-data)))

(defn check-db-schema [conn]
  (let [expected-schema #:page{:title
                               #:db{:ident :page/title,
                                    :cardinality :db.cardinality/one,
                                    :unique :db.unique/identity,
                                    :valueType :db.type/string},
                               :tags
                               #:db{:valueType :db.type/keyword,
                                    :cardinality :db.cardinality/many,
                                    :ident :page/tags},
                               :counter
                               #:db{:ident :page/counter,
                                    :cardinality :db.cardinality/one,
                                    :valueType :db.type/long},
                               :text
                               #:db{:ident :page/text,
                                    :cardinality :db.cardinality/one,
                                    :valueType :db.type/string}}
        schema (-> (d/schema @conn)
                   (update :page/title dissoc :db/id)
                   (update :page/tags dissoc :db/id)
                   (update :page/counter dissoc :db/id)
                   (update :page/text dissoc :db/id))]
    (= expected-schema schema)))

(defn check-db-reverse-schema [conn]
  (let [expected-reverse-schema {:db/ident #{:page/counter :page/text :page/tags :page/title},
                                 :db.cardinality/many #{:page/tags},
                                 :db/unique #{:page/title},
                                 :db.unique/identity #{:page/title},
                                 :db/index #{:page/title}}
        reverse-schema (d/reverse-schema @conn)]
    (= expected-reverse-schema
       reverse-schema)))

(defn check-tx-count [conn]
  (let [expected-tx-count 101 ;; schema + 100 txs à 10
        tx-count (d/q '[:find (count ?t) .
                        :where
                        [?t :db/txInstant _ ?t]]
                      @conn)]
    (= expected-tx-count
       tx-count)))

(defonce conn-results (atom {::version compat-version
                             :datahike.lib/version ""
                             ::connection? false
                             ::count? false
                             ::data? false
                             ::schema? false
                             ::reverse-schema? false
                             ::tx-count? false
                             ::import false
                             :datahike.import/connection? false
                             :datahike.import/count? false
                             :datahike.import/data? false
                             :datahike.import/schema? false
                             :datahike.import/reverse-schema? false
                             :datahike.import/tx-count? false}))

(defn assert-db [name pred]
  (swap! conn-results assoc name pred))

(defn test-db [conn type]
  (t/info "Testing data ...")
  (assert-db (keyword type "connection?") true)
  (assert-db (keyword type "count?") (= 6114 (count @conn)))
  (assert-db (keyword type "data?") (check-db-data conn))
  (assert-db (keyword type "schema?") (check-db-schema conn))
  (assert-db (keyword type "reverse-schema?") (check-db-reverse-schema conn))
  (assert-db (keyword type "tx-count?") (check-tx-count conn)))

(defn read-db [{:keys [version target-folder]
                :or {version "development"}
                :as opts}]
  (t/set-level! :info)
  (t/info "Opts" opts)
  (let [lib-version (or (dt/get-version 'io.replikativ/datahike) "development")
        target-folder (or target-folder (System/getProperty "java.io.tmpdir"))
        config (default-conn-cfg target-folder version)
        date (Date.)
        results-file (format "%s/%s" target-folder (format "%tY%tm%td%tH%tM%tS_%s_%s.edn" date date date date date date lib-version version))]
    (swap! conn-results assoc :datahike.lib/version lib-version)
    (swap! conn-results assoc :datahike.db/version version)
    (swap! conn-results assoc ::date date)
    (try
      (let [conn (d/connect config)]
        (test-db conn "datahike.compat"))
      (catch Exception _
        (t/error "Connection failed.")))
    (try
      (let [import-cfg (-> config
                           (update-in [:store :path] str (str "_imported_" lib-version))
                           (dissoc :initial-tx))
            _ (d/delete-database import-cfg)
            import-file (format "%s/datahike_%s.nippy" target-folder version)
            _ (t/info (format "Importing data from %s to %s" import-file import-cfg))
            _ (w/migrate {:wanderung/type :nippy
                          :filename import-file}
                         (merge import-cfg
                                {:wanderung/type :datahike}))
           ;; _ (dm/import-db conn import-file)
            conn (d/connect import-cfg)]
        (swap! conn-results assoc ::import? true)
        (test-db conn "datahike.import"))
      (catch Exception _
        (t/error "Import failed.")))
    (t/info "Results: " results-file)
    (spit results-file @conn-results)))

(comment

  (write-db)
  (def target-folder (System/getProperty "java.io.tmpdir"))
  (def version (or (dt/get-version 'io.replikativ/datahike) "development"))
  (def config (default-cfg target-folder version))

  (def conn (d/connect config))

  (read-db {:version "0.4.0"
            :target-folder "/home/konrad/data/datahike/compat"} )


  (w/migrate {:wanderung/type :nippy
              :filename "/home/konrad/data/datahike/compat/datahike_0.4.0.nippy"}
             {:wanderung/type :datahike
              :store {:backend :file
                      :path "/tmp/0.4.0_to_latest"}
              :name "0.4.0-latest"
              :schema-flexibility :write
              :keep-history? true})

  (def config {:wanderung/type :datahike
               :store {:backend :file
                       :path "/tmp/0.4.0_to_latest"}
               :name "0.4.0-latest"
               :schema-flexibility :write
               :keep-history? true})

  (def conn (d/connect config))

  @conn

  (test-db conn "datahike.import")

  (dm/import-db conn "/home/konrad/data/datahike/compat/datahike_0.4.0.nippy")

  )

