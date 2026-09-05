(ns datahike.test.transaction-options-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.api.types :as types]
            [datahike.core :as core]
            [datahike.db.transaction :as dbt]
            [datahike.writing :as writing]
            [malli.core :as m]
            [malli.instrument :as mi]))

(def ^:private backfill {:allow-index-backfill? true})

(defn- with-db
  ([f] (with-db {} f))
  ([extra-config f]
   (let [config (merge {:store {:backend :memory :id (random-uuid)}
                        :schema-flexibility :write
                        :keep-history? true
                        :max-string-length 0
                        :allow-index-backfill? false}
                       extra-config)]
     (d/create-database config)
     (let [conn (d/connect config)]
       (try
         (d/transact conn
                     [{:db/ident :sample/id :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                      {:db/ident :sample/value :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :sample/other :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :sample/retired :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
         (f conn)
         (finally
           (d/release conn)
           (d/delete-database config)))))))

(defn- datom-set [db index & components]
  (into #{} (map (juxt :e :a :v :tx :added)) (apply d/datoms db index components)))

(defn- error-data [f]
  (try (f) nil
       (catch Exception e
         (loop [cause e]
           (if (or (:error (ex-data cause)) (nil? (ex-cause cause)))
             (ex-data cause)
             (recur (ex-cause cause)))))))

(deftest transaction-option-backfills-current-and-history-avet
  (doseq [attribute-refs? [false true]
          keep-history? [false true]]
    (testing (str "attribute refs " attribute-refs? ", history " keep-history?)
      (with-db
        {:attribute-refs? attribute-refs? :keep-history? keep-history?}
        (fn [conn]
          (d/transact conn [{:db/id 100 :sample/value "old" :sample/retired "gone"}
                            {:db/id 101 :sample/value "retained"}])
          (d/transact conn [[:db/add 100 :sample/value "new"]
                            [:db/retract 100 :sample/retired "gone"]])
          (let [before @conn
                attrs [:sample/value :sample/retired]
                current (into {} (map (fn [a] [a (datom-set before :aevt a)])) attrs)
                history (when keep-history?
                          (into {} (map (fn [a] [a (datom-set (d/history before) :aevt a)])) attrs))
                report (d/transact conn {:tx-data (mapv (fn [a] [:db/add a :db/index true]) attrs)
                                         :tx-options backfill})
                after (:db-after report)]
            (doseq [a attrs]
              (is (= (current a) (datom-set after :avet a)))
              (when keep-history?
                (is (= (history a) (datom-set (d/history after) :avet a)))))
            (is (= (:config before) (:config after)))
            (is (not (contains? report :tx-options)))
            (is (not (contains? (:tx-meta report) :tx-options)))
            (is (not (contains? after :tx-options)))
            (is (not (contains? (:meta after) :tx-options)))))))))

(deftest option-is-local-to-one-transaction
  (with-db
    (fn [conn]
      (d/transact conn [{:db/ident :allow-index-backfill? :db/valueType :db.type/boolean
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:sample/value "value" :sample/other "other"}])
      (d/transact conn {:tx-data [[:db/add :sample/value :db/index true]]
                        :tx-options backfill})
      (let [before @conn
            tx [[:db/add :sample/other :db/index true]]]
        (doseq [options [nil {} {:allow-index-backfill? false}]]
          (is (= :transact/schema
                 (:error (error-data #(d/with before tx nil options)))))
          (is (= :transact/schema
                 (:error (error-data #(d/transact conn {:tx-data tx :tx-options options}))))))
        (is (= {:error :transact/schema :attribute :sample/other}
               (select-keys (error-data #(d/transact conn {:tx-data tx :tx-meta backfill}))
                            [:error :attribute]))
            "transaction metadata is not an execution option")
        (is (= (:max-tx before) (:max-tx @conn)))
        (is (= (:config before) (:config @conn)))
        (is (not (get-in @conn [:schema :sample/other :db/index])))
        (is (d/transact conn {:tx-data tx :tx-options backfill}))))))

(deftest persisted-config-still-permits-backfill
  (with-db
    {:allow-index-backfill? true}
    (fn [conn]
      (d/transact conn [{:sample/value "value" :sample/other "other"}])
      (is (d/transact conn [[:db/add :sample/value :db/index true]]))
      (is (d/transact conn {:tx-data [[:db/add :sample/other :db/index true]]
                            :tx-options {:allow-index-backfill? false}}))
      (is (true? (get-in @conn [:config :allow-index-backfill?]))))))

(deftest immutable-and-writer-backfill-are-equivalent
  (with-db
    (fn [conn]
      (d/transact conn [{:db/id 100 :sample/value "old"}])
      (d/transact conn [[:db/add 100 :sample/value "new"]])
      (let [before @conn
            tx [[:db/add :sample/value :db/index true]
                [:db/add -1 :sample/value "added"]]
            tx-meta {:db/txInstant (java.util.Date. 2000000000000)}
            previews [(core/with before tx tx-meta backfill)
                      (d/with before tx tx-meta backfill)
                      (d/with before {:tx-data tx :tx-meta tx-meta :tx-options backfill})]
            committed @(d/transact! conn {:tx-data tx :tx-meta tx-meta :tx-options backfill})]
        (doseq [preview previews]
          (is (= (:tempids preview) (:tempids committed)))
          (is (= (:tx-data preview) (:tx-data committed)))
          (is (= (:tx-meta preview) (dissoc (:tx-meta committed) :db/commitId)))
          (is (= (:schema (:db-after preview)) (:schema (:db-after committed))))
          (is (= (:config before) (:config (:db-after preview))))
          (doseq [index [:eavt :aevt :avet]]
            (is (= (datom-set (:db-after preview) index)
                   (datom-set (:db-after committed) index)))
            (is (= (datom-set (d/history (:db-after preview)) index)
                   (datom-set (d/history (:db-after committed)) index)))))))))

(deftest retries-retain-transaction-options
  (doseq [upsert-tx [[[:db/add -1 :sample/other "updated"]
                      [:db/add -1 :sample/id "existing"]]
                     [{:db/id -1 :sample/other "updated"}
                      {:db/id -1 :sample/id "existing"}]]]
    (with-db
      (fn [conn]
        (d/transact conn [{:db/id 100 :sample/id "existing" :sample/value "value"}])
        (let [tx (conj upsert-tx [:db/add :sample/value :db/index true])
              original dbt/transact-tx-data
              calls (atom 0)]
          (with-redefs [dbt/transact-tx-data (fn [& args]
                                               (swap! calls inc)
                                               (apply original args))]
            (let [preview (d/with @conn tx nil backfill)]
              (is (= 2 @calls) "the tempid conflict really restarted the transaction")
              (is (= 100 (get (:tempids preview) -1)))
              (is (= "updated" (:sample/other (d/entity (:db-after preview) 100))))
              (is (= 1 (count (d/datoms (:db-after preview) :avet :sample/value)))))
            (reset! calls 0)
            (let [report (d/transact conn {:tx-data tx :tx-options backfill})]
              (is (= 2 @calls))
              (is (= 100 (get (:tempids report) -1)))
              (is (= 1 (count (d/datoms @conn :avet :sample/value))))))
          (is (false? (get-in @conn [:config :allow-index-backfill?]))))))))

(deftest unique-backfill-validates-existing-values-atomically
  (with-db
    (fn [conn]
      (d/transact conn [{:db/id 100 :sample/value "duplicate"}
                        {:db/id 101 :sample/value "duplicate"}])
      (let [before @conn
            tx [[:db/add :sample/value :db/unique :db.unique/identity]]]
        (is (= :transact/schema
               (:error (error-data #(d/with before tx nil backfill)))))
        (is (= :transact/schema
               (:error (error-data #(d/transact conn {:tx-data tx :tx-options backfill})))))
        (is (= (:max-tx before) (:max-tx @conn)))
        (is (= (:meta before) (:meta @conn)))
        (is (nil? (get-in @conn [:schema :sample/value :db/unique])))
        (d/transact conn [[:db/add 101 :sample/value "different"]])
        (is (d/transact conn {:tx-data tx :tx-options backfill}))
        (is (= 100 (:db/id (d/entity @conn [:sample/value "duplicate"]))))
        (is (= :transact/unique
               (:error (error-data #(d/transact conn [[:db/add 102 :sample/value "duplicate"]])))))))))

(deftest newly-unique-existing-index-is-validated-without-rebuilding
  (doseq [attribute-refs? [false true]
          unique [:db.unique/value :db.unique/identity]]
    (with-db
      {:attribute-refs? attribute-refs?}
      (fn [conn]
        (d/transact conn [[:db/add :sample/value :db/index true]])
        (d/transact conn [{:db/id 100 :sample/value "duplicate"}
                          {:db/id 101 :sample/value "duplicate"}])
        (let [before @conn
              tx [[:db/add :sample/value :db/unique unique]]]
          (is (= :transact/schema
                 (:error (error-data #(d/with before tx nil backfill)))))
          (is (= :transact/schema
                 (:error (error-data #(d/transact conn {:tx-data tx :tx-options backfill})))))
          (is (= (:max-tx before) (:max-tx @conn)))
          (is (= (:meta before) (:meta @conn)))
          (is (nil? (get-in @conn [:schema :sample/value :db/unique])))
          (d/transact conn [[:db/add 101 :sample/value "different"]])
          (let [before @conn
                baseline (d/with before [[:db/add :sample/value :db/doc "no backfill"]])
                preview (d/with before tx nil backfill)
                committed (d/transact conn {:tx-data tx :tx-options backfill})]
            (is (= (:op-count (:db-after baseline)) (:op-count (:db-after preview)))
                "adding uniqueness must not reinsert already indexed current/history datoms")
            (doseq [report [preview committed]]
              (is (= unique (get-in report [:db-after :schema :sample/value :db/unique])))
              (is (= (datom-set before :avet :sample/value)
                     (datom-set (:db-after report) :avet :sample/value)))
              (is (= (datom-set (d/history before) :avet :sample/value)
                     (datom-set (d/history (:db-after report)) :avet :sample/value))))))))))

(deftest invalid-options-are-rejected-at-every-entry-point
  (with-db
    (fn [conn]
      (let [before @conn
            invalid [true false 1 [] #{:allow-index-backfill?}
                     {:unknown true}
                     {:allow-index-backfill? true :unknown false}
                     {:allow-index-backfill? nil}
                     {:allow-index-backfill? 1}
                     {:allow-index-backfill? "true"}
                     {:allow-index-backfill? :yes}]]
        (doseq [options invalid]
          (testing (pr-str options)
            (doseq [call [#(core/with before [] nil options)
                          #(d/with before [] nil options)
                          #(d/with before {:tx-data [] :tx-options options})
                          #(d/transact conn {:tx-data [] :tx-options options})
                          #(d/transact! conn {:tx-data [] :tx-options options})
                          #(writing/transact! before {:tx-data [] :tx-options options})]]
              (is (= :transact/invalid-options (:error (error-data call)))))
            (is (not (m/validate types/STxOptions options)))))
        (doseq [options [nil {} {:allow-index-backfill? false} backfill]]
          (is (m/validate types/STxOptions options)))
        (is (= (:max-tx before) (:max-tx @conn)))))))

(deftest public-api-options-work-under-instrumentation
  (with-db
    (fn [conn]
      (d/transact conn [{:sample/value "value"}])
      (let [violations (atom [])
            tx [[:db/add :sample/value :db/index true]]]
        (mi/instrument! {:report (fn [type data]
                                   (swap! violations conj [type (:fn-name data)]))})
        (try
          (is (d/with @conn tx nil backfill))
          (is (d/with @conn {:tx-data tx :tx-options backfill}))
          (is (d/transact conn {:tx-data tx :tx-options backfill}))
          (is (empty? @violations))
          (finally (mi/unstrument!)))))))
