(ns datahike.test.utils
  (:require [datahike.api :as d]
            [datahike.tools :as tools])
  #?(:clj (:import [java.util UUID Date])))

(defn get-all-datoms
  "Based on Wanderung function `wanderung.datahike/extract-datahike-data`."
  ([db] (get-all-datoms db identity))
  ([db final-xform]
   (let [txs (->> db
                  (d/q '[:find ?tx ?inst
                         :in $
                         :where
                         [?tx :db/txInstant ?inst]
                         [(< #inst "1970-01-02" ?inst)]])
                  (sort-by first))
         query {:query '[:find ?e ?a ?v ?t ?added
                         :in $ ?t
                         :where
                         [?e ?a ?v ?t ?added]
                         (not [?e :db/txInstant ?v ?t ?added])]
                :args [(d/history db)]}]
     (into []
           (comp (mapcat
                  (fn [[tid tinst]]
                    (->> (d/q (update-in query [:args] conj tid))
                         (sort-by first)
                         (into [[tid :db/txInstant tinst tid true]]))))
                 final-xform)
           txs))))

(defn unmap-tx-timestamp [[e a _ tx added :as datom]]
  (if (= a :db/txInstant)
    [e a :timestamp tx added]
    datom))

(defn cfg-template
  "Returning a config template with a random store-id"
  []
  {:store {:backend :mem}
   :keep-history? true
   :schema-flexibility :read})

(defn setup-db
  "Setting up a test-db in memory by default.  Deep-merges the passed config into the defaults."
  ([]
   (setup-db {}))
  ([cfg]
   (setup-db cfg (not (get-in cfg [:store :id]))))
  ([cfg gen-uuid?]
   (let [cfg (cond-> (tools/deep-merge (cfg-template) cfg)
               gen-uuid? (assoc-in [:store :id] (str (UUID/randomUUID))))]
     (d/delete-database cfg)
     (d/create-database cfg)
     (d/connect cfg))))

(defn all-true? [c] (every? true? c))

(defn all-eq? [c1 c2] (all-true? (map = c1 c2)))

(defn setup-default-db [config test-data]
  (let [_ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]
    (d/transact conn test-data)
    conn))

(defn teardown-db [conn]
  (d/release conn)
  (d/delete-database (:config @conn)))

(defn with-db
  "Test database fixture"
  ([config f]
   (with-db config [] f))
  ([config test-data f]
   (let [conn (setup-default-db config test-data)]
     (f)
     (teardown-db conn))))

(defn sleep [ms]
  #?(:clj (Thread/sleep ms)
     :cljs (js/setTimeout (fn []) ms)))

(defn get-time []
  #?(:clj (.getTime (Date.))
     :cljs (.getTime (js/Date.))))

(defn conn-id []
  (str (get-time)))
