(ns datahike.middleware
  (:require
    [datahike.core :as d]
    [datahike.db :as db :refer [-datoms -search init-db entid-strict empty-db]]
    #?(:cljs [datahike.db :refer [DB]]))
  #?(:clj
      (:import [datahike.db DB])))

;;;
;;; Metadata
;;;

(defn keep-meta-middleware
  "tx-middleware to keep any meta-data on the db-value after a transaction. Assumes all metadata is in {:map :format}."
  [transact]
  (fn [report txs]
    (let [{:as report :keys [db-after db-before]} (transact report txs)]
      (update-in
        report
        [:db-after]
        with-meta
        (into
          (or (meta db-before) {})
          (meta db-after))))))

(def keep-meta-meta
  {:datahike.db/tx-middleware keep-meta-middleware})

;;;
;;; Transactional Schema
;;;

(def bare-bones-schema
  {:db/ident {:db/unique :db.unique/identity}
   :db/unique {:db/valueType :db.type/ref}
   :db/valueType {:db/valueType :db.type/ref}
   :db/cardinality {:db/valueType :db.type/ref}})

(def enum-idents
  [{:db/ident :db.cardinality/many}
   {:db/ident :db.cardinality/one}

   {:db/ident :db.unique/identity}
   {:db/ident :db.unique/value}

   {:db/ident :db.type/keyword}
   {:db/ident :db.type/string}
   {:db/ident :db.type/boolean}
   {:db/ident :db.type/long}
   {:db/ident :db.type/bigint}
   {:db/ident :db.type/float}
   {:db/ident :db.type/double}
   {:db/ident :db.type/bigdec}
   {:db/ident :db.type/ref}
   {:db/ident :db.type/instant}
   {:db/ident :db.type/uuid}
   {:db/ident :db.type/uri}
   {:db/ident :db.type/bytes}

   {:db/ident :db.part/db}
   {:db/ident :db.part/user}
   {:db/ident :db.part/tx}])

(def schema-idents
  [{:db/ident :db/ident
    :db/valueType :db.type/keyword
    :db/unique :db.unique/identity}
   {:db/ident :db/cardinality
    :db/valueType :db.type/ref}
   {:db/ident :db/unique
    :db/valueType :db.type/ref}
   {:db/ident :db/valueType
    :db/valueType :db.type/ref}
   {:db/ident :db/doc
    :db/valueType :db.type/string}
   {:db/ident :db/index
    :db/valueType :db.type/boolean}
   {:db/ident :db/fulltext
    :db/valueType :db.type/boolean}
   {:db/ident :db/isComponent
    :db/valueType :db.type/boolean}
   {:db/ident :db/noHistory
    :db/valueType :db.type/boolean}

   {:db/ident :db.install/attribute
    :db/valueType :db.type/ref}
   {:db/ident :db.alter/attribute
    :db/valueType :db.type/ref}
   {:db/ident :db.install/partition
    :db/valueType :db.type/ref}])


(defn validate-schema-change [db-before db-after]
  ;; TODO: insert optimized version of alexandergunnarson validation from posh
  ;; ???: should we call from full databases or schema and datoms?
  )

(defn- ^DB replace-schema
  [db schema & {:as options :keys [validate?] :or {validate? true}}]
  ;; ???: Can we make more performant by only updating :avet datom set when :db/index becomes active, rather than doing an entire init-db?
;;   (prn "replacing-schema" schema)
  (let [db-after (init-db (-datoms db :eavt []) schema)]
    (when validate?
      (validate-schema-change db db-after))
    db-after))

(def schema-attrs #{:db/ident :db/cardinality :db/unique :db/index :db/isComponent :db/valueType})

(defn- schema-datom? [[e a v tx add?]]
  (schema-attrs a))

(defn- supported-schema-value? [a v]
  (case a
    :db/valueType (= v :db.type/ref)
    true))

(defn- resolve-ident [db ident-eid]
  ;; TODO: use existing function
  (let [resolved-eid (entid-strict db ident-eid)]
    (-> (-search db [resolved-eid :db/ident])
        first
        :v)))

(defn- resolve-enum [db attr value]
  ;; FIXME: hardcoded enums
  (if (#{:db/cardinality :db/unique :db/valueType} attr)
    (let [rident (resolve-ident db value)]
      rident)
    value))

(defn- conj-schema-datom
  ;; TODO: handle retractions
  ([] (empty-db))
  ([db] db)
  ([db [eid attr value _ _]]
   (let [attr-ident (resolve-ident db eid)
         resolved-value (resolve-enum db attr value)]
     (if (supported-schema-value? attr resolved-value)
       (assoc-in db [:schema attr-ident attr]
                 resolved-value)
       db))))

(defn schema-middleware
  "Takes schema transactions and puts them into the simplified schema map."
  [transact]
  (fn [report txs]
    (let [{:as report :keys [db-after tx-data]} (transact report txs)
          db-after' (transduce
                    (filter schema-datom?)
                    conj-schema-datom
                    db-after
                    tx-data)]
      (if (= (:schema db-after) (:schema db-after'))
        report
        (assoc report
          :db-after (replace-schema db-after (:schema db-after')))))))

(def schema-meta {:datahike.db/tx-middleware schema-middleware})

(defn schema-db []
  (-> (d/empty-db bare-bones-schema)
      (d/db-with enum-idents schema-meta)
      (d/db-with schema-idents schema-meta)))

(defn create-schema-conn
  "Creates a conn that has all the necessary base schema to be used with transactional schema. You should also use schema-meta whenever you use any of d/transact! d/with d/db-with"
  []
  (-> (d/conn-from-db (schema-db))))

;;;
;;; Combined Meta
;;;
(def kitchen-sink-meta {:datahike.db/tx-middleware (comp keep-meta-middleware schema-middleware)})
