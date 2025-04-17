(ns datahike.experimental.unstructured
  "Experimental feature for unstructured data input through schema inference.
   This namespace provides functions to convert unstructured EDN/JSON-like data 
   into Datahike's transaction format with automatic schema generation."
  (:require
   [datahike.api :as d]
   [datahike.db.interface :as dbi]))

(defn value->type
  "Determine the Datahike valueType from a value."
  [v]
  (cond
    (int? v) :db.type/long
    (float? v) :db.type/float
    (double? v) :db.type/double
    (number? v) :db.type/number
    (string? v) :db.type/string
    (boolean? v) :db.type/boolean
    (keyword? v) :db.type/keyword
    (symbol? v) :db.type/symbol
    (uuid? v) :db.type/uuid
    (inst? v) :db.type/instant
    (map? v) :db.type/ref
    (bytes? v) :db.type/bytes
    ;; Handle nil values - default to string for now
    (nil? v) :db.type/string
    :else :db.type/string))

(defn infer-value-schema
  "Create a schema entry for a given attribute and value."
  [attr v]
  (cond
    (vector? v)
    (when (seq v)
      {:db/ident attr
       :db/valueType (value->type (first v))
       :db/cardinality :db.cardinality/many})

    (map? v)
    {:db/ident attr
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one}

    :else
    {:db/ident attr
     :db/valueType (value->type v)
     :db/cardinality :db.cardinality/one}))

(defn process-unstructured-data
  "Process unstructured data recursively, converting it to Datahike transactions,
   while inferring schema. Returns a map with :schema and :tx-data."
  [data]
  (let [res (atom [])
        temp-id (atom 0)
        schema (atom [])]
    ((fn eval-data [data]
       (cond
         (map? data)
         (let [new-map (into {} (map (fn [[k v]]
                                       (when-let [schema-entry (infer-value-schema k v)]
                                         (swap! schema conj schema-entry))
                                       [k (eval-data v)])
                                     data))
               map-id (swap! temp-id dec)
               new-map (assoc new-map :db/id map-id)]
           (swap! res conj new-map)
           map-id)

         (vector? data)
         (mapv eval-data data)

         :else
         data))
     data)
    {:schema (vec (distinct (remove nil? @schema)))
     :tx-data @res}))

(defn check-schema-compatibility
  "Check if the inferred schema is compatible with the database's existing schema.
   Returns a map with :compatible? and :conflicts."
  [db inferred-schema]
  (let [db-schema (dbi/-schema db)
        conflicts (atom [])]
    (doseq [{:keys [db/ident db/valueType db/cardinality] :as attr-schema} inferred-schema]
      (when-let [existing-schema (get db-schema ident)]
        (when (not= (:db/valueType existing-schema) valueType)
          (swap! conflicts conj {:attr ident
                                 :conflict :value-type
                                 :existing (:db/valueType existing-schema)
                                 :inferred valueType}))
        (when (not= (:db/cardinality existing-schema) cardinality)
          (swap! conflicts conj {:attr ident
                                 :conflict :cardinality
                                 :existing (:db/cardinality existing-schema)
                                 :inferred cardinality}))))
    {:compatible? (empty? @conflicts)
     :conflicts @conflicts}))

(defn prepare-transaction
  "Prepare a transaction for unstructured data based on the database configuration.
   For schema-on-read databases, simply returns the transaction data.
   For schema-on-write databases, adds necessary schema definitions first."
  [db data]
  (let [result (process-unstructured-data data)
        schema-flexibility (get-in (dbi/-config db) [:schema-flexibility])]
    (if (= schema-flexibility :read)
      ;; For schema-on-read, just return the transaction data
      (:tx-data result)
      ;; For schema-on-write, we need to check compatibility and add schema
      (let [compatibility (check-schema-compatibility db (:schema result))]
        (if (:compatible? compatibility)
          ;; Add schema first, then data
          (concat (:schema result) (:tx-data result))
          ;; Not compatible - throw an error with details
          (throw (ex-info "Schema conflict detected with existing database schema"
                          {:conflicts (:conflicts compatibility)
                           :inferred-schema (:schema result)})))))))

(defn transact-unstructured
  "Public API for transacting unstructured data. Takes a connection and unstructured data.
   Automatically determines required schema and applies it before inserting the data."
  [conn data]
  (let [db (deref conn)
        tx-data (prepare-transaction db data)]
    ;; Use the standard transaction API
    (d/transact conn tx-data)))