(ns datahike.experimental.unstructured
  "Experimental feature for unstructured data input through schema inference.
   This namespace provides functions to convert unstructured EDN/JSON-like data
   into Datahike's transaction format with automatic schema generation.

   IDENTITY OF NESTED OBJECTS. By default (`:identity :fresh`) every nested map
   becomes its OWN entity — two occurrences of the same object are two entities, so
   there is no structural sharing. That is safe but limited.

   `:identity :content` gives each nested map a CONTENT id (a recursive `hasch` hash
   of the subtree), stored under a `:db.unique/identity` attribute, so structurally
   identical objects collapse to ONE shared entity — sharing appears in the datom
   graph exactly as it does in the store for index nodes and blobs. This is the same
   content-addressing principle one level up: change the content and the id changes,
   so you can never alias-and-mutate someone else's value object (VALUE semantics, by
   construction).

   WHEN TO USE WHICH. Content identity is correct for VALUE objects (an address, a geo
   point, money) and merges COINCIDENTALLY-identical maps — which is wrong for
   ENTITIES that merely happen to share every field. JSON cannot tell the two apart
   (this is the JSON-LD blank-node / \"identity is declared, never inferred\" point),
   so it is a deliberate choice: for entity/record semantics, give the map a natural
   key — an attribute you declare `:db.unique/identity` — and datahike's ordinary
   upsert dedups it under either mode, no content id needed.

   Note: under `:write` flexibility this generates `:db.type/string` schema for
   inferred string fields, so string values are subject to the per-database
   value-size default (`:max-string-length`, 4096 by default) — ingesting a
   larger text field is rejected with `:transact/max-length`. Store large
   payloads off-database as a pointer, use `:db.secondary/only`, or disable the
   cap (per attribute `:db/maxLength 0`, or `:max-string-length 0` at creation)."
  (:require
   [datahike.api :as d]
   [datahike.db.interface :as dbi]
   [hasch.core :as hasch]))

(def ^:const default-id-attr :unstructured/id)

(defn content-id-schema
  "Schema for the content-identity attribute (`:identity :content`)."
  [id-attr]
  {:db/ident id-attr :db/valueType :db.type/uuid
   :db/unique :db.unique/identity :db/cardinality :db.cardinality/one})

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
   while inferring schema. Returns a map with :schema and :tx-data.

   Options:
     :identity  :fresh (default) — every nested map is its own entity (fresh tempid).
                :content         — nested maps get a recursive content id (`hasch`),
                                   so structurally identical objects share one entity.
     :id-attr   the :db.unique/identity attribute the content id is stored under
                (default :unstructured/id). Only used under :identity :content."
  ([data] (process-unstructured-data data {}))
  ([data {:keys [identity id-attr] :or {identity :fresh id-attr default-id-attr}}]
   (let [res     (atom [])
         temp-id (atom 0)
         schema  (atom [])
         ;; content-id -> tempid, so identical subtrees within a tx share a tempid
         ;; (and :id-attr's uniqueness dedups them across txs). Emit each once.
         seen    (atom {})]
     ((fn eval-data [data]
        (cond
          (map? data)
          (let [new-map (into {} (map (fn [[k v]]
                                        (when-let [schema-entry (infer-value-schema k v)]
                                          (swap! schema conj schema-entry))
                                        [k (eval-data v)])
                                      data))]
            (if (= identity :content)
              ;; Content id over the RAW subtree — hasch recurses, so this is the
              ;; Merkle id of (this map's scalars + its children's content). Identical
              ;; subtrees anywhere ⇒ identical id ⇒ one shared entity.
              (let [cid (hasch/uuid data)]
                (if-let [tid (get @seen cid)]
                  tid
                  (let [tid (swap! temp-id dec)]
                    (swap! seen assoc cid tid)
                    (swap! res conj (assoc new-map :db/id tid id-attr cid))
                    tid)))
              (let [map-id (swap! temp-id dec)]
                (swap! res conj (assoc new-map :db/id map-id))
                map-id)))

          (vector? data)
          (mapv eval-data data)

          :else
          data))
      data)
     {:schema (cond-> (vec (distinct (remove nil? @schema)))
                (= identity :content) (conj (content-id-schema id-attr)))
      :tx-data @res})))

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
   For schema-on-read databases, returns the transaction data (plus the content-id
   attribute's schema when `:identity :content`, since uniqueness needs a schema
   entry even under schema-on-read). For schema-on-write databases, adds all inferred
   schema definitions first. See `process-unstructured-data` for options."
  ([db data] (prepare-transaction db data {}))
  ([db data {:keys [identity id-attr] :or {identity :fresh id-attr default-id-attr} :as opts}]
   (let [result (process-unstructured-data data opts)
         schema-flexibility (get-in (dbi/-config db) [:schema-flexibility])]
     (if (= schema-flexibility :read)
       ;; Schema-on-read: no type schema needed — but a unique-identity attribute
       ;; still must be declared for upsert to dedup, so include just that.
       (cond->> (:tx-data result)
         (= identity :content) (concat [(content-id-schema id-attr)]))
       ;; Schema-on-write: check compatibility and add all inferred schema.
       (let [compatibility (check-schema-compatibility db (:schema result))]
         (if (:compatible? compatibility)
           (concat (:schema result) (:tx-data result))
           (throw (ex-info "Schema conflict detected with existing database schema"
                           {:conflicts (:conflicts compatibility)
                            :inferred-schema (:schema result)}))))))))

(defn transact-unstructured
  "Public API for transacting unstructured data. Takes a connection, unstructured
   data, and optional opts (see `process-unstructured-data`). Automatically infers
   and applies the required schema before inserting.

   Pass `{:identity :content}` to share structurally identical nested objects as one
   entity instead of duplicating them."
  ([conn data] (transact-unstructured conn data {}))
  ([conn data opts]
   (let [db (deref conn)
         tx-data (prepare-transaction db data opts)]
     (d/transact conn tx-data))))