(ns datahike.value-types
  "EXPERIMENTAL — a registry seam for custom `:db/valueType`s, so a value type can
   be added WITHOUT editing datahike core each time.

   A VALUE TYPE IS SOMETHING THAT SORTS. Read this before registering one.

   Datahike's AVET order IS the value's `compareTo` — `datahike.datom/compare-value`
   ends in `(compare v1 v2)`. That is what makes a value FINDABLE: seekable, joinable,
   range-queryable. It is the whole reason to put data in this database rather than a
   document store.

   So if your value has no meaningful total order, it is NOT a value type:

     - bytes with no queryable structure (a PDF, an image, model weights)
         -> `:db.type/store-ref` — name it, store it, let GC track it.
     - a document you would ever want to filter or JOIN on
         -> transact it as DATOMS (`datahike.experimental.unstructured`), or index it
            with a secondary index (`:db.secondary/*`).

   Datoms are already sparse — an entity has whatever attributes it has, and you never
   declared your fields. The schema only pins TYPES, cardinality, uniqueness, indexing.
   So storing a document as an opaque value buys you no flexibility you did not already
   have; it only costs you the indices. (Every database in this family agrees: XTDB
   indexes every field automatically and offers no opaque option at all; Datalevin's
   nippy escape hatch explicitly gives up range queries.)

   Good value types: an RDF literal, a geo point, money, a tensor with a defined
   comparison. They all sort.

   What a custom value type supplies:

     :pred            (fn [v] boolean)           — value validation
     :describe        string                     — for error messages
     :reachable-keys  (fn [v store] #{keys})     — see below; OMIT if the value
                                                   names no storage
     :fressian-read   {tag ReadHandler}          — durable store (PSS) decode
     :fressian-write  {Class {tag WriteHandler}} — durable store (PSS) encode
     :transit-read    {tag read-handler}         — remote/HTTP decode (optional)
     :transit-write   {Class write-handler}      — remote/HTTP encode (optional)
     :edn-tag/:edn-reader                        — pr/read (optional)

   ORDER is carried by the value implementing `Comparable` (JVM) / participating
   in `cljs.core/compare` — no comparator registration; its `compareTo` IS the
   on-disk AVET order.

   :reachable-keys — THE GARBAGE-COLLECTION CONTRACT.

   A value may NAME AN OBJECT IN THE STORE — a blob, an out-of-line payload, a
   document. Datahike's collector marks from the branch heads through the index
   trees; it does not look INSIDE datom values. So an object named only by a
   value is unreachable, and the sweep deletes it.

   `:reachable-keys` is how a type tells the collector otherwise: given a value
   (and the store, for types whose objects reference further objects), return the
   konserve keys that must be kept alive. `datahike.gc` unions them into the mark.

   Omitting it ASSERTS THAT YOUR VALUES NAME NO STORAGE. If that assertion is
   wrong, GC will delete live data — silently. There is no way for datahike to
   check this for you, so: if a value can hold a konserve key, declare it.

   The rule this establishes, and the one to teach: THE DATABASE IS THE ROOT SET.
   An object in the store lives iff a datom points at it. Anything else is
   garbage by definition — including anything you write into the store yourself
   and never reference.

   (Writing an object and referencing it in a LATER transaction leaves a window in
   which nothing names it. Hold `datahike.gc-guard/with-unreferenced-writes`
   across the two, or the collector may take it in between.)

   CROSS-PEER CONTRACT: the `:db/valueType` keyword persists in the stored schema
   and travels to every peer for free — the impl does NOT. Every peer that opens a
   store using a custom value type MUST have registered the same impl (ideally by
   depending on ONE canonical namespace, since the comparator defines the on-disk
   B-tree order). `assert-registered!` fails loudly at connect if a schema names an
   unregistered custom type — which is also what stops a peer that cannot interpret
   a type from collecting its objects away."
  (:require [clojure.set :as set]))

(defonce ^{:doc "valueType-keyword -> impl-map"} registry (atom {}))

(defn register!
  "Register a custom value type. Idempotent per keyword (last write wins)."
  [value-type impl-map]
  (swap! registry assoc value-type impl-map))

(defn registered? [value-type] (contains? @registry value-type))

(defn impl [value-type] (get @registry value-type))

(defn pred [value-type] (:pred (get @registry value-type)))

;; --------------------------------------------------------------------------
;; GC

(defn reachable-keys-fn
  "The `:reachable-keys` fn of `value-type`, or nil if it declares none (i.e. it
   asserts its values name no storage)."
  [value-type]
  (:reachable-keys (get @registry value-type)))

(defn key-bearing-types
  "Registered value types whose values may name objects in the store. The GC mark
   scans the datoms of attributes declared with one of these — and ONLY those, so
   a database that uses none pays nothing."
  []
  (into #{} (keep (fn [[vt impl]] (when (:reachable-keys impl) vt))) @registry))

;; --------------------------------------------------------------------------
;; Codecs

(defn fressian-read-handlers
  "Merged {tag ReadHandler} across all registered types."
  []
  (into {} (mapcat :fressian-read) (vals @registry)))

(defn fressian-write-handlers
  "Merged {Class {tag WriteHandler}} across all registered types."
  []
  (into {} (mapcat :fressian-write) (vals @registry)))

(defn transit-read-handlers []
  (into {} (mapcat :transit-read) (vals @registry)))

(defn transit-write-handlers []
  (into {} (mapcat :transit-write) (vals @registry)))

(defn edn-readers
  "Merged {tag reader-fn} across all registered types, for EDN `read-string`."
  []
  (into {} (keep (fn [{:keys [edn-tag edn-reader]}]
                   (when (and edn-tag edn-reader) [edn-tag edn-reader])))
        (vals @registry)))

(defn assert-registered!
  "Fail loudly at connect if `schema` declares a custom `:db/valueType` that no
   impl is registered for — turning a deep, late deserialization crash into an
   early, actionable error. `known-builtin` is the set of core valueTypes.

   This is also a GC safety property: a peer that cannot interpret a value type
   cannot connect, and therefore cannot sweep away the objects that type names."
  [schema known-builtin]
  (let [declared (into #{} (keep :db/valueType) (vals schema))
        custom   (set/difference declared known-builtin)
        missing  (remove registered? custom)]
    (when (seq missing)
      (throw (ex-info (str "Schema uses custom :db/valueType(s) with no registered impl: "
                           (vec missing) ". Load the namespace that registers them before connecting.")
                      {:error :value-type/unregistered :missing (vec missing)})))))
