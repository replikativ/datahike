(ns datahike.index.interface
  "All the functions in this namespace must be implemented for each index type"
  #?(:cljs (:refer-clojure :exclude [-seq -count -persistent! -flush])))

(defprotocol IIndex
  (-all [index] "Returns a sequence of all datoms in the index")
  (-seq [index] "Returns a sequence of all datoms in the index")
  (-count [index] "Returns the number of datoms in the index")
  (-insert [index datom index-type op-count] "Inserts a datom into the index")
  (-temporal-insert [index datom index-type op-count] "Inserts a datom in a history index")
  (-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom into the index")
  (-temporal-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom in a history index")
  (-remove [index datom index-type op-count] "Removes a datom from the index")
  (-slice [index from to index-type] "Returns a slice of the index")
  (-flush [index backend] "Saves the changes to the index to the given konserve backend")
  (-transient [index] "Returns a transient version of the index")
  (-persistent! [index] "Returns a persistent version of the index")
  (-mark [index] "Return konserve addresses that should be whitelisted for mark and sweep gc."))

(defmulti empty-index
  "Creates an empty index"
  (fn [index-name _store _index-type _index-config]
    index-name))

(defmulti init-index
  "Creates an index with datoms"
  (fn [index-name _store _datoms _index-type _op-count _index-config]
    index-name))

(defmulti add-konserve-handlers
  "Adds read and write handlers for the index data types."
  (fn [config _store] (:index config)))

(defmulti konserve-backend
  "Returns a konserve store capable of handling the index. Used for flushing."
  (fn [index-name _store] index-name))

(defmulti default-index-config
  "Returns the default index configuration."
  (fn [index-name] index-name))
