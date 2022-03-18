(ns datahike.index.interface
  "All the functions in this namespace must be implemented for each index type")

(defprotocol IIndex
  (-all [index] "Returns a sequence of all datoms in the index")
  (-seq [index] "Returns a sequence of all datoms in the index")
  (-count [index] "Returns the number of datoms in the index")
  (-insert [index datom index-type op-count] "Inserts a datom into the index")
  (-temporal-insert [index datom index-type op-count] "Inserts a datom in a history index")
  (-upsert [index datom index-type op-count] "Inserts or updates a datom into the index")
  (-temporal-upsert [index datom index-type op-count] "Inserts or updates a datom in a history index")
  (-remove [index datom index-type op-count] "Removes a datom from the index")
  (-slice [index from to index-type] "Returns a slice of the index")
  (-flush [index backend] "Saves the changes to the index to the given backend")
  (-transient [index] "Returns a transient version of the index")
  (-persistent! [index] "Returns a persistent version of the index"))

(defmulti empty-index
  "Creates an empty index"
  (fn [index _index-type _index-config]
    index))

(defmulti init-index
  "Creates an index with datoms"
  (fn [index _datoms _index-type _op-count _index-config]
    index))
