(ns clj-foundationdb.utils
  (:import (com.apple.foundationdb.tuple Tuple)
           (com.apple.foundationdb FDB)))

;; https://github.com/vedang/clj_fdb/blob/a52d1e665cc52d83e6370c9d7be4ed040be5b6ec/src/clj_fdb/FDB.clj#L4

(defn select-api-version
  "
  Select the version for the client API.
  "
  [^Integer version]
  (FDB/selectAPIVersion version))

(defn open
  "
  Initializes networking, connects with the default fdb.cluster file,
  and opens the database.
  "
  [^FDB db]
  (.open db))

(defn bytes-to-str
  "
  Convert bytes to string
  "
  [bytes]
  (apply str (map char bytes)))

(defn key->tuple
  "
  Return tuple encoding for the given key
  "
  [key]
  (let [key (if (sequential? key) key [key])]
    (Tuple/from (to-array key))))

(defn key->packed-tuple
  "
  Pack the key with respect to Tuple encoding
  "
  [key]
  (.pack (key->tuple key)))

(defn bytes->key
  "
  Get the key from the KeyValue object. Since the keys might be nested use .getItems.
  Since all the keys might not be encoded with Tuple layer use stringification for those cases
  "
  [bytes]
  (let [key (.getKey bytes)]
    (try
      (vec (.getItems (Tuple/fromBytes key)))
      (catch IllegalArgumentException e (bytes-to-str key)))))

(defn bytes->value
  "
  Get the value from the KeyValue object.
  Since all the values might not be encoded with Tuple layer use stringification for those cases
  "
  [bytes]
  (let [value (.getValue bytes)]
    (try
      (.get (Tuple/fromBytes value) 0)
      (catch IllegalArgumentException e (bytes-to-str value)))))

(defn range->kv
  "
  Convert range object into a vector of key value pairs.
  "
  [range-query]
  (->> range-query
       (mapv #(vector
               (bytes->key %1)
               (bytes->value %1)))))
