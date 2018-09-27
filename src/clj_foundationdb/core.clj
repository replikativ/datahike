(ns clj-foundationdb.core
  (:require [clojure.spec.alpha :as spec]
            [clj-foundationdb.utils :refer :all])
  (:import (com.apple.foundationdb Database
                                   FDB
                                   Range
                                   KeySelector
                                   Transaction)
           (com.apple.foundationdb.tuple Tuple)
           (com.apple.foundationdb.subspace Subspace)
           (java.util List)))

(def tr? #(instance? com.apple.foundationdb.Database %1))

;; Refer https://github.com/apple/foundationdb/blob/e0c8175f3ccad92c582a3e70e9bcae58fff53633/bindings/java/src/main/com/apple/foundationdb/tuple/TupleUtil.java#L171

(def serializable? #(or (number? %1)
                        (string? %1)
                        (decimal? %1)
                        (instance? List %1)))

(defmacro tr!
  "Transaction macro to perform actions. Always use tr for actions inside
  each action since the transaction variable is bound to tr in the functions.

  ```
  (let [fd    (select-api-version 520)
        key   \"foo\"
        value \"bar\"]
  (with-open [db (open fd)]
     (tr! db
          (set-val tr key value)
          (get-val tr key))))
  ```
  "
  [db & actions]
  `(.run ~db
         (reify
           java.util.function.Function
           (apply [this tr]
             ~@actions))))

(def ^:dynamic *subspace* nil)

(defn make-subspace
  "
  Returns a key with name as prefix

  ```
  (make-subspace [\"class\" \"intro\"] [\"algebra\"]) returns (\"class\" \"intro\" \"algebra\")
  ```
  "
  [prefix key]
  (flatten (map vector [prefix key])))

(defmacro with-subspace
  "
  Sets and gets the keys with the given subspace key prefixed.
  This essentially executes with code binding the given prefix to *subspace*.

  ```
  (let [fd    (select-api-version 520)
        key   \"foo\"
        value \"bar\"]
    (with-open [db (open fd)]
      (tr! db
           (clear-all tr)
           (with-subspace \"class\"
             (set-val tr key value)
             (get-val tr key))
            (nil? (get-val tr key)))))
  ```
  "
  [prefix & actions]
  `(binding [*subspace* ~prefix]
     ~@actions))

(defn get-val
  "Get the value for the given key. Accepts the below :

  :subspace - Subspace to be prefixed
  :coll     - Boolean to indicate if the value needs to be deserialized as collection

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db
          (get-val tr key))))

  (let [fd    (select-api-version 520)
       key   \"foo\"
       value [1 2 3]]
  (with-open [db (open fd)]
    (tr! db
         (set-val tr key value)
         (get-val tr key) ;; 1
         (get-val tr key :coll true)))) ;; [1 2 3]
  ```
  "
  [tr key & {:keys [subspace coll] :or {subspace *subspace* coll false}}]
  (let [key   (-> (if subspace (make-subspace subspace key) key)
                  key->packed-tuple)]
    (if-let [value @(.get tr key)]
      (if coll
        (.getItems (Tuple/fromBytes value))
        (.get (Tuple/fromBytes value) 0)))))

(spec/fdef get-val
           :args (spec/cat :tr tr? :key serializable?)
           :ret (spec/nilable serializable?))

(defn set-val
  "Set a value for the key. Accepts the below :

  :subspace - Subspace to be prefixed

  ```
  (let [fd    (select-api-version 520)
        key   \"foo\"
        value \"bar\"]
  (with-open [db (open fd)]
     (tr! db
          (set-val tr key value))))
  ```
  "
  [tr key value & {:keys [subspace] :or {subspace *subspace*}}]
  (let [key   (-> (if subspace (make-subspace subspace key) key)
                  key->packed-tuple)
        value (key->packed-tuple value)]
    (.set tr key value)))

(spec/fdef set-val
           :args (spec/cat :tr tr? :key serializable? :value serializable?)
           :ret (spec/nilable serializable?))

(defn set-keys
  "Set given keys with the value

  ```
  (let [fd    (select-api-version 520)
        keys  [\"foo\" \"baz\"]
        value \"bar\"]
  (with-open [db (open fd)]
     (tr! db (set-keys tr keys value))))
  ```
  "
  [tr keys value]
  (let [keys  (map #(key->packed-tuple %1) keys)
        value (key->packed-tuple value)]
    (doseq [key keys] (.set tr key value))))

(spec/fdef set-keys
           :args (spec/cat :tr tr? :key (spec/coll-of serializable?) :value serializable?))

(defn clear-key
  "Clear a key from the database

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db (clear-key tr key))))
  ```
  "
  [tr key]
  (let [key (key->packed-tuple key)]
    (.clear tr key)))

(spec/fdef clear-key
           :args (spec/cat :tr tr? :key serializable?))

(defn get-range-startswith
  "Get a range of key values as a vector that starts with prefix

  ```
  (let [fd     (select-api-version 520)
        prefix \"f\"]
  (with-open [db (open fd)]
     (tr! db (get-range-startswith tr key prefix))))
  ```
  "
  [tr prefix]
  (let [prefix      (key->packed-tuple prefix)
        range-query (Range/startsWith prefix)]
    (->> (.getRange tr range-query)
         range->kv)))

(spec/fdef get-range-startswith
           :args (spec/cat :tr tr? :prefix serializable?))

(defn watch
  "
  A key to watch and a callback function to be executed on change. It returns a future object
  that is realized when there is a change to key. Change in key value or clearing the key
  is noted as a change. A set statement with the old value is not a change.

  ```
  (let [fd    (select-api-version 520)
       key    \"foo\"
       value  \"bar\"]
  (with-open [db (open fd)]
    (tr! db
         (clear-key tr key)
         (watch tr key #(println \"key is set\"))
         (set-val tr key value)
         (watch tr key #(println \"key is changed to 1\"))
         (set-val tr key value) ;; Doesn't trigger watch
         (set-val tr key \"1\")
         (watch tr key #(println \"cleared key\"))
         (clear-key tr key))))
  ```
  key is set
  key is changed to 1
  cleared key

  "
  [tr key callback]
  (let [key   (key->packed-tuple key)
        watch (.watch tr key)]
    (future (do (.join watch)
                (callback)))))

(spec/fdef watch
           :args (spec/cat :tr tr? :key serializable? :callback ifn?))

(defn get-range
  "Get a range of key values as a vector

  ```
  (let [fd    (select-api-version 520)
        begin \"foo\"
        end   \"foo\"]
  (with-open [db (open fd)]
     (tr! db (get-range tr begin end))))
  ```
  "
  ([tr begin]
   (let [begin       (Tuple/from (to-array (if (sequential? begin) begin [begin])))
         range-query (.getRange tr (.range begin))]
     (range->kv range-query)))
  ([tr begin end]
   (let [begin       (key->packed-tuple begin)
         end         (key->packed-tuple end)
         range-query (.getRange tr (Range. begin end))]
     (range->kv range-query))))

(spec/fdef get-range
           :args (spec/cat :tr tr? :begin serializable? :end serializable?))

;; https://stackoverflow.com/a/21421524/2610955
;; Refer : https://forums.foundationdb.org/t/how-to-clear-all-keys-in-foundationdb-using-java/351/2

(defn get-all
  "Get all key values as a vector"
  [tr]
  (let [begin       (byte-array [])
        end         (byte-array [0xFF])
        range-query (.getRange tr (Range. begin end))]
    (range->kv range-query)))

(spec/fdef get-all
           :args (spec/cat :tr tr?))

(defn clear-range
  "Clear a range of keys from the database.
  When only begin is given then the keys with starting with the tuple are cleared.
  When begin and end are specified then end is exclusive of the range to be cleared.

  ```
  (let [fd    (select-api-version 520)
        begin \"foo\"]
  (with-open [db (open fd)]
     (tr! db (clear-range tr begin))))

  (let [fd    (select-api-version 520)
        begin \"foo\"
        end   \"foo\"]
  (with-open [db (open fd)]
     (tr! db (clear-range tr begin end))))
  ```
  "
  ([tr begin]
   (let [begin (key->tuple begin)]
     (.clear tr (.range begin))))
  ([tr begin end]
   (let [begin (key->packed-tuple begin)
         end   (key->packed-tuple end)]
     (.clear tr (Range. begin end)))))

(spec/fdef clear-range
           :args (spec/cat :tr tr? :begin serializable? :end serializable?))

;; https://stackoverflow.com/a/21421524/2610955
;; Refer : https://forums.foundationdb.org/t/how-to-clear-all-keys-in-foundationdb-using-java/351/2

(defn clear-all
  "Clear all  keys from the database"
  [tr]
  (let [begin (byte-array [])
        end   (byte-array [0xFF])]
    (.clear tr (Range. begin end))))

(spec/fdef clear-all
           :args (spec/cat :tr tr?))

(defn last-less-than
  "Returns key and value pairs with keys less than the given key for the given limit

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db (last-less-than tr key))))
  ```
  "
  ([tr key]
   (last-less-than tr key 1))
  ([tr key limit]
   (let [key         (KeySelector/lastLessThan (key->packed-tuple key))
         end         (.add key limit)
         range-query (.getRange tr key end)]
     (range->kv range-query))))

(spec/fdef last-less-than
           :args (spec/cat :tr tr? :key serializable? :limit (spec/? pos-int?))
           :ret (spec/coll-of (spec/tuple serializable? serializable?)))

(defn last-less-or-equal
  "Returns key and value pairs with keys less than or equal the given key for the given limit

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db (last-less-or-equal tr key))))
  ```
  "
  ([tr key]
   (last-less-or-equal tr key 1))
  ([tr key limit]
   (let [key         (KeySelector/lastLessOrEqual (key->packed-tuple key))
         end         (.add key limit)
         range-query (.getRange tr key end)]
     (range->kv range-query))))

(spec/fdef last-less-or-equal
           :args (spec/cat :tr tr? :key serializable? :limit (spec/? pos-int?))
           :ret (spec/coll-of (spec/tuple serializable? serializable?)))

(defn first-greater-than
  "Returns key and value pairs with keys greater than the given key for the given limit

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db (first-greater-than tr key))))
  ```
  "
  ([tr key]
   (first-greater-than tr key 1))
  ([tr key limit]
   (let [key         (KeySelector/firstGreaterThan (key->packed-tuple key))
         end         (.add key limit)
         range-query (.getRange tr key end)]
     (range->kv range-query))))

(spec/fdef first-greater-than
           :args (spec/cat :tr tr? :key serializable? :limit (spec/? pos-int?))
           :ret (spec/coll-of (spec/tuple serializable? serializable?)))

(defn first-greater-or-equal
  "Returns key and value pairs with keys greater than or equal to the given key for the given limit

  ```
  (let [fd  (select-api-version 520)
        key \"foo\"]
  (with-open [db (open fd)]
     (tr! db (first-greater-or-equal tr key))))
  ```
  "
  ([tr key]
   (first-greater-or-equal tr key 1))
  ([tr key limit]
   (let [key         (KeySelector/firstGreaterOrEqual (key->packed-tuple key))
         end         (.add key limit)
         range-query (.getRange tr key end)]
     (range->kv range-query))))

(spec/fdef first-greater-or-equal
           :args (spec/cat :tr tr? :key serializable? :limit (spec/? pos-int?))
           :ret (spec/coll-of (spec/tuple serializable? serializable?)))
