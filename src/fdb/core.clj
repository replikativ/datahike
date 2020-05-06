(ns fdb.core
  (:import (com.apple.foundationdb FDB
                                   Transaction
                                   Range
                                   KeySelector)
           (java.util List))
  (:require [fdb.keys :refer [key ->byteArr key->vect]]))

(defmacro tr!
  "Transaction macro to perform actions. Always use tr for actions inside
  each action since the transaction variable is bound to tr in the functions."
  [db & actions]
  `(.run ~db
         (reify
           java.util.function.Function
           (apply [this tr]
             ~@actions))))


(defn db
  []
  (let [fd (FDB/selectAPIVersion 510)]
    (with-open [db (.open fd)]
      db)))


;; TODO: Currently there is only one and only one instance of FDB ever created.
;; I.e. I can't create multiple instances of the db with giving each a name for instance.
(defn empty-db
  "Clear all keys from the database. Thus returns an empty db."
  []
  (let [fd    (FDB/selectAPIVersion 510)
        begin (byte-array [])
        end   (byte-array [0xFF])]
    (with-open [db (.open fd)]
      (tr! db (.clear tr (Range. begin end)))
      db)))


(defn clear-all []
  (empty-db))


;; TODO?: remove the db arg
(defn get
  [db index-type [e a v t]]
  (let [fd  (FDB/selectAPIVersion 510)
        key (key index-type [e a v t])]
    (with-open [db (.open fd)]
      (tr! db @(.get tr key)))))


(defn insert
  "Inserts one vector"
  [index-type [e a v t]]
  (let [fd    (FDB/selectAPIVersion 510)
        key   (key index-type [e a v t])
        ;; Putting the key also in the value
        value key]
    (with-open [db (.open fd)]
      (tr! db (.set tr key value))
      db)))


(defn batch-insert
  "Batch inserts multiple vectors. `index-type` is :eavt, etc..."
  [index-type vectors]
  (let [fd   (FDB/selectAPIVersion 510)
        keys (map #(key index-type %) vectors)
        v    (byte-array [])]
    (with-open [db (.open fd)]
      ;; The value 5000 depends on the size of a fdb key.
      ;; I.e. We have to find a combination such that ~
      ;; 5000 * <fdb key size> does not exceed 10MB (the transaction max size
      ;; for fdb).
      (doall (doseq [some_vecs (partition 5000 keys)]
               (tr! db (doseq [k some_vecs]
                         ;; (println k)
                         (.set tr k v))))))))


(defn- get-range-as-byte-array
  "Returns fdb keys in the range [begin end] as a collection of byte-arrays. `begin` and `end` are vectors.
  index-type is `:eavt`, `:aevt` and `:avet`"
  [index-type begin end]
  (let [fd        (FDB/selectAPIVersion 510)
        ;; TODO: on the console: put (datom 124 :likes "Hans" 1 true) in an empty db and then
        ;; do (KeySelector/firstGreaterOrEqual (key :eavt [124 nil nil nil]))
        ;; What does it return?
        ;;
        begin-key (KeySelector/firstGreaterOrEqual (key index-type begin))
        end-key (KeySelector/firstGreaterThan (key index-type end))
        #_(if (= begin end)
            ;; TODO: this is just a hack to force it to return something
            ;; when begin = end. Coz in this case getRange returns empty
            (KeySelector/firstGreaterOrEqual (byte-array [0xFF]))
            ;; 'GreaterThan' makes sure that `end` is in the returned interval
            (KeySelector/firstGreaterThan (key index-type end)))]
    (with-open [db (.open fd)]
      (tr! db
           (mapv #(.getKey %)
                 (.getRange tr begin-key end-key))))))


(defn get-range
  "Returns fdb keys in the range [begin end]. `begin` and `end` are vectors.
  index-type is `:eavt`, `:aevt` and `:avet`"
  [index-type begin end]
  (map (partial key->vect index-type) (get-range-as-byte-array index-type begin end)))

;;------------ KeySelectors and iterations

(defn get-key
  "Returns the key behind a key-selector"
  [key-selector]
  (let [fd (FDB/selectAPIVersion 510)]
    (with-open [db (.open fd)]
      (tr! db
           @(.getKey tr key-selector)))))

;; NOTE: Works but not used. Using range instead as it should be faster.
(defn iterate-from
  "Lazily iterates through the keys starting from `begin` (a key in fdb format)"
  [index-type begin]
  (let [key-selector (KeySelector/firstGreaterOrEqual (key index-type begin))
        key      (get-key key-selector)
        next-key (get-key (.add key-selector 1))]
    (when-not (= (seq key) (seq next-key)) ;; seq makes [B comparable
      (lazy-seq (cons key (iterate-from index-type next-key))))))


;;;;;;;;;;; Debug HELPER

;; ;; debug
;; (defn bArr
;;   [i]
;;   (let [arr (byte-array 1)]
;;     (aset-byte arr 0 i)
;;     arr))

;; (defn insert-int
;;   [db i]
;;   (let [fd    (FDB/selectAPIVersion 510)
;;         key   (bArr i)
;;         ;; Putting the key also in the value
;;         value key]
;;     (with-open [db (.open fd)]
;;       (tr! db (.set tr key value))
;;       db)))


;; (defn get-range-int
;;   [db begin end]
;;   (let [fd        (FDB/selectAPIVersion 510)
;;         begin-key (bArr begin)
;;         end-key   (bArr end)]
;;    (with-open [db (.open fd)]
;;      (tr! db ;;(.getRange tr (Range. (bArr 1) (bArr 2)))
;;           (.getRange tr begin-key end-key)
;;           ))))
