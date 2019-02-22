(ns fdb.core
  (:import (com.apple.foundationdb FDB
                                   Transaction
                                   Range
                                   KeySelector)
           (java.util List))
  (:require [fdb.keys :refer [->byteArr]]))

(defmacro tr!
  "Transaction macro to perform actions. Always use tr for actions inside
  each action since the transaction variable is bound to tr in the functions."
  [db & actions]
  `(.run ~db
         (reify
           java.util.function.Function
           (apply [this tr]
             ~@actions))))

(defn empty-db
  []
  (let [fd (FDB/selectAPIVersion 510)]
    (with-open [db (.open fd)]
      db)))


;; TODO: [v] is converted to a String for now
;; TODO: move to fdb.keys
;; TODO: rename into binary-key may be
(defn eavt-key
  "Converts a datom into a fdb key"
  ;; Can take ^Datom object as input (as they are array)
  [[e a v t]]
  (->byteArr [e a (str v) t]))

(defn clear-all
  "Clear all  keys from the database"
  []
  (let [fd    (FDB/selectAPIVersion 510)
        begin (byte-array [])
        end   (byte-array [0xFF])]
    (with-open [db (.open fd)]
      (tr! db (.clear tr (Range. begin end))))))

;; TODO?: remove the db arg
(defn get
  [db [e a v t]]
  (let [fd  (FDB/selectAPIVersion 510)
        key (eavt-key [e a v t])]
    (with-open [db (.open fd)]
      (tr! db @(.get tr key)))))


(defn insert
  "Inserts one vector"
  [[e a v t]]
  (let [fd    (FDB/selectAPIVersion 510)
        key   (eavt-key [e a v t])
        ;; Putting the key also in the value
        value key]
    (with-open [db (.open fd)]
      (tr! db (.set tr key value))
      db)))


(defn batch-insert
  "Batch inserts multiple vectors"
  [vectors]
  (let [fd   (FDB/selectAPIVersion 510)
        keys (map #(fdb.core/eavt-key %) vectors)
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


(defn get-range
  "Returns keys in the range [begin end] (Keys are vector datoms)."
  [begin end]
  (let [fd        (FDB/selectAPIVersion 510)
        begin-key (KeySelector/firstGreaterOrEqual (eavt-key begin))
        end-key   (if (= begin end)
                    (.add (KeySelector/firstGreaterOrEqual (eavt-key end)) 1)
                    (KeySelector/firstGreaterThan (eavt-key end)))]
    (with-open [db (.open fd)]
      (tr! db
           (mapv #(.getKey %)
                 (.getRange tr begin-key end-key))))))

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
  "Lazily iterates through the keys starting from key (in fdb format)"
  [key]
  (let [ks       (KeySelector/firstGreaterOrEqual key)
        key      (get-key ks)
        next-key (get-key (.add ks 1))]
    (when-not (= (seq key) (seq next-key)) ;; seq makes [B comparable
      (lazy-seq (cons key (iterate-from next-key))))))


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
