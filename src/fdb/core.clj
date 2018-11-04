(ns fdb.core
  (:import (com.apple.foundationdb FDB
                                   Transaction
                                   Range
                                   KeySelector)
           (java.util List))
  (:require [fdb.keys :refer [->byteArr datom]]))

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
(defn key
  "Converts a datom into a fdb key"
  ;; Can take ^Datom object as input (as they are array)
  [[e a v t]]
  (->byteArr [e (str a) (str v) t]))

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
        key (key [e a v t])]
    (with-open [db (.open fd)]
      (tr! db @(.get tr key)))))

(defn insert
  [[e a v t]]
  (let [fd    (FDB/selectAPIVersion 510)
        key   (key [e a v t])
        ;; Putting the key also in the value
        value key]
    (with-open [db (.open fd)]
      (tr! db (.set tr key value))
      db)))


(defn get-range
  "Returns keys in the range [begin end["
  [begin end]
  (let [fd        (FDB/selectAPIVersion 510)
        begin-key (key begin)
        end-key   (key end)]
    (with-open [db (.open fd)]
      (tr! db
           (mapv #(.getKey %)
                 (.getRange tr (Range. begin-key end-key)))))))

;;------------ KeySelectors and iterations

(defn get-key
  "Returns the key behind a key-selector"
  [key-selector]
  (let [fd (FDB/selectAPIVersion 510)]
    (with-open [db (.open fd)]
      (tr! db
           @(.getKey tr key-selector)))))


(defn iterate-from
  "Lazily iterates through the keys starting from key (in fdb format)"
  [key]
  (let [ks (KeySelector/lastLessOrEqual key)]
    ;; TODO: rename ks-key into datom or smthg meaning a real app key
    (when-let [key (get-key ks)]
        #_(lazy-seq (cons (clojure.lang.MapEntry. ks-key ks-key) (iterate-from (get-key (.add ks 1)))))
        (lazy-seq (cons #_(clojure.lang.MapEntry. dat dat)
                        key
                        (iterate-from (get-key (.add ks 1))))))))


;;;;;;;;;;; DEBUG HELPER

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
