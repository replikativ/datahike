(ns fdb-playground.core
  (:import (com.apple.foundationdb FDB
                                   Transaction)
           (java.util List))
  (:require [fdb-playground.keys :refer [->byteArr]]
            [clj-foundationdb.utils :refer [open select-api-version]]))

;; =======================================================

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

;; =======================================================

(defn empty-fdb
  []
  (let [fd (select-api-version 510)]
    (with-open [db (.open fd)]
      db)))


(defn fdb-key
  "Converts a datom into a fdb key.
  Note the conversion of byte array into a string to fit the clj fdb library interface."
  [[e a v t]]
  (->byteArr [e (str a) (str v) t]))


(defn get
  [db [e a v t]]
  (let [fd  (select-api-version 510)
        key (fdb-key [e a v t])]
    (with-open [db (.open fd)]
      (tr! db @(.get tr key)))))

(defn fdb-insert
  [db [e a v t]]
  (let [fd    (select-api-version 510)
        key   (fdb-key [e a v t])
        ;; Putting the key also in the value
        value key]
    (with-open [db (.open fd)]
      (tr! db (.set tr key value))
      db)))





;; ;; TODO: finish specing it and implement
;; ;;(defn fdb-get)

;; ;; FoundationDB write datoms

;; ;; 10 "Elapsed time: 105.700624 msecs"
;; ;; 100 "Elapsed time: 126.062635 msecs"
;; ;; 1000 "Elapsed time: 480.132474 msecs"
;; ;; 10000 "Elapsed time: 3685.370752 msecs"
;; ;; 100000 "Elapsed time: 35680.397137 msecs"

;; (defn write-datoms []
;;   (let [fd (select-api-version 510)
;;         kv (map #(vector (fdb-key [%1 (str ":attribute/" %1) %1 %1])  %1)
;;                 (range 100000))]
;;     (time (let [clients (repeatedly 10 #(future
;;                                           (with-open [db (open fd)]
;;                                             (tr! db
;;                                                  (doall (doseq [[k v] kv]
;;                                                           ;;(set-val tr k v)
;;                                                           (fdb-insert k)
;;                                                           ))))))]
;;             (doall (map deref clients))
;;             "Finished"))))

;; ;;(write-datoms)

;; ;; "Elapsed time: 34156.643569 msecs"
;; ;; "Elapsed time: 34472.143205 msecs"

;; ;; FoundationDB read datoms

;; (defn read-datoms []
;;   (let [fd (select-api-version 510)
;;         ks (map #(vector (fdb-key [%1 (str ":attribute/" %1) %1 %1]))
;;                 (range 100000))]
;;     (time (with-open [db (open fd)]
;;             (let [clients (repeatedly 10 #(future
;;                                             (tr! db
;;                                                  (doall (doseq [k ks]
;;                                                           (get-val tr k))))))]
;;               (doall (map deref clients)))
;;             "Finished"))))

;; ;;(read-datoms)

;; ;; "Elapsed time: 57196.754037 msecs"
;; ;; "Elapsed time: 57096.223357 msecs"
