(ns fdb-playground.core
  (:import (com.apple.foundationdb FDB))
  (:require [clj-foundationdb.utils :refer :all]
            [clj-foundationdb.core :refer :all]
            [fdb-playground.keys :refer [->byteBuffer]]))


;; ;; Set a key

;; (let [fd    (FDB/selectAPIVersion 510)
;;       key   "foo"
;;       value 1]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (set-val tr key value))))


;; ;; Get a key

;; (let [fd  (select-api-version 510)
;;       key "foo"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (get-val tr key))))


;; ;; Perform multiple operations in a single transaction

;; (let [fd    (select-api-version 510)
;;       key   "foo"
;;       value 1]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (set-val tr key value)
;;          (get-val tr key))))


;; ;; Perform multiple operations in a single transaction

;; (let [fd    (select-api-version 510)
;;       key   "foo"
;;       value 1]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (set-val tr key value)
;;          (get-val tr key))))



;; ;; Set multiple keys with same value

;; (let [fd    (select-api-version 510)
;;       key   [["bar"] ["car"] ["dar"] ["far"]]
;;       value 1]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (set-keys tr key value))))



;; ;; Get a range of keys

;; (let [fd    (select-api-version 510)
;;       begin "car"
;;       end   "far"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (get-range tr begin end))))


;; ;; Get all keys

;; (let [fd (select-api-version 510)]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (get-all tr))))


;; ;; First key less than given key

;; (let [fd  (select-api-version 510)
;;       key "car"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (last-less-than tr key))))

;; ;; [[["bar"] 1]]

;; ;; First key greater than given key

;; (let [fd  (select-api-version 510)
;;       key "car"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (first-greater-than tr key))))

;; ;; [[["dar"] 1]]

;; ;; Nested keys

;; (let [fd      (select-api-version 510)
;;       classes [["class" "intro"] ["class" "algebra"] ["class" "maths"] ["class" "bio"]]
;;       time    "10:00"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (set-keys tr classes time)
;;          (get-val tr ["class" "algebra"]))))

;; ;; "10:00"

;; ;; Automatic subspace prefix within a context

;; (let [fd      (select-api-version 510)
;;       classes ["intro" "algebra" "maths" "bio"]
;;       time    "10:00"]
;;   (with-open [db (open fd)]
;;     (tr! db
;;          (with-subspace "class"
;;            (mapv #(set-val tr %1 time) classes)
;;            (get-val tr "algebra")))))

;; ;; "10:00"

;; ;;


(defn empty-fdb
  []
  (let [fd (FDB/selectAPIVersion 510)]
    (with-open [db (open fd)]
      db)))


(defn fdb-key
  "Converts a datom into a fdb key.
  Note the conversion of byte array into a string to fit the clj fdb library interface."
  [[e a v t]]
  (str (->byteBuffer [e (str a) (str v) t])))


;; Get
(defn get
  [db [e a v t]]
  (let [fd  (select-api-version 510)
        key (fdb-key [e a v t])]
    (with-open [db (open fd)]
      (tr! db
           (get-val tr key)))))


;; TODO: will have to insert the avet and co as well
;; TODO: will have to return smthg that we can attach to datahike
;; TODO: - db is the current fbd db, the with-open [db...] is obselete: we shall no longer open the db from there
;;       - So look at how the clj-fdb lib is opening the db and do the same
;;       - it could be the right time to get rid of the clj-fdb lib as well.
(defn fdb-insert
  [db [e a v t]]
  (let [fd    (FDB/selectAPIVersion 510)
        key   (fdb-key [e a v t])
        value key ;; Putting the key here, but we could put anything.
        ]
    (with-open [db (open fd)]
      (tr! db
           (set-val tr key value))
      db)))

;; TODO: finish specing it and implement
;;(defn fdb-get)

;; FoundationDB write datoms

;; 10 "Elapsed time: 105.700624 msecs"
;; 100 "Elapsed time: 126.062635 msecs"
;; 1000 "Elapsed time: 480.132474 msecs"
;; 10000 "Elapsed time: 3685.370752 msecs"
;; 100000 "Elapsed time: 35680.397137 msecs"

(defn write-datoms []
  (let [fd (select-api-version 510)
        kv (map #(vector (fdb-key [%1 (str ":attribute/" %1) %1 %1])  %1)
                (range 100000))]
    (time (let [clients (repeatedly 10 #(future
                                          (with-open [db (open fd)]
                                            (tr! db
                                                 (doall (doseq [[k v] kv]
                                                          ;;(set-val tr k v)
                                                          (fdb-insert k)
                                                          ))))))]
            (doall (map deref clients))
            "Finished"))))

;;(write-datoms)

;; "Elapsed time: 34156.643569 msecs"
;; "Elapsed time: 34472.143205 msecs"

;; FoundationDB read datoms

(defn read-datoms []
  (let [fd (select-api-version 510)
        ks (map #(vector (fdb-key [%1 (str ":attribute/" %1) %1 %1]))
                (range 100000))]
    (time (with-open [db (open fd)]
            (let [clients (repeatedly 10 #(future
                                            (tr! db
                                                 (doall (doseq [k ks]
                                                          (get-val tr k))))))]
              (doall (map deref clients)))
            "Finished"))))

;;(read-datoms)

;; "Elapsed time: 57196.754037 msecs"
;; "Elapsed time: 57096.223357 msecs"


;; FoundationDB write 1 million keys with 10 parallel clients and 100k keys per client

#_(let [fd (select-api-version 510)
        kv (map #(vector (str %1) %1) (range 100000))]
    (time (let [clients (repeatedly 10 #(future
                                          (with-open [db (open fd)]
                                            (tr! db
                                                 (doall (doseq [[k v] kv]
                                                          (set-val tr k v)))))))]
            (doall (map deref clients))
            "Finished")))

;; "Elapsed time: 27903.477365 msecs"



;; FoundationDB read 100k keys with 10 parallel clients and 10k keys per client

#_(let [fd (select-api-version 510)
      kv (map #(vector (str %1) %1) (range 100000))]
  (time (with-open [db (open fd)]
          (let [clients (repeatedly 10 #(future
                                          (tr! db
                                               (doall (doseq [k (range 10000)]
                                                        (get-val tr k))))))]
            (doall (map deref clients)))
          "Finished")))


;; "Elapsed time: 13783.479886 msecs"
