(ns fdb-playground.keys
  (:import (java.nio ByteBuffer))
  (:require [octet.core :as buf]))


(def buf-len 10000)

;; Positions in the byte buffer where each section ends
(def a-end 300)
(def v-end 9979)
(def t-end 9999)

(defn- offset
  "Returns the offset where to start writting a string
  given the end position of the string storage section"
  [string storage-end]
  (let [string-size (count (.getBytes string))]
    ;; + 4 as octet uses a 32 bits integer to store the string size
    (- storage-end (+ string-size 4))))


(defn ->byteBuffer
  [[e a v t]]
  (let [buffer (buf/allocate buf-len {:impl :nio :type :direct})]
    (buf/write! buffer [e] (buf/spec buf/int64))
    (buf/write! buffer [a] (buf/spec buf/string*) {:offset (offset a a-end)})
    (buf/write! buffer [v] (buf/spec buf/string*) {:offset (offset v v-end)})
    (buf/write! buffer [t] (buf/spec buf/int64)   {:offset (- t-end (- 8 1))})
    buffer))

(defn ->byteArr
  [[e a v t]]
  (let [arr (byte-array buf-len)]
    (.get (->byteBuffer [e a v t]) arr)
    arr))

(defn print-buf
  [buffer]
  (for [x (range buf-len)]
    (.get buffer x)))



;; ---- Tests
;;
(assert (== (offset "hello" 9) 0))

(def test-buff (->byteBuffer [20 "hello" "some analysis" 3]))
(assert (== (.get test-buff 7) 20))
;; size of 'hello' is 5
(assert (== (.get test-buff (- a-end 6)) 5))
;; the transaction id is ok
(assert (== (.get test-buff t-end) 3))


(comment

  (def datom [1000 ":analysis:name" "without name" 2])

  (print-buf (->byteBuffer datom))

  (println "arr contains" (vec (->byteArr datom)))
  )
