(ns fdb.keys
  (:import (java.nio ByteBuffer)
           (datahike.db Datom))
  (:require [octet.core :as buf]))


(def buf-len 10000)

;; Positions in the byte buffer where each section ends
(def a-end 300)
(def v-end 9979)
(def t-end 9999)

(defn- str-size
  [string]
  (count (.getBytes string)))

(defn- offset
  "Returns the offset where to start writting a string
  given the end position of the string storage section.
  (The offset is a shift to the left from the end position.)"
  [string storage-end]
  (let [string-size (str-size string)]
    ;; 4 as octet uses a 32 bits integer to store the string size
    ;; (whereas octet writes it at the beginning)
    (- storage-end (+ string-size 4))))


(defn- str-bytes
  "Return the bytes of a string"
  [string]
  (.getBytes string "UTF-8"))


(defn- write-bytes
  "Dumps the bytes from 'offset' into the buffer.
  (Without writing a size.)"
  [buffer bytes offset]
  (let [len (alength bytes)]
    (map #(.put buffer %1 %2) (range offset (+ offset len)) bytes)))


(defn- read-bytes
  [buffer offset len]
  (map #(.get buffer %) (range offset (+ offset len))))

;; TODO: [v] can only be a string for now
;;
;; Why write strings at the end of a section? Because we want
;; the keys (i.e. bytes) to be ordered correctly in fdb
;;
;; By writing strings at the end of a section, we are also
;; forced to write the string size at the end so that we can
;; parse it when reading.
(defn- ->byteBuffer
  [[e a v t]]
  (let [buffer (buf/allocate buf-len {:impl :nio :type :direct})]
    (buf/write! buffer [e] (buf/spec buf/int64))
    (write-bytes buffer (str-bytes a) (offset a a-end))
    (buf/write! buffer [(str-size a)] (buf/spec buf/int32)
                {:offset (- a-end (- 8 1))})
    ;; (buf/write! buffer [(str-bytes v)] (buf/vector* buf/byte)
    ;;             {:offset (offset v v-end)})
    (write-bytes buffer (str-bytes v) (offset v v-end))
    (buf/write! buffer [(str-size v)] (buf/spec buf/int32)
                {:offset (- v-end (- 8 1))})
    ;; TODO: replace last - by 7
    (buf/write! buffer [t] (buf/spec buf/int64)   {:offset (- t-end (- 8 1))})
    buffer))

(defn ->byteArr
  [[e a v t]]
  (let [arr (byte-array buf-len)]
    (.get (->byteBuffer [e a v t]) arr)
    arr))


;; String str = new String(bytes, StandardCharsets.UTF_8);

(defn- str-from-bytes
  [bytes]
  (String. bytes "UTF_8"))


(defn datom
  "Converts a fdb key into a "
  [key]
  (let [e (buf/read key (buf/spec buf/int64))
        a-size (buf/read key (buf/spec buf/int32) {:offset a-end})
        a (buf/read key (buf/spec buf/string*) {:offset (- a-end a-size)})
        v-size (buf/read key (buf/spec buf/int32) {:offset v-end})
        v (buf/read key (buf/spec buf/string*) {:offset (- v-end v-size)})
        ;; TODO: replace last - by 7
        t (buf/read key (buf/spec buf/int64) {:offset (- t-end (- 8 1))})]
    (Datom. e 2 3 4 5)))


(defn print-buf
  [buffer]
  (for [x (range buf-len)]
    (.get buffer x)))



;; ---- Tests
;;
(assert (== (offset "hello" 9) 0))

(def test-buff (->byteBuffer [20 "hello" "some analysis" 3]))
;; There are 64 bits for [e]. The last byte is at index 7.
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
