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
  [string-size storage-end]
  ;; 2 * 4 bytes as we store the string size twice:
  ;; - octet puts the size before the string
  ;; - we put it at the end
  (- storage-end (+ string-size (* 2 4))))

(defn- shift-left
  [offset n]
  "Shift left by 'n' bytes starting at the location given by 'offset'.
   (Can be used to find out where to write n + 1 bytes that ends at the location
   given by 'offset')"
  (- offset n))


;; TODO: [v] can only be a string for now
;; When writing a string also need to write its size at the end so that
;; when we want to read it we know where to start.
;; The place of the size of the string should match the end of the section.
(defn ->byteBuffer
  [[e a v t]]
  (let [buffer (buf/allocate buf-len {:impl :nio :type :direct})]
    (buf/write! buffer [e] (buf/spec buf/int64))
    (buf/write! buffer [a] (buf/spec buf/string*)
                {:offset (offset (str-size a) a-end)})
    (buf/write! buffer [(str-size a)] (buf/spec buf/int32)
                {:offset (shift-left a-end 3)})
    (buf/write! buffer [v] (buf/spec buf/string*)
                {:offset (offset (str-size v) v-end)})
    (buf/write! buffer [(str-size v)] (buf/spec buf/int32)
                {:offset (shift-left v-end 3)})
    (buf/write! buffer [t] (buf/spec buf/int64) {:offset (shift-left t-end 7)})
    buffer))


(defn ->byteArr
  [[e a v t]]
  (let [arr (byte-array buf-len)]
    (.get (->byteBuffer [e a v t]) arr)
    arr))


(defn byteArr->byteBuffer
  [byteArr]
  (ByteBuffer/wrap byteArr))

(defn byteBuffer->vect
  "Converts a fdb key (bytebuffer) into a ^Datom"
  [key]
  (let [e (first (buf/read key (buf/spec buf/int64)))
        a-size (first (buf/read key (buf/spec buf/int32)
                                {:offset (shift-left a-end 3)}))
        a (first (buf/read key (buf/spec buf/string*)
                           {:offset (offset a-size a-end)}))
        v-size (first (buf/read key (buf/spec buf/int32)
                                {:offset (shift-left v-end 3)}))
        v (first (buf/read key (buf/spec buf/string*)
                           {:offset (offset v-size v-end)}))
        t (first (buf/read key (buf/spec buf/int64) {:offset (shift-left t-end 7)}))]
    ;; TODO: ask what 'true' is
    #_(Datom. e a v t true)
    [e a v t]))


(defn key->vect
  [byteArr]
  (byteBuffer->vect (byteArr->byteBuffer byteArr)))

(defn print-buf
  [buffer]
  (for [x (range buf-len)]
    (.get buffer x)))



;; ---- Tests
;;
(assert (== (offset (str-size "hello") 13) 0))

(def vect [20 "hello" "some analysis" 3])
(def test-buff (->byteBuffer vect))

(assert (= (byteBuffer->vect test-buff) vect))

(assert (= (key->vect (->byteArr vect)) vect))

;; There are 64 bits for [e]. The last byte is at index 7.
(assert (== (.get test-buff 7) 20))
;; ;; size of 'hello' is 5
;; (assert (== (.get test-buff (shift-left a-end 3)) 5))
;; ;; the transaction id is ok
;; (assert (== (.get test-buff t-end) 3))


(comment

  (def datom [1000 ":analysis:name" "without name" 2])

  (print-buf (->byteBuffer datom))

  (println "arr contains" (vec (->byteArr datom)))
  )
