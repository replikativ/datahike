(ns fdb.keys
  (:import (java.nio ByteBuffer))
  (:require [octet.core :as buf]))


(def buf-len 100)

;; Positions in the byte buffer where each section ends
(def a-end 40)
(def v-end 80)
(def t-end 99)

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
  "Returns the location given by shifting left by 'n' bytes starting
   at the location given by 'offset'.
   (Can be used to find out where to write n + 1 bytes that ends at the location
   given by 'offset')"
  (- offset n))


(defn- attribute-as-str
  "Expects a datom attribute, i.e. a keyword and converts it into a string.
  If nil, return an empty string."
  [a]
  (if a
    (let [a-namespace (namespace a)]
      (str a-namespace (when a-namespace "/") (name a)))
    ""))

;; TODO: [v] can only be a string for now.
;; TODO: add validations that each of e a v t does not overflow.
;;
;; When writing a string also need to write its size at the end so that
;; when we want to read it we know where to start.
;; The string size location must be at the end of the section.
(defn ->byteBuffer
  [[e a v t]]
  (when a (assert (instance? clojure.lang.Keyword a)))
  (let [a-as-str (attribute-as-str a)
        buffer   (buf/allocate buf-len {:impl :nio :type :direct})]
    (buf/write! buffer [e] (buf/spec buf/int64))
    (buf/write! buffer [a-as-str] (buf/spec buf/string*)
                {:offset (offset (str-size a-as-str) a-end)})
    (buf/write! buffer [(str-size a-as-str)] (buf/spec buf/int32)
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
  "Converts a fdb key (bytebuffer) into a datom vector"
  [key]
  (let [e      (first (buf/read key (buf/spec buf/int64)))
        a-size (first (buf/read key (buf/spec buf/int32)
                                {:offset (shift-left a-end 3)}))
        a      (keyword (first (buf/read key (buf/spec buf/string*)
                                         {:offset (offset a-size a-end)})))
        v-size (first (buf/read key (buf/spec buf/int32)
                                {:offset (shift-left v-end 3)}))
        v      (first (buf/read key (buf/spec buf/string*)
                                {:offset (offset v-size v-end)}))
        t      (first (buf/read key (buf/spec buf/int64) {:offset (shift-left t-end 7)}))]
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

(def vect [20 :hello "some analysis" 3])
(def test-buff (->byteBuffer vect))
(def buff->vect (byteBuffer->vect test-buff))
(prn buff->vect)
(assert (= buff->vect vect))

(assert (= (key->vect (->byteArr vect)) vect))

;; There are 64 bits for [e]. The last byte is at index 7.
(assert (== (.get test-buff 7) 20))
;; ;; size of 'hello' is 5
;; (assert (== (.get test-buff (shift-left a-end 3)) 5))
;; ;; the transaction id is ok
;; (assert (== (.get test-buff t-end) 3))


(def with-keyword [20 :shared/policy "some analysis" 3])
(def buff (->byteBuffer with-keyword))
(def buff->vect (byteBuffer->vect buff))
(prn buff->vect)
(assert (=  buff->vect with-keyword))

(comment

  (def datom [1000 ":analysis:name" "without name" 2])

  (print-buf (->byteBuffer datom))

  (println "arr contains" (vec (->byteArr datom)))
  )
