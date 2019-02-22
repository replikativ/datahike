(ns fdb.keys
  (:import (java.nio ByteBuffer))
  (:require [octet.core :as buf]))


(def buf-len 100)

;; Positions in the byte buffer where each section ends
(def eavt {:e-end 7 :a-end 40 :v-end 80 :t-end 99})



(defn- str-size
  [string]
  (count (.getBytes string)))

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

;; Def. and implement the other types
(def INT 1)
(def LONG 2)
(def STRING 3)

(defn- cst->type
  [int]
  "Returns the type corresponding to its encoding"
  (cond
    (= int INT)    java.lang.Integer
    (= int LONG)   java.lang.Long
    (= int STRING) java.lang.String))

(defn- str-offset
  "Returns the offset where to start writing a string
  given the end position of the string storage section.
  (Here offset means a shift to the left from the end position.)"
  [string-size section-end]
  ;; 2 * 4 bytes: as we store the string size twice:
  ;; - octet puts the size before the string
  ;; - we also put it again at the end
  ;; 4 more bytes: to store the encoding that we store a String
  (- section-end (+ string-size (* 2 4) 4)))

;; ------- writing --------

(defn- write-str
  [val buffer section-end]
  (assert (= (type val) java.lang.String))
  (buf/write! buffer [val] (buf/spec buf/string*)
              {:offset (str-offset (str-size val) section-end)})
  (buf/write! buffer [(str-size val)] (buf/spec buf/int32)
              {:offset (shift-left section-end 7)})
  (buf/write! buffer [STRING] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write-int
  [val buffer section-end]
  (buf/write! buffer [val] (buf/spec buf/int32)
              {:offset (shift-left section-end 7)})
  (buf/write! buffer [INT] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write-a
  "Write the 'a' part in eavt"
  [a buffer a-end]
  (let [a-as-str (attribute-as-str a)]
    (write-str a-as-str buffer a-end)))

(defn- write-long
  [val buffer section-end]
  (buf/write! buffer [val] (buf/spec buf/int64)
              {:offset (shift-left section-end 11)})
  (buf/write! buffer [LONG] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write
  [val buffer section-end]
  "Write 'val' into 'buffer' given 'section-end', the end of the section where it should be written"
  (let [type (type val)]
    (cond
      (= type java.lang.Integer) (write-int val buffer section-end)
      (= type java.lang.Long)    (write-long val buffer section-end)
      (= type java.lang.String)  (write-str val buffer section-end))))


;; ------- reading --------

(defn- read-int
  [buffer section-end shift-left-val]
  (first (buf/read buffer (buf/spec buf/int32)
                   {:offset (shift-left section-end shift-left-val)})))

(defn- read-long
  [buffer section-end]
  (first (buf/read buffer (buf/spec buf/int64)
                   {:offset (shift-left section-end 11)})))
(defn- read-str
  [buffer section-end]
  (let [size (read-int buffer section-end 7)]
    (first (buf/read buffer (buf/spec buf/string*)
                     {:offset (str-offset size section-end)}))))

(defn- read
  [buffer section-end]
  (let [type (cst->type (read-int buffer section-end 3))]
    (cond
      (= type java.lang.Integer) (read-int buffer section-end 7)
      (= type java.lang.Long)    (read-long buffer section-end)
      (= type java.lang.String)  (read-str buffer section-end))))

;; TODO: add validations that each of e a v t does not overflow.
;;
(defn ->byteBuffer
  [[e a v t]]
  (when a (assert (instance? clojure.lang.Keyword a)))
  (let [buffer (buf/allocate buf-len {:impl :nio :type :direct})]
    (buf/write! buffer [e] (buf/spec buf/int64) {:offset (shift-left (:e-end eavt) 7)})
    (write-a a buffer (:a-end eavt))
    (write v buffer (:v-end eavt))
    (buf/write! buffer [t] (buf/spec buf/int64) {:offset (shift-left (:t-end eavt) 7)})
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
  [buffer]
  (let [e (first (buf/read buffer (buf/spec buf/int64)
                           {:offset (shift-left (:e-end eavt) 7)}))
        a (keyword (read-str buffer (:a-end eavt)))
        v (read buffer (:v-end eavt))
        t (first (buf/read buffer (buf/spec buf/int64)
                           {:offset (shift-left (:t-end eavt) 7)}))]
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
(assert (== (str-offset (str-size "hello") 17) 0))

(def vect [20 :hello "some analysis" 3])
(def test-buff (->byteBuffer vect))
(def buff->vect (byteBuffer->vect test-buff))
(prn buff->vect)
(assert (= buff->vect vect))

(assert (= (key->vect (->byteArr vect)) vect))

;; There are 64 bits for [e]. The last byte is at index 7.
(assert (== (.get test-buff (:e-end eavt)) 20))
;; ;; size of 'hello' is 5
;; (assert (== (.get test-buff (shift-left (:a-end eavt) 3)) 5))
;; ;; the transaction id is ok
;; (assert (== (.get test-buff (:t-end eavt)) 3))


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
