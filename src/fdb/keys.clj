(ns fdb.keys
  (:import (java.nio ByteBuffer))
  (:require [octet.core :as buf])
  (:require [clojure.spec.alpha :as s]))


(def buf-len 100)

;; TODO: Could/Should? add a version number in the bytebuffer so that if we cope
;; with changes.
(defn position
  [index-type section-end]
  "Given an `index-type`, returns the position in the byte buffer where a given `section-end` is located. `index-type` and `section-end` are both keywords."
  (assert (s/valid? keyword? index-type))
  (assert (s/valid? keyword? section-end))
  (case index-type
    :eavt (section-end {:code 0 :e-end 8 :a-end 40 :v-end 80 :t-end 99})
    :aevt (section-end {:code 0 :a-end 40 :e-end 48 :v-end 80 :t-end 99})
    :avet (section-end {:code 0 :a-end 32 :v-end 72 :e-end 80 :t-end 99})
    (throw (IllegalArgumentException. (str "invalid index-type " index-type)))))

(def index-type->code {:eavt 0 :aevt 1 :avet 2})


(defn- str-size
  [string]
  (count (.getBytes string)))

(defn- shift-left
  [offset n]
  "Returns the location given by shifting left by `n` bytes starting
   at the location given by `offset`.
   (Can be used to find out where to write n + 1 bytes that ends at the location
   given by `offset`)"
  (assert (s/valid? int? offset))
  (assert (s/valid? int? n))
  (- offset n))

(defn- attribute-as-str
  "Expects a datom attribute, i.e. a keyword and converts it into a string.
  If nil, return an empty string."
  [a]
  (if a
    (do
      (assert (s/valid? keyword? a))
      (let [a-namespace (namespace a)]
        (str a-namespace (when a-namespace "/") (name a))))
    ""))

;; TODO: Def. and implement the other types
(def INT 1)
(def LONG 2)
(def STRING 3)

(defn- cst->type
  [int]
  "Returns the type corresponding to its encoding"
  (assert (s/valid? int? int))
  (cond
    (= int INT)    java.lang.Integer
    (= int LONG)   java.lang.Long
    (= int STRING) java.lang.String))

(defn- str-offset
  "Returns the offset where to start writing a string
  given the end position of the string storage section.
  (Here offset means a shift to the left from the end position.)"
  [string-size section-end]
  (assert (s/valid? int? string-size))
  (assert (s/valid? int? section-end))
  ;; 2 * 4 bytes: as we store the string size twice:
  ;; - octet puts the size before the string
  ;; - we also put it again at the end
  ;; 4 more bytes: to store the encoding that we store a String
  (- section-end (+ string-size (* 2 4) 4)))

;; ------- writing --------

(defn- write-str
  [val buffer section-end]
  (assert (s/valid? string? val))
  (buf/write! buffer [val] (buf/spec buf/string*)
              {:offset (str-offset (str-size val) section-end)})
  (buf/write! buffer [(str-size val)] (buf/spec buf/int32)
              {:offset (shift-left section-end 7)})
  (buf/write! buffer [STRING] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write-int
  [val buffer section-end]
  (assert (s/valid? int? val))
  (buf/write! buffer [val] (buf/spec buf/int32)
              {:offset (shift-left section-end 7)})
  (buf/write! buffer [INT] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write-a
  "Write the `a` part in eavt"
  [a buffer a-end]
  (assert (s/valid? (s/alt :nil nil?
                           :keyword keyword?) [a]))
  (let [a-as-str (attribute-as-str a)]
    (write-str a-as-str buffer a-end)))

(defn- write-long
  [val buffer section-end]
  (assert (s/valid? integer? val))
  (buf/write! buffer [val] (buf/spec buf/int64)
              {:offset (shift-left section-end 11)})
  (buf/write! buffer [LONG] (buf/spec buf/int32)
              {:offset (shift-left section-end 3)}))

(defn- write
  [val buffer section-end]
  "Write `val` into `buffer` given `section-end`, the end of the section where it should be written"
  (let [type (type val)]
    (cond
      (= type java.lang.Integer) (write-int val buffer section-end)
      (= type java.lang.Long)    (write-long val buffer section-end)
      (= type java.lang.String)  (write-str val buffer section-end))))




;; TODO: add validations that each of e a v t does not overflow.
;;
;; 06-05-2020: 'index-type' is a useless arg for now as we only one copy of the datom in
;; the [e a v t] form. But if later we decide to store the data in another order than eavt,
;; we will need the argument.
(defn ->byteBuffer
  "Converts a vector into a bytebuffer.
  Whatever the index-type, expects the datom in the [e a v t] format.
  (Datahike code will always send [e a v t] and only the index-type will vary.)"
  [index-type [e a v t]]
  (assert (s/valid? keyword? index-type))
  (assert (some #{:eavt :aevt :avet} [index-type]))
  (let [buffer          (buf/allocate buf-len {:impl :nio :type :direct})
        index-type-code (index-type->code index-type)]
    (when a (assert (instance? clojure.lang.Keyword a)))
    (assert (and (<= 0 index-type-code) (>= 2 index-type-code)))
    ;;(println "Writing: " index-type e a v t)
    ;; Write a code in the first byte to distinguish between the diff. indices. The code is like a namespace.
    (buf/write! buffer [index-type-code] (buf/spec buf/byte))
    (buf/write! buffer [e] (buf/spec buf/int64) {:offset (shift-left (position index-type :e-end) 7)})
    (write-a a buffer (position index-type :a-end))
    (write v buffer (position index-type :v-end))
    (buf/write! buffer [t] (buf/spec buf/int64) {:offset (shift-left (position index-type :t-end) 7)})
    buffer))

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
      (= type java.lang.Integer) (read-int   buffer section-end 7)
      (= type java.lang.Long)    (read-long  buffer section-end)
      (= type java.lang.String)  (read-str   buffer section-end))))


(defn ->byteArr
  [index-type [e a v t]]
  (let [arr (byte-array buf-len)]
    (.get (->byteBuffer index-type [e a v t]) arr)
    arr))

(defn byteArr->byteBuffer
  [byteArr]
  (ByteBuffer/wrap byteArr))

(defn byteBuffer->vect
  "Converts a bytebuffer representation of a fdb key into a datom vector"
  [index-type buffer]
  (let [index-type-code (first (buf/read buffer (buf/spec buf/byte))) ;; offset: 0
        expected-index-type-code (index-type->code index-type)
        e (first (buf/read buffer (buf/spec buf/int64)
                   {:offset (shift-left (position index-type :e-end) 7)}))
        a (keyword (read-str buffer (position index-type :a-end)))
        v (read buffer (position index-type :v-end))
        t (first (buf/read buffer (buf/spec buf/int64)
                   {:offset (shift-left (position index-type :t-end) 7)}))]

    (when-not (= index-type-code expected-index-type-code)
      (throw (RuntimeException. (str "Expecting to read an index of type: " expected-index-type-code ", but got type: " index-type-code))))

    (case index-type
      :eavt [e a v t]
      :aevt [a e v t]
      :avet [a v e t]
      (throw (IllegalArgumentException. (str "invalid index-type " index-type))))))


(defn key->vect
  "Converts a fdb key (represented as a byte array) into a clojure vector."
  [index-type byteArr]
  (byteBuffer->vect index-type (byteArr->byteBuffer byteArr)))


;; TODO: what's corresponding to 'v' can not be of any type for now. As only some of the basic types are supported.
(defn key
  "Converts a datom into a fdb key. `index-type` is keyword representing the index type."
  ;; Can take ^Datom object as input (as they are array) ;; <- TODO: careful here as Datom does not provide on e a v t.
  [index-type [a b c t]]
  ;;(prn (str index-type  [a b c t])) ;; TODO
  (->byteArr index-type [a b c t]))


(defn print-buf
  [buffer]
  (for [x (range buf-len)]
    (.get buffer x)))


;; ---- Tests   ;; TODO: move into comments at the end
;;
(assert (== (str-offset (str-size "hello") 17) 0))

(def vect [20 :hello "some analysis" 3])
(def test-buff (->byteBuffer :eavt vect))
(def buff->vect (byteBuffer->vect :eavt test-buff))
;;(prn buff->vect)
(assert (= buff->vect vect))

(assert (= (key->vect :eavt (->byteArr :eavt vect)) vect))

;; There are 64 bits for [e]. The last byte is at index 7.
(assert (== (.get test-buff (position :eavt :e-end)) 20))
;; ;; size of `hello` is 5
;; (assert (== (.get test-buff (shift-left (:a-end eavt) 3)) 5))
;; ;; the transaction id is ok
;; (assert (== (.get test-buff (:t-end eavt)) 3))


(def with-keyword [20 :shared/policy "some analysis" 3])
(def buff (->byteBuffer :eavt with-keyword))
(def buff->vect (byteBuffer->vect :eavt buff))
;;(prn buff->vect)
(assert (=  buff->vect with-keyword))

(comment

  (def datom [1000 ":analysis:name" "without name" 2])

  (print-buf (->byteBuffer datom))

  (println "arr contains" (vec (->byteArr datom)))
  )
