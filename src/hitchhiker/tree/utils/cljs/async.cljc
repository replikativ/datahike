(ns hitchhiker.tree.utils.cljs.async
  #?(:cljs (:require-macros [hitchhiker.tree.utils.cljs.async :refer [go-try <? if-async?]]))
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]))


;;  This namespace override exists purely for development work on porting to cljs
;;  Instruments the <?? operator to throw an error when called in cljs
;;  Once the full port is completed this override will be removed.


(def ^:dynamic *async?* true)

(defmacro if-async?
  ""
  {:style/indent 2}
  [then else]
  (if *async?*
    then
    else))

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (instance? #?(:clj Exception :cljs js/Error) x)
    (throw #_(ex-info (or #?(:clj (.getMessage ^Exception x)) (str x))
                      (or (ex-data x) {})
                      x)
           x)
    x))

(defn promise-chan
  [x]
  (doto (async/promise-chan)
    (async/offer! x)))

(defmacro go-try
  "Asynchronously executes the body in a go block. Returns a channel
  which will receive the result of the body when completed or the
  exception if an exception is thrown. You are responsible to take
  this exception and deal with it! This means you need to take the
  result from the channel at some point."
  {:style/indent 1}
  [& body]
  (if-async?
   (let [e (if (:ns &env) 'js/Error Throwable)]
     `(async/go
        (try
          (do ~@body)
          (catch ~e e# e#))))
   `(do ~@body)))

(defmacro <?
  "Same as core.async <! but throws an exception if the channel returns a
  throwable error."
  [ch]
  (if-async?
   `(throw-if-exception (async/<! ~ch))
   ch))

#?(:clj
   (defmacro <??
     "Same as core.async <!! but throws an exception if the channel returns a
  throwable error."
     [ch]
     (if (:ns &env)
       `(js/Error "This is a syncronous call. Not allowed in JS environment")
       (if-async?
        `(throw-if-exception (async/<!! ~ch))
        ch))))

(defn map<
  "Maps over a coll or colls with a go function go-f."
  ([go-f s]
   (go-try
    (loop [res []
           s s]
      (if (seq s)
        (recur (conj res (<? (go-f (first s)))) (rest s))
        res))))
  ([go-f s1 s2]
   (go-try
    (loop [res []
           s1 s1
           s2 s2]
      (if (and (seq s1) (seq s2))
        (recur (conj res (<? (go-f (first s1) (first s2)))) (rest s1) (rest s2))
        res)))))



(defmacro maybe<?
  [ch]
  (if-async?
   `(if (satisfies? async-protocols/ReadPort ~ch)
     (throw-if-exception (async/<! ~ch))
     ~ch)
   ch))



(defn filter<
  "Filters a collection. If the function is async it returns "
  ([pred s]
   (go-try
    (loop [res []
           s s]
      (if (seq s)
        (let [to-conj? (pred (first s))
              to-conj? (if (satisfies? async-protocols/WritePort to-conj?) (<? to-conj?) to-conj?)]
          (if to-conj?
            (recur (conj res (first s)) (rest s))
            (recur  res (rest s))))
        res)))))





(defn reduce<
  "Reduces over a sequence s with a go function go-f given the initial value
  init."
  [go-f init s]
  (go-try
   (loop [res init
          [f & r] s]
     (if f
       (recur (<? (go-f res f)) r)
       res))))

(defn mapv<
  "Returns a vector consisting of the result of applying f to the
  set of first items of each coll, followed by applying f to the set
  of second items in each coll, until any one of the colls is
  exhausted.  Any remaining items in other colls are ignored. Function
  f should accept number-of-colls arguments."
  {:added "1.4"
   :static true}
  ([go-f coll]
   (reduce< (fn [v o] (go-try (conj! v (<? (go-f o))))) [] coll))
  ([go-f c1 c2]
   (go-try (into [] (<? (map< go-f c1 c2))))))

(defn update-in< [m ks go-f & args]
  (let [up (fn up [m ks go-f args]
             (go-try
              (let [[k & ks] ks]
                (if ks
                  (assoc m k (<? (up (get m k) ks go-f args)))
                  (assoc m k (<? (apply go-f (get m k) args)))))))]
    (up m ks go-f args)))

#?(:clj
   (defn chan-seq [ch]
     (when-some [v (<?? ch)]
       (cons v (lazy-seq (chan-seq ch))))))
