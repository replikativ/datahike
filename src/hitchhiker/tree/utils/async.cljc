(ns hitchhiker.tree.utils.async
  (:require
   [hitchhiker.tree.utils.platform :as platform]
   #?(:clj [clojure.core.async :as async]
      :cljs [cljs.core.async :as async :include-macros true])))

(def ^:dynamic *async?*
  #?(:clj false :cljs true))

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
    (throw (ex-info (or #?(:clj (.getMessage ^Exception x)) (str x))
                    (or (ex-data x) {})
                    x))
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
      `(async/go
         (try
           ~@body
           (catch #?(:clj Exception :cljs js/Error) e#
             e#)))
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
     (if-async?
         `(throw-if-exception (async/<!! ~ch))
       ch)))

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

#?(:clj
   (defn chan-seq [ch]
     (when-some [v (<?? ch)]
       (cons v (lazy-seq (chan-seq ch))))))
