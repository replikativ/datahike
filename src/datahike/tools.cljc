(ns datahike.tools
  (:require [me.tonsky.persistent-sorted-set.arrays :as arrays]))


#?(:clj
  (defmacro raise [& fragments]
    (let [msgs (butlast fragments)
          data (last fragments)]
      `(throw (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)))))

(defn #?@(:clj  [^Boolean seqable?]
          :cljs [^boolean seqable?])
  [x]
  (and (not (string? x))
  #?(:cljs (or (cljs.core/seqable? x)
               (arrays/array? x))
     :clj  (or (seq? x)
               (instance? clojure.lang.Seqable x)
               (nil? x)
               (instance? Iterable x)
               (arrays/array? x)
               (instance? java.util.Map x)))))
