(ns hitchhiker.tree.utils.platform
  "FIXME would be nice to get rid of that ns"
  (:refer-clojure :exclude [satisfies?]))

#?(:cljs (defn satisfies? [p x] (cljs.core/satisfies? p x))
   :clj
   (def satisfies?
     ;; FIXME for now `(satisfies? IFoo node)` is crippled,
     ;; implementers have no cache see:
     ;; https://dev.clojure.org/jira/browse/CLJ-1814.  So we just check
     ;; types manually to get x2 speed from satisfies?
     ;; check cache first, otherwise call satisfies? and cache result
     (let [cache (volatile! {})]
       (fn [proto node]
         ;; check cache first, otherwise call satisfies? and cache result
         (let [kls (class node)
               k [proto kls]
               cached-val (get @cache k ::nothing)]
           (if (identical? cached-val ::nothing)
             (let [ret (clojure.core/satisfies? proto node)]
               (vswap! cache assoc k ret)
               ret)
             cached-val))))))
