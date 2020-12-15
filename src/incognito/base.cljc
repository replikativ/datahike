(ns incognito.base
  #?(:clj (:refer-clojure :exclude [read-string]))
  (:require #?(:clj [clojure.edn :refer [read-string]]
               :cljs [cljs.reader :refer [read-string
                                          *tag-table* *default-data-reader-fn*]])))


;;  This namespace override exists as part of the development work on porting to cljs. 
;;  We are currently thinking through how to improve the incognito library for better handling cross platform naming.
;;  Once the full port is completed this override will be removed.


(defrecord IncognitoTaggedLiteral [tag value])

(defn rec-name-parser [handlers]
  #?(:clj  handlers
     :cljs (into {} (map (fn [[k v]]
                           [(-> k
                                name
                                (clojure.string/replace-first  #"(?s)(.*)(\.)" "$1/")
                                symbol)
                            v])
                         handlers))))

(defn incognito-reader [read-handlers m]
  (if (read-handlers (:tag m))
    ((read-handlers (:tag m)) (:value m))
    (map->IncognitoTaggedLiteral m)))

(defn incognito-writer [write-handlers r]
  (let [write-handlers      (rec-name-parser write-handlers)
        s                   (-> r type pr-str symbol)
        break-map-recursion (if (map? r) (into {} r) r)
        [tag v]             (if (write-handlers s)
                              [s ((write-handlers s) break-map-recursion)]
                              [s break-map-recursion]
                              #_(pr-str->pure-read-string r))]
    {:tag   tag
     :value v}))
