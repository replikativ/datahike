(ns ^:no-doc datahike.tools
  (:require
   [superv.async :refer [throw-if-exception-]]
   #?(:clj [clojure.java.io :as io])
   [taoensso.timbre :as log])
  #?(:clj (:import [java.util Properties UUID Date])))

(defn combine-hashes [x y]
  #?(:clj  (clojure.lang.Util/hashCombine x y)
     :cljs (hash-combine x y)))

#?(:clj
   (defn- -case-tree [queries variants]
     (if queries
       (let [v1 (take (/ (count variants) 2) variants)
             v2 (drop (/ (count variants) 2) variants)]
         (list 'if (first queries)
               (-case-tree (next queries) v1)
               (-case-tree (next queries) v2)))
       (first variants))))

#?(:clj
   (defmacro case-tree [qs vs]
     (-case-tree qs vs)))

(defn get-time []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defmacro raise
  "Logging an error and throwing an exception with message and structured data.
   Arguments:
   - Any number of strings that describe the error
   - Last argument is a map of data that helps understanding the source of the error"
  [& fragments]
  (let [msgs (butlast fragments)
        data (last fragments)]
    (list `(log/log! :error :p ~fragments ~{:?line (:line (meta &form))})
          `(throw #?(:clj  (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)
                     :cljs (error (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data))))))

; (throwable-promise) derived from (promise) in clojure/core.clj.
; *   Clojure
; *   Copyright (c) Rich Hickey. All rights reserved.
; *   The use and distribution terms for this software are covered by the
; *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; *   which can be found in the file epl-v10.html at the root of this distribution.
; *   By using this software in any fashion, you are agreeing to be bound by
; * 	 the terms of this license.
; *   You must not remove this notice, or any other, from this software.
(defn throwable-promise
  "Returns a promise object that can be read with deref/@, and set,
  once only, with deliver. Calls to deref/@ prior to delivery will
  block, unless the variant of deref with timeout is used. All
  subsequent derefs will return the same delivered value without
  blocking. Exceptions delivered to the promise will throw on deref."
  []
  (let [d (java.util.concurrent.CountDownLatch. 1)
        v (atom d)]
    (reify
      clojure.lang.IDeref
      (deref [_] (.await d) (throw-if-exception- @v))
      clojure.lang.IBlockingDeref
      (deref
        [_ timeout-ms timeout-val]
        (if (.await d timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (throw-if-exception- @v)
          timeout-val))
      clojure.lang.IPending
      (isRealized [this]
        (zero? (.getCount d)))
      clojure.lang.IFn
      (invoke
        [this x]
        (when (and (pos? (.getCount d))
                   (compare-and-set! v d x))
          (.countDown d)
          this)))))

(defn get-version
  "Retrieves the current version of a dependency. Thanks to https://stackoverflow.com/a/33070806/10978897"
  [dep]
  #?(:clj
     (let [path (str "META-INF/maven/" (or (namespace dep) (name dep))
                     "/" (name dep) "/pom.properties")
           props (io/resource path)]
       (when props
         (with-open [stream (io/input-stream props)]
           (let [props (doto (Properties.) (.load stream))]
             (.getProperty props "version")))))
     :cljs
     "JavaScript"))

(defn meta-data []
  {:datahike/version (or (get-version 'io.replikativ/datahike) "DEVELOPMENT")
   :konserve/version (get-version 'io.replikativ/konserve)
   :hitchhiker.tree/version (get-version 'io.replikativ/hitchhiker-tree)
   :datahike/id (UUID/randomUUID)
   :datahike/created-at (Date.)})

(defn deep-merge
  "Recursively merges maps together. If all the maps supplied have nested maps
  under the same keys, these nested maps are merged. Otherwise the value is
  overwritten, as in `clojure.core/merge`.

  Copied from weavejester/medley 1.3.0"
  {:arglists '([& maps])
   :added    "1.1.0"}
  ([])
  ([a] a)
  ([a b]
   (when (or a b)
     (letfn [(merge-entry [m e]
               (let [k  (key e)
                     v' (val e)]
                 (if (contains? m k)
                   (assoc m k (let [v (get m k)]
                                (if (and (map? v) (map? v'))
                                  (deep-merge v v')
                                  v')))
                   (assoc m k v'))))]
       (reduce merge-entry (or a {}) (seq b)))))
  ([a b & more]
   (reduce deep-merge (or a {}) (cons b more))))
