(ns datahike.db.search
  (:require
   [clojure.core.cache.wrapped :as cw]
   [datahike.array :refer [a=]]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :refer [datom datom-tx datom-added]]
   [datahike.db.utils :as dbu]
   [datahike.index :as di]
   [datahike.lru :refer [lru-datom-cache-factory]]
   [datahike.tools :as dt :refer [case-tree]]
   [environ.core :refer [env]])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]]
                            [datahike.tools :refer [case-tree]]))
  #?(:clj (:import [datahike.datom Datom])))

(def db-caches (cw/lru-cache-factory {} :threshold (:datahike-max-db-caches env 5)))

(defn memoize-for [db key f]
  (if (or (zero? (or (:cache-size (:config db)) 0))
          (zero? (:hash db))) ;; empty db
    (f)
    (let [db-cache (cw/lookup-or-miss db-caches
                                      (:hash db)
                                      (fn [_] (lru-datom-cache-factory {} :threshold (:cache-size (:config db)))))]
      (cw/lookup-or-miss db-cache key (fn [_] (f))))))

(defn validate-pattern
  "Checks if database pattern is valid"
  [pattern]
  (let [[e a v tx added?] pattern]

    (when-not (or (number? e)
                  (nil? e)
                  (and (vector? e) (= 2 (count e))))
      (dt/raise "Bad format for entity-id in pattern, must be a number, nil or vector of two elements."
             {:error :search/pattern :e e :pattern pattern}))

    (when-not (or (number? a)
                  (keyword? a)
                  (nil? a))
      (dt/raise "Bad format for attribute in pattern, must be a number, nil or a keyword."
             {:error :search/pattern :a a :pattern pattern}))

    (when-not (or (not (vector? v))
                  (nil? v)
                  (and (vector? v) (= 2 (count v))))
      (dt/raise "Bad format for value in pattern, must be a scalar, nil or a vector of two elements."
             {:error :search/pattern :v v :pattern pattern}))

    (when-not (or (nil? tx)
                  (number? tx))
      (dt/raise "Bad format for transaction ID in pattern, must be a number or nil."
             {:error :search/pattern :tx tx :pattern pattern}))

    (when-not (or (nil? added?)
                  (boolean? added?))
      (dt/raise "Bad format for added? in pattern, must be a boolean value or nil."
             {:error :search/pattern :added? added? :pattern pattern}))))

(defn- search-indices
  "Assumes correct pattern form, i.e. refs for ref-database"
  [eavt aevt avet pattern indexed? temporal-db?]
  (validate-pattern pattern)
  (let [[e a v tx added?] pattern]
    (if (and (not temporal-db?) (false? added?))
      '()
      (case-tree [e a (some? v) tx]
                 [(di/-slice eavt (datom e a v tx) (datom e a v tx) :eavt) ;; e a v tx
                  (di/-slice eavt (datom e a v tx0) (datom e a v txmax) :eavt) ;; e a v _
                  (->> (di/-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt) ;; e a _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (di/-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt) ;; e a _ _
                  (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ v tx
                       (filter (fn [^Datom d] (and (a= v (.-v d))
                                                   (= tx (datom-tx d))))))
                  (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ v _
                       (filter (fn [^Datom d] (a= v (.-v d)))))
                  (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ _ _
                  (if indexed?                              ;; _ a v tx
                    (->> (di/-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
                         (filter (fn [^Datom d] (= tx (datom-tx d)))))
                    (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (and (a= v (.-v d))
                                                     (= tx (datom-tx d)))))))
                  (if indexed?                              ;; _ a v _
                    (di/-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
                    (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (a= v (.-v d))))))
                  (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt) ;; _ a _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt) ;; _ a _ _
                  (filter (fn [^Datom d] (and (a= v (.-v d)) (= tx (datom-tx d)))) (di/-all eavt)) ;; _ _ v tx
                  (filter (fn [^Datom d] (a= v (.-v d))) (di/-all eavt)) ;; _ _ v _
                  (filter (fn [^Datom d] (= tx (datom-tx d))) (di/-all eavt)) ;; _ _ _ tx
                  (di/-all eavt)]))))

(defn search-current-indices [db pattern]
  (memoize-for db [:search pattern]
               #(let [[_ a _ _] pattern]
                  (search-indices (:eavt db)
                                  (:aevt db)
                                  (:avet db)
                                  pattern
                                  (dbu/indexing? db a)
                                  false))))

(defn search-temporal-indices [db pattern]
  (memoize-for db [:temporal-search pattern]
               #(let [[_ a _ _ added] pattern
                      result (search-indices (:temporal-eavt db)
                                             (:temporal-aevt db)
                                             (:temporal-avet db)
                                             pattern
                                             (dbu/indexing? db a)
                                             true)]
                  (case added
                    true (filter datom-added result)
                    false (remove datom-added result)
                    nil result))))

(defn temporal-search [db pattern]
  (dbu/distinct-datoms db
                       (search-current-indices db pattern)
                       (search-temporal-indices db pattern)))

(defn temporal-seek-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (dbu/distinct-datoms db
                         (di/-slice index from to index-type)
                         (di/-slice temporal-index from to index-type))))

(defn temporal-rseek-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (concat
     (-> (dbu/distinct-datoms db
                              (di/-slice index from to index-type)
                              (di/-slice temporal-index from to index-type))
         vec
         rseq))))

(defn temporal-index-range [db current-db attr start end]
  (when-not (dbu/indexing? db attr)
    (dt/raise "Attribute" attr "should be marked as :db/index true" {}))
  (dbu/validate-attr attr (list '-index-range 'db attr start end) db)
  (let [from (dbu/resolve-datom current-db nil attr start nil e0 tx0)
        to (dbu/resolve-datom current-db nil attr end nil emax txmax)]
    (dbu/distinct-datoms db
                         (di/-slice (:avet db) from to :avet)
                         (di/-slice (:temporal-avet db) from to :avet))))
