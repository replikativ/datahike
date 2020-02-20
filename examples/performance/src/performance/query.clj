(ns performance.query
  (:require [performance.measure :refer [measure-query-times]]
            [performance.uri :as uri]
            [performance.const :as c]
            [datahike.api :as d]
            [datomic.api :as da]
            [incanter.stats :as is]
            [incanter.charts :as charts]
            [incanter.core :as ic])
  (:import (java.util Date UUID)))

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])

(defmacro placeholder [a]
  (read-string (str "'?" a)))

(placeholder "r1")

(def ?i 2)

(defmacro join-clauses [ref-name ref-val attr-name attr-val]
  )

(join-clauses ":R1" "?r1" ":A1" "?res1")

;[~b :R1 ~a]

(defmacro f1 [b]
  (let [b# ~b]
    '(vec b#))
  )



(map (fn [a v] '[?e ~a ~~v] ) [:R1] ['?r1])                 ;; ! ?

'(vec ~(symbol "?e"))

(f1 "e")

;;(macroexpand (f '?e '?r1))
(macroexpand (f1 "?e"))

;; 1 union, say: find res, set m=n on db creation
((map (fn [a r res] '[res [?e (keyword (str "R" r)) ?r]
                          [?r (keyword (str "A" a)) res]])
      (shuffle (range 3))
      (shuffle (range 3))
      '[?res1 ?res2 ?res3]))

;;(defn create-n-queries [n n-direct-comp n-union-comp] '[:find ?e    :where [?e ]])


(def query1 '[:find ?e ?res
              :in $
              :where [?e :R1 ?r]
                     [?r :A3 ?res]])

(def query2 '[:find ?e ?res1 ?res2
              :in $
              :where [?e :R1 ?r1]
                     [?r1 :A3 ?res1]
                     [?e :R2 ?r2]
                     [?r2 :A4 ?res2]])

;;(def initital-query '{:find ?e :in $ :where []})

(comment (defn get-query [n-joins n-attr max-attr]
           (let [initital-query '{:find ?e :in $ :where []}
                 ref-names (map #(keyword (str "R" %)) (take n-attr (shuffle (range max-attr))))
                 attr-names (map #(keyword (str "A" %)) (take n-attr (shuffle (range max-attr))))
                 ref-vals (map #(symbol (str "?r" %)) (take n-attr (range max-attr)))
                 attr-vals (map #(symbol (str "?res" %)) (take n-attr (range max-attr)))
                 join-clauses (map (fn [a v] '[?e ~a ~v]) ref-names ref-vals)
                 attr-clauses (map (fn [e a v] '[~e ~a ~v]) ref-vals attr-names attr-vals)]
             (println join-clauses)
             (println attr-clauses)
             (-> initital-query
                 (update :where conj (first join-clauses))
                 (update :where conj (first attr-clauses))
                 println)
             )))


(defn add-join-clauses [initital-query n-joins n-attr]
  (let [ref-names (map #(keyword (str "R" %)) (take n-joins (shuffle (range n-attr))))
        attr-names (map #(keyword (str "A" %)) (take n-joins (shuffle (range n-attr))))
        ref-symbols (map #(symbol (str "?r" %)) (take n-joins (range n-attr)))
        attr-symbols (map #(symbol (str "?rres" %)) (take n-joins (range n-attr)))
        join-clauses (map (fn [a ref] (conj '[?e] a ref)) ref-names ref-symbols)
        attr-clauses (map (fn [ref a v] (conj '[] ref a v)) ref-symbols attr-names attr-symbols)]
    (reduce (fn [query [res join-clause attr-clause]]
              (-> query
                  (update :find conj res)
                  (update :where conj join-clause)
                  (update :where conj attr-clause)))
            initital-query
            (map vector attr-symbols join-clauses attr-clauses))))

(defn add-direct-clauses [initital-query n-clauses n-attr]
  (let [attr-names (map #(keyword (str "A" %)) (take n-clauses (shuffle (range n-attr))))
        attr-symbols (map #(symbol (str "?ares" %)) (take n-clauses (range n-attr)))
        attr-clauses (map (fn [a v] (conj '[?e] a v)) attr-names attr-symbols)]
    (println attr-clauses)

    (reduce (fn [query [res attr-clause]]
              (-> query
                  (update :find conj res)
                  (update :where conj attr-clause)))
            initital-query
            (map vector attr-symbols attr-clauses))))


(add-join-clauses '{:find [?e] :in $ :where []} 2 10 )
(add-direct-clauses '{:find [?e] :in $ :where []} 2 10 )



(defn create-n-queries [n n-direct-vals n-ref-vals m]
  "Assumes database with entities of m direct and m reference attributes"
  (let [initial-query '{:find [?e] :in $ :where []}]
    (repeatedly n
                #(-> initial-query
                     (add-direct-clauses n-direct-vals m)
                     (add-join-clauses n-ref-vals m)
                     ))))

(create-n-queries 1 1 1 3)















(defn create-n-transactions [n]
  (->> (repeatedly #(str (UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))

(defn prepare-datahike [uri tx & args]
  (do
    (d/delete-database uri)
    (apply d/create-database uri args)
    (let [conn (d/connect uri)]
      (d/transact conn tx)
      (d/release conn))))

(defn prepare-datomic [uri schema tx]
  (do
    (da/delete-database uri)
    (da/create-database uri)
    (let [conn (da/connect uri)]
      @(da/transact conn schema)
      @(da/transact conn tx)
      conn)))


(def query '[:find ?name :in $])

(defn run-combinations [iterations]
  "Returns observation in following order:
   [:datoms :backend :mean :sd :schema-on-read :temporal-index]"
  (-> (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]
            uri uri/all]
        (let [tx (create-n-transactions d-count)]
          (if (not= "Datomic" (key uri))
            (for [sor [true false] ti [true false]]
              (let [conn (prepare-datahike (val uri) tx :schema-on-read sor :temporal-index ti :initial-tx (if sor schema []))
                    ti (measure-query-times iterations true (d/db conn) query)]
                [d-count (key uri) (is/mean ti) (is/sd ti) sor ti]))
            (let [conn (prepare-datomic (val uri) schema tx)
                  ti (measure-query-times iterations false (da/db conn) query)]
              [[d-count (key uri) (is/mean ti) (is/sd ti) false false]]))))
      concat
      vec))


(defn overall-means-datoms-plot [data]
  (ic/with-data data
                (doto
                  (charts/xy-plot
                    :datoms
                    :mean
                    :group-by :backend
                    :points true
                    :legend true
                    :title "transaction time vs. datoms in database of available datahike backends"
                    :y-label "transaction time (ms)")
                  (charts/set-axis :x (charts/log-axis :base 2, :label "Number of datoms in database")))))


(defn make-query-plots [config file-suffix]
  (let [result (run-combinations 256)                       ;; test!
        header [:datoms :backend :mean :sd :schema-on-read :temporal-index]
        data (ic/dataset (mapv (fn [observation] (zipmap header observation)) result)) ;; vector of maps
        means-plot (overall-means-datoms-plot data)]
    (ic/save data (str (:data-dir config) "/" (.format (:date-formatter config) (Date.)) "-" file-suffix ".dat"))
    (ic/save means-plot (str (:data-dir config) "/" (.format (:date-formatter config) (Date.)) "-" file-suffix ".png") :width 1000 :height 750)))