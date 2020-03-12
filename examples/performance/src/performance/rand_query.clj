(ns performance.rand-query
  (:require [performance.db :as db]
            [performance.schema :refer [make-col]]
            [performance.measure :refer [measure-query-times]]
            [performance.conf :as c]
            [performance.error :as e]
            [incanter.io]
            [incanter.core :as ic])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))


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
    ;;(println " Clauses:" attr-clauses)
    (reduce (fn [query [res attr-clause]]
              (-> query
                  (update :find conj res)
                  (update :where conj attr-clause)))
            initital-query
            (map vector attr-symbols attr-clauses))))


(defn create-value [type]
  (case type
    :db.type/long (long (rand-int c/max-int))
    :db.type/string (str (rand-int c/max-int))))


(defn create-query [n-direct-vals n-ref-vals m]
  "Assumes database with entities of m direct and m reference attributes"
  (let [initial-query '{:find [?e] :where []}]
    (-> initial-query
        (add-direct-clauses n-direct-vals m)
        (add-join-clauses n-ref-vals m))))

(defn make-hom-schema [ident-prefix type cardinality n-attributes]
  "Creates homogeneous database schema with attributes of a single type"
  (mapv (fn [i] (make-col (keyword (str ident-prefix i)) type cardinality))
        (range n-attributes)))


(defn make-entity [base-map ident-prefix n-attributes max-attribute value-list]
  (into base-map
        (map (fn [i v] [(keyword (str ident-prefix i)) v])
             (take n-attributes (shuffle (range max-attribute)))
             value-list)))


(defn create-value-ref-dbs
  "Creates db of
   - 2n+1 different attributes (m <= n)
     - n attributes of given type
     - n reference attributes
     - 1 attribute to identify non-schema entities
   - e entities with
     - 2m+1 values (m <= e)
       - m direct attributes
       - m reference attributes
       - 1 attribute identifying it as non-schema entity"
  ([uris type n e]                          ;; n=m -> entities have all possible attributes
   (create-value-ref-dbs uris type n n e))
  ([uris type n m e]
   (println "Set up databases...")
   (println " Type:" type)
   (println " Number of different attributes:" (inc (* 2 n)))
   (println " Number of entities:" e)
   (println " Number of attributes per entity:" (inc (* 2 m)))
   (let [schema (into [(make-col :randomEntity :db.type/boolean)]
                      (concat (make-hom-schema "A" type :db.cardinality/one n)
                              (make-hom-schema "R" :db.type/ref :db.cardinality/one n)))
         entities (mapv (fn [_] (make-entity {:randomEntity true} "A" m n
                                             (repeatedly m  #(create-value type))))
                        (range e))]
     (println " Schema:" schema)
     (println " Entities:" entities)
     (for [uri uris
           :let [sor (:schema-on-read uri)
                 ti (:temporal-index uri)]]
       (time
         (let [_ (println " Uri:" uri)
               conn (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) entities :schema-on-read sor :temporal-index ti)
               ids (map first (db/q (:lib uri)
                                    '[:find ?e :where [?e :randomEntity]]
                                    (db/db (:lib uri) conn)))
                _ (println "  IDs:" ids)
               add-to-entity (mapv (fn [id] (make-entity {:db/id id} "R" m n
                                                         (take m (shuffle (filter #(not= id %) ids)))))
                                   ids)]
           (println "  Values to add:" add-to-entity)
           (db/transact (:lib uri) conn add-to-entity)
           (print "   ")
           conn))))))


(defn run-combinations [uris iterations]
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index :n-attr :entities :n-clauses :n-joins :n-direct :mean :sd]"
  (println "Getting random query times...")
  (let [header [:backend :schema-on-read :temporal-index :n-attr :entities :n-clauses :n-joins :n-direct :mean :sd]
        res (for [n-entities [10000]                                      ;; use at least 1 Mio, but takes too long
                  n-ref-attr [5 10 50 100]                            ;;[5 10 50 100]                               ;; until?
                  type [:db.type/long :db.type/string]
                  :let [n-attr (+ 1 (* 2 n-ref-attr))]]
              (let [connections (create-value-ref-dbs uris type n-ref-attr n-entities)]
                (println " Connections established for:" " n-entities;" n-entities " n-ref-attr:" n-ref-attr)
                (println connections)
                (for [[conn uri] (map vector connections uris)
                      n-clauses [(* 2 (min n-ref-attr 5))]  ;; max 10 clauses
                      n-joins (range n-clauses)
                      :let [sor (:schema-on-read uri)
                            ti (:temporal-index uri)
                            n-direct-clauses (- n-clauses n-joins)]]
                  (try
                    (let [_ (println " Type:" type "Attributes per entity:" n-attr " Clauses with joins:" n-joins "of" n-clauses " Number of datoms:" n-entities  " Uri:" uri)
                          db (db/db (:lib uri) conn)
                          t (measure-query-times iterations (:lib uri) db #(create-query n-direct-clauses n-joins n-ref-attr))]
                      (db/release (:lib uri) conn)
                      (println "  Mean Time:" (:mean t) "ms")
                      (println "  Standard deviation:" (:sd t) "ms")
                      [(:name uri) sor ti n-attr n-entities n-clauses n-joins n-direct-clauses (:mean t) (:sd t)])
                    (catch Exception e (e/short-report e)
                      )))))]
    [header (apply concat res)]))


(defn get-rand-query-times [file-suffix]
  (let [[header res] (run-combinations (remove #(contains? #{"LevelDB" "Datomic Mem" "Datomic Free"} (:name %)) c/uris) 10) ;; error "connection released" for datomic
        data (ic/dataset header (remove nil? res))]
    (print "Save random query times...")
    (ic/save data (str c/data-dir "/" (.format c/date-formatter (Date.)) "-" file-suffix ".dat"))
    (print " saved\n")))


(get-rand-query-times "rand-query")



























