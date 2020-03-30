(ns performance.rand-query
  (:require [performance.db.api :as db]
            [performance.common :refer [make-attr short-error-report]]
            [performance.measure :refer [measure-query-times]]
            [performance.config :as c]
            [incanter.io]
            [incanter.core :as ic]
            [clojure.string :refer [split]]))


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


(defn create-query
  "Assumes database with entities of m direct and m reference attributes"
  [n-direct-vals n-ref-vals m]
  (let [initial-query '{:find [?e] :where []}]
    (-> initial-query
        (add-direct-clauses n-direct-vals m)
        (add-join-clauses n-ref-vals m))))

(defn make-hom-schema
  "Creates homogeneous database schema with attributes of a single type"
  [ident-prefix type cardinality n-attributes]
  (mapv (fn [i] (make-attr (keyword (str ident-prefix i)) type cardinality))
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
   (println " - thereof reference attributes:" n)
   (println " Number of attributes per entity:" (inc (* 2 m)))
   (println " - thereof reference attributes:" m)
   (println " Number of entities:" e)
   (let [schema (into [(make-attr :randomEntity :db.type/boolean)]
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
           (print " -> ")
           conn))))))


(defn create-value-ref-db
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
  ([uri type n e]                          ;; n=m -> entities have all possible attributes
   (create-value-ref-db uri type n n e))
  ([uri type n m e]
   (println "Set up database:" uri)
   (println " Type:" type)
   (println " Number of different attributes:" (inc (* 2 n)))
   (println " - thereof reference attributes:" n)
   (println " Number of attributes per entity:" (inc (* 2 m)))
   (println " - thereof reference attributes:" m)
   (println " Number of entities:" e)
   (time
     (let [schema (into [(make-attr :randomEntity :db.type/boolean)]
                        (concat (make-hom-schema "A" type :db.cardinality/one n)
                                (make-hom-schema "R" :db.type/ref :db.cardinality/one n)))
           entities (mapv (fn [_] (make-entity {:randomEntity true} "A" m n
                                               (repeatedly m #(create-value type))))
                          (range e))
           sor (:schema-on-read uri)
           ti (:temporal-index uri)
           conn (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) entities :schema-on-read sor :temporal-index ti)
           ids (map first (db/q (:lib uri)
                                '[:find ?e :where [?e :randomEntity]]
                                (db/db (:lib uri) conn)))
           add-to-entity (mapv (fn [id] (make-entity {:db/id id} "R" m n
                                                     (take m (shuffle (filter #(not= id %) ids)))))
                               ids)]
       (db/transact (:lib uri) conn add-to-entity)
       (print "   ")
       conn))))



(defn run-combinations-same-db                     ;; direct comparison on the same database across backends; causes out-of-memory exceptions
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index :n-attr :entities :dtype :n-clauses :n-joins :n-direct :mean :sd]"
  [uris iterations]
  (println "Getting random query times...")
  (let [header [:backend :schema-on-read :temporal-index :n-attr :entities :dtype :n-clauses :n-joins :n-direct :mean :sd]
        res (for [n-entities [1000]                                      ;; use at least 1 Mio, but takes too long
                  n-ref-attr [5 50 100]                               ;; until?
                  type [:db.type/long :db.type/string]
                  :let [n-attr (+ 1 (* 2 n-ref-attr))]]
              (let [conn-uri-map (map vector (create-value-ref-dbs uris type n-ref-attr n-entities) uris)
                    db-res (for [[conn uri] conn-uri-map
                                 n-clauses [(* 2 (min n-ref-attr 5))] ;; i.e. max 10 clauses
                                 n-joins (range n-clauses)
                                 :let [sor (:schema-on-read uri)
                                       ti (:temporal-index uri)
                                       dtype (last (split (str type) #"/"))
                                       n-direct-clauses (- n-clauses n-joins)
                                       db (db/db (:lib uri) conn)]]
                             (do
                               (println " Type:" dtype "Attributes per entity:" n-attr " Clauses with joins:" n-joins "of" n-clauses " Number of datoms:" n-entities " Uri:" uri)
                               (try
                                 (let [t (measure-query-times iterations (:lib uri) db #(create-query n-direct-clauses n-joins n-ref-attr))]
                                   (println "  Mean Time:" (:mean t) "ms")
                                   (println "  Standard deviation:" (:sd t) "ms")
                                   [(:name uri) sor ti n-attr n-entities dtype n-clauses n-joins n-direct-clauses (:mean t) (:sd t)])
                                 (catch Exception e (short-error-report e)))))]
                (doall (apply map #(db/release (:lib %2) %1) conn-uri-map))
                db-res))]
    [header (apply concat res)]))


(defn run-combinations
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index :n-attr :entities :dtype :n-clauses :n-joins :n-direct :mean :sd]"
  [uris iterations]
  (println "Getting random query times...")
  (let [header [:backend :schema-on-read :temporal-index :n-attr :entities :dtype :n-clauses :n-joins :n-direct :mean :sd]
        res (doall (for [n-entities [100]                   ;; use at least 1 Mio, but takes too long, 100 ok, 1000 to much
                         n-ref-attr [5 50 100]                            ;; until?
                         type [:db.type/long :db.type/string]
                         uri uris
                         :let [n-attr (+ 1 (* 2 n-ref-attr))
                               dtype (last (split (str type) #"/"))
                               sor (:schema-on-read uri)
                               ti (:temporal-index uri)]]
                     (do
                       (println " Type:" dtype "Attributes per entity:" n-attr " Number of datoms:" n-entities " Uri:" uri)
                       (try
                         (let [conn (create-value-ref-db uri type n-ref-attr n-entities)
                               db (db/db (:lib uri) conn)
                               db-res (doall (for [n-clauses [(* 2 (min n-ref-attr 5))] ;; i.e. max 10 clauses
                                                   n-joins (range (inc n-clauses))
                                                   :let [n-direct-clauses (- n-clauses n-joins)]]
                                               (do
                                                 (println "  Clauses with joins:" n-joins "of" n-clauses)
                                                 (try
                                                   (let [t (measure-query-times iterations (:lib uri) db #(create-query n-direct-clauses n-joins n-ref-attr))]
                                                     (println "   Mean Time:" (:mean t) "ms")
                                                     (println "   Standard deviation:" (:sd t) "ms")
                                                     [(:name uri) sor ti n-attr n-entities dtype n-clauses n-joins n-direct-clauses (:mean t) (:sd t)])
                                                   (catch Exception e (short-error-report e))))))]
                           (db/release (:lib uri) conn)
                           db-res)
                         (catch Exception e (short-error-report e))))))]
    [header (apply concat res)]
    ))


(defn get-rand-query-times [file-suffix]
  (let [[header res] (run-combinations c/uris 100)
        data (ic/dataset header (remove nil? res))]
    (print "Save random query times...")
    (ic/save data (c/filename file-suffix))
    (print " saved\n")))
