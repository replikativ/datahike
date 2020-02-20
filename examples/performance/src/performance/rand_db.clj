(ns performance.rand-db
  (:require [datahike.api :as d]
            [performance.uri :as uri]
            [performance.const :as c]))


;;


(def identification-attribute
  {:db/ident       :randomEntity
   :db/valueType   :db.type/boolean
   :db/cardinality :db.cardinality/one})

(defn make-schema [ident-prefix type cardinality n-attributes]
  (mapv (fn [i] {:db/ident       (keyword (str ident-prefix i))
                 :db/valueType   type
                 :db/cardinality cardinality})
       (range n-attributes)))

(make-schema "A" :db.type/string :db.cardinality/one 10)


(defn make-entity [base-map ident-prefix n-attributes max-attribute value-list]
  (into base-map
        (map (fn [i v] [(keyword (str ident-prefix i)) v])
             (take n-attributes (shuffle (range max-attribute)))
             value-list)))


;;(make-entity {:randomEntity true} "A" 5 10 #(rand-int max-int))

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
  ([uri type value-generator n e]
   (create-value-ref-db uri type value-generator n n e))
  ([uri type value-generator n m e]
   (let [schema (into [identification-attribute]
                      (concat (make-schema "A" type :db.cardinality/one n)
                              (make-schema "R" :db.type/ref :db.cardinality/one n)))
         entities (mapv (fn [_] (make-entity {:randomEntity true} "A" m n
                                             (repeatedly m #(value-generator))))
                        (range e))
         _ (println entities)]
     (d/delete-database uri)
     (d/create-database uri :initial-tx schema)
     (let [conn (d/connect uri)]
       (d/transact conn entities)
       (let [ids (map first (d/q '[:find ?e :where [?e :randomEntity]] @conn))
             _ (println ids)
             add-to-entity (mapv (fn [id] (make-entity {:db/id id} "R" m n
                                                       (take m (shuffle (filter #(not= id %) ids)))))
                                 ids)
             _ (println add-to-entity)]
         (d/transact conn add-to-entity)
         ;; (println (mapv (fn [id] (str (d/datoms @conn :eavt id) "\n")) ids))
         (println (d/datoms (d/db conn) :eavt))
         (d/release conn))))))

(defn create-int-ref-db [uri n m e]
  (create-value-ref-db uri :db.type/bigint #(rand-int c/max-int) n m e))

(defn create-str-ref-db [uri n m e]
  (create-value-ref-db uri :db.type/string #(str (rand-int c/max-int)) n m e))



;;(create-int-ref-db (get uri/datahike "In-Memory") 10 2 4)
(create-int-ref-db (get uri/datahike "In-Memory") 5 5 10)

;;(create-str-ref-db (get uri/datahike "In-Memory") 10 2 4)