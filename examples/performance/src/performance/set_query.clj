(ns performance.set-query
  (:require [performance.measure :refer [measure-query-times]]
            [performance.db.api :as db]
            [performance.config :as c]
            [performance.common :refer [make-attr short-error-report]]
            [incanter.io]
            [incanter.core :as ic])
  (:import (java.util Date)))


(def q4-conds
  [['[?e :K2 1]]
   ['[?e :K100 ?v1]
    '[(< 80 ?v1)]]
   ['[?e :K10000 ?v2]
    '[(< 2000 ?v2)]
    '[(< ?v2 3000)]]
   ['[?e :K5 3]]
   ['(or [?e :K25 11]
         [?e :K25 19])]
   ['[?e :K4 3]]
   ['[?e :K100 ?v5]
    '[(< ?v5 41)]]
   ['[?e :K1000 ?v4]
    '[(< 850 ?v4)]
    '[(< ?v4 950)]]
   ['[?e :K10 7]]
   ['(or [?e :K25 3]
         [?e :K25 4])]])


(def set-queries
  (let [count-query '{:find [(count ?e)] :in [$] :where []}
        sum-query '{:find [(sum ?s)] :with [?e] :in [$] :where []}
        res2-query '{:find [?res1 ?res2] :with [?e] :in [$] :where []}
        res2-count-query '{:find [?res1 ?res2 (count ?e)] :in [$] :where []}] ;; implicit grouping

    (concat
      (map #(hash-map
              :category "Q1"
              :specific (str %)
              :query    (update count-query :where conj
                                (conj '[?e] (keyword (str "K" %)) 2)))
           [500000 250000 100000 40000 10000 1000 100 25 10 5 4 2])

      [{:category "Q1"
        :specific "seq"
        :query    '{:find [(count ?e)] :in [$] :where [[?e :KSEQ 2]]}}]

      (map #(hash-map
              :category "Q2a"
              :specific (str %)
              :query    (update count-query :where conj
                                '[?e :K2 2]
                                (conj '[?e] (keyword (str "K" %)) 3)))
           [500000 250000 100000 40000 10000 1000 100 25 10 5 4])


      (map #(hash-map
              :category "Q2b"
              :specific (str %)
              :query    (update count-query :where conj
                                '[?e :K2 2]
                                (conj '() (conj '[?e] (keyword (str "K" %)) 3) 'not)))
           [500000 250000 100000 40000 10000 1000 100 25 10 5 4])


      (map #(hash-map
              :category "Q3a"
              :specific (str %)
              :query    (update sum-query :where conj
                                '[?e :K1000 ?s]
                                '[?e :KSEQ ?v]
                                '[(< 400000 ?v)]
                                '[(< ?v 500000)] ;; [(< 400000 ?v 500000)] not possible in datomic
                                (conj '[?e] (keyword (str "K" %)) 3)))
           [500000 250000 100000 40000 10000 1000 100 25 10 5 4])

      (map #(hash-map
              :category "Q3b"
              :specific (str %)
              :query    (update sum-query :where conj
                                '[?e :K1000 ?s]
                                '[?e :KSEQ ?v]
                                '(or
                                   (and [(< 400000 ?v)]
                                        [(< ?v 410000)])
                                   (and [(< 420000 ?v)]
                                        [(< ?v 430000)])
                                   (and [(< 440000 ?v)]
                                        [(< ?v 450000)])
                                   (and [(< 460000 ?v)]
                                        [(< ?v 470000)])
                                   (and [(< 480000 ?v)]
                                        [(< ?v 500000)]))
                                (conj '[?e] (keyword (str "K" %)) 3)))
           [500000 250000 100000 40000 10000 1000 100 25 10 5 4])


      (map #(hash-map
              :category "Q4"
              :specific (str % "-" (+ % 2))
              :query    (update res2-query :where (partial apply conj)
                                '[?e :KSEQ ?res1]
                                '[?e :K500000 ?res2]
                                (into '[] (apply concat (subvec q4-conds % (+ % 3))))))
           (range 7))
      (map #(hash-map
              :category "Q4"
              :specific (str % "-" (+ % 4))
              :query    (update res2-query :where (partial apply conj)
                                '[?e :KSEQ ?res1]
                                '[?e :K500000 ?res2]
                                (into '[] (apply concat (subvec q4-conds % (+ % 5))))))
           (range 5))


      (map (fn [[kn1 kn2]] {:category "Q5"
                            :specific (str "(" kn1 " " kn2 ")")
                            :query    (update res2-count-query :where conj
                                              (conj '[?e] (keyword (str "K" kn1)) '?res1)
                                              (conj '[?e] (keyword (str "K" kn2)) '?res2))})
           [[2 100] [4 25] [10 25]])


      (map #(hash-map
              :category "Q6a"
              :specific (str %)
              :query    (update count-query :where conj
                                (conj '[?e] (keyword (str "K" %)) 49)
                                '[?e :K250000 ?v1]
                                '[?e2 :K500000 ?v2]
                                '[(= ?v1 ?v2)]))
           [100000 40000 10000 1000 100])

      (map #(hash-map
              :category "Q6b"
              :specific (str %)
              :query    (update res2-query :where conj
                                (conj '[?e] (keyword (str "K" %)) 99)
                                '[?e :KSEQ ?res1]
                                '[?e :K250000 ?v1]
                                '[?e2 :K25 19]
                                '[?e2 :KSEQ ?res2]
                                '[?e2 :K500000 ?v2]
                                '[(= ?v1 ?v2)]))
           [40000 10000 1000 100]))))


(defn rand-str [length]
  (let [chars (map char (range 33 127))]
    (apply str (take length (repeatedly #(rand-nth chars))))))


(defn make-set-query-schema []
  (let [int-cols (mapv #(make-attr (keyword (str "K" %)) :db.type/long)
                       [500000 250000 100000 40000 10000 1000 100 25 10 5 4 2])
        str-cols (mapv #(make-attr (keyword (str "S" %)) :db.type/string)
                       [1 2 3 4 5 6 7 8])]
    (into [(make-attr :KSEQ :db.type/long)]
          (concat int-cols str-cols))))


(defn make-set-query-entity [index]
  (let [int-cols (mapv #(vector (keyword (str "K" %)) (long (rand-int %)))
                       [500000 250000 100000 40000 10000 1000 100 25 10 5 4 2])
        first-str-col [:S1 (rand-str 8)]
        str-cols (mapv #(vector (keyword (str "S" %)) (rand-str 20))
                       [2 3 4 5 6 7 8])]
    (into {:KSEQ index}
          (concat int-cols [first-str-col] str-cols))))


(defn prepare-databases
  "Creates set query dbs and queries after 91 paper with entities of 21 attributes:
  - 1 attribute of sequential integers
  - 12 attributes of random integers within specified range
  - 8 attributes of random strings
    - 1 of length 8
    - 7 of length 20"
  [uris n-entities]
  (let [schema (make-set-query-schema)
        entities (mapv #(make-set-query-entity %) (range n-entities))]
    (for [uri uris
          :let [sor (:schema-on-read uri)
                ti (:temporal-index uri)]]
      (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) entities :schema-on-read sor :temporal-index ti))))


(defn run-combinations-same-db             ;; can easily cause out-of-memory exceptions
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index ::entities :mean :sd]"
  [uris iterations]
  (println "Getting set query times...")
  (let [header [:backend :schema-on-read :temporal-index :entities :category :specific :mean :sd]
        res (for [n-entities [1000000]]                            ;; use at least 1 Mio
              (try
                (let [conn-uri-map (map vector (prepare-databases uris n-entities) uris)
                      db-res (doall (for [[conn uri] conn-uri-map
                                          query set-queries
                                          :let [sor (:schema-on-read uri)
                                                ti (:temporal-index uri)
                                                db (db/db (:lib uri) conn)]]
                                      (do
                                        (println " Query:" (:category query) " Number of datoms:" n-entities " Uri:" uri)
                                        (try
                                          (let [t (measure-query-times iterations (:lib uri) db #(identity (:query query)))]
                                            (println "  Mean Time:" (:mean t) "ms")
                                            (println "  Standard deviation:" (:sd t) "ms")
                                            [(:name uri) sor ti n-entities (:category query) (:specific query) (:mean t) (:sd t)])
                                          (catch Exception e (short-error-report e))))))]
                  (doall (apply map #(db/release (:lib %2) %1) conn-uri-map))
                  db-res)
                (catch Exception e (short-error-report e))))]
    [header (apply concat res)]))

(defn run-combinations
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index ::entities :mean :sd]"
  [uris iterations]
  (println "Getting set query times...")
  (let [header [:backend :schema-on-read :temporal-index :entities :category :specific :mean :sd]
        schema (make-set-query-schema)
        res (doall (for [n-entities [1000]  ;; use at least 1 Mio ;; 1000 plausible
                         uri uris
                         :let [sor (:schema-on-read uri)
                               ti (:temporal-index uri)]]
                     (try
                       (let [entities (mapv #(make-set-query-entity %) (range n-entities))
                             conn (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) entities :schema-on-read sor :temporal-index ti)
                             db (db/db (:lib uri) conn)
                             db-res (doall (for [query set-queries]
                                             (do
                                               (println " Query:" (:category query) "(" (:specific query) ")" " Number of datoms:" n-entities " Uri:" uri)
                                               (try
                                                 (let [t (measure-query-times iterations (:lib uri) db #(identity (:query query)))]
                                                   (println "  Mean Time:" (:mean t) "ms")
                                                   (println "  Standard deviation:" (:sd t) "ms")
                                                   [(:name uri) sor ti n-entities (:category query) (:specific query) (:mean t) (:sd t)])
                                                 (catch Exception e (short-error-report e))))))]
                         (db/release (:lib uri) conn)
                         db-res)
                       (catch Exception e (short-error-report e)))))]
    [header (apply concat res)]))


(defn get-set-query-times [file-suffix]
  (let [[header res] (run-combinations c/uris 100)
        data (ic/dataset header (remove nil? res))]
    (print "Save set query times...")
    (ic/save data (c/filename file-suffix))
    (print " saved\n")))
