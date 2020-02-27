(ns performance.set-query
  (:require [performance.measure :refer [measure-query-times]]
            [performance.db :as db]
            [performance.uri :as uri]
            [performance.const :as c]
            [performance.schema :refer [make-col]]
            [incanter.io]
            [incanter.core :as ic])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))


(defn rand-str [length]
  (let [chars (map char (range 33 127))]
    (apply str (take length (repeatedly #(rand-nth chars))))))


(defn make-set-query-schema []
  (let [int-cols (mapv #(make-col (keyword (str "K" %)) :db.type/bigint)
                       [500000 250000 100000 40000 10000 1000 100 25 10 5 4 2])
        str-cols (mapv #(make-col (keyword (str "S" %)) :db.type/string)
                       [1 2 3 4 5 6 7 8])]
    (into [(make-col :KSEQ :db.type/bigint)]
          (concat int-cols str-cols))))


(defn make-set-query-entity [index]
  (let [int-cols (mapv #(vector (keyword (str "K" %)) (rand-int %))
                       [500000 250000 100000 40000 10000 1000 100 25 10 5 4 2])
        first-str-col [:S1 (rand-str 8)]
        str-cols (mapv #(vector (keyword (str "S" %)) (rand-str 20))
                       [2 3 4 5 6 7 8])]
    (into {:KSEQ index}
          (concat int-cols [first-str-col] str-cols))))


(def q4-conds
  [['[?e :K2 1]]
   ['[?e :K100 ?v1]
    '[(< 80 ?v1)]]
   ['[?e :K10000 ?v2]
    '[(< 2000 ?v2 3000)]]
   ['[?e :K5 3]]
   ['[(or [?e :K25 11]
          [?e :K25 19])]]
   ['[?e :K4 3]]
   ['[?e :K100 ?v5]
    '[(< ?v5 41)]]
   ['[?e :K1000 ?v4]
    '[(< 850 ?v4 950)]]
   ['[?e :K10 7]]
   ['[(or [?e :K25 3]
          [?e :K25 4])]]])


(def set-queries
  (let [count-query '{:find [(count ?e)] :in [$] :where []}
        sum-query '{:find [(sum ?s)] :with [?e] :in [$] :where []}
        res2-query '{:find [?res1 ?res2] :in [$] :where []}
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
                                  '[(< 400000 ?v 500000)]
                                  (conj '[?e] (keyword (str "K" %)) 3)))
             [500000 250000 100000 40000 10000 1000 100 25 10 5 4])

        (map #(hash-map
                :category "Q3b"
                :specific (str %)
                :query    (update sum-query :where conj
                                  '[?e :K1000 ?s]
                                  '[?e :KSEQ ?v]
                                  '[(or (< 400000 ?v 410000)
                                        (< 420000 ?v 430000)
                                        (< 440000 ?v 450000)
                                        (< 460000 ?v 470000)
                                        (< 480000 ?v 500000))]
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


(defn prepare-databases [uris n-entities]
  "Creates set query dbs and queries after 91 paper with entities of 21 attributes:
  - 1 attribute of sequential integers
  - 12 attributes of random integers within specified range
  - 8 attributes of random strings
    - 1 of length 8
    - 7 of length 20"
  (let [schema (make-set-query-schema)
        entities (mapv #(make-set-query-entity %) (range n-entities))]
    (for [uri uris
          :let [sor (:schema-on-read uri)
                ti (:temporal-index uri)]]
      (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) entities :schema-on-read sor :temporal-index ti))))


(defn run-combinations [iterations]
  "Returns observations in following order:
   [:backend :schema-on-read :temporal-index ::entities :mean :sd]"
  (let [uris [(first uri/all)]
        header [:backend :schema-on-read :temporal-index :entities :category :specific :mean :sd]
        res (for [n-entities [1000]]                            ;; use at least 1 Mio
              (let [connections (prepare-databases uris n-entities)]
                (apply concat (for [[conn uri] (map vector connections uris)
                                    :let [sor (:schema-on-read uri)
                                          ti (:temporal-index uri)
                                          db (db/db (:lib uri) conn)]]
                                (map (fn [query] (let [t (measure-query-times iterations (:lib uri) db #(identity (:query query)))]
                                                   (db/release (:lib uri) conn)
                                                   [(:name uri) sor ti n-entities (:category query) (:specific query) (:mean t) (:sd t)]))
                                     set-queries)))))]
    [header (apply concat res)]))


(defn get-set-query-times [file-suffix]
  (let [[header res] (run-combinations 10)
        data (ic/dataset header res)]
    (ic/save data (str c/data-dir "/" (.format c/date-formatter (Date.)) "-" file-suffix ".dat"))))


;;(get-set-query-times "set-query")