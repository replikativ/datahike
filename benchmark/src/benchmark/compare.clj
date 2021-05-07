(ns benchmark.compare
  (:require [clojure.edn :as edn]
            [clojure.string :refer [join]]
            [incanter.core :as ic]
            [incanter.charts :as charts]
            [incanter.stats :refer [quantile]]
            [benchmark.config :as c]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
  (:import [java.awt Color]))

(defn comparison-table [group filenames]
  (let [grouped (group-by :context group)
        unique-contexts (sort-by (apply juxt c/context-cell-order) (keys grouped))
        unique-context-key-spaces (map (fn [k] (max (count (name k))
                                                    (apply max (map #(count (str (get % k)))
                                                                    unique-contexts))))
                                       c/context-cell-order)
        num-space 8
        col-width (+ 6 (* 3 num-space))
        titles (concat (map name c/context-cell-order) filenames)
        title-spaces (concat unique-context-key-spaces (repeat (count filenames) col-width))
        subtitles (concat (repeat (count c/context-cell-order) "")
                          (apply concat (repeat (count filenames)
                                                ["median" "mean" "std"])))
        subtitle-spaces (concat unique-context-key-spaces
                                (repeat (* 3 (count filenames)) num-space))
        dbl-fmt (str "%" num-space ".2f")
        str-fmt (fn [cw] (str "%-" cw "s"))]
    (str "  | " (join " | " (map #(format (str-fmt %2) %1)    titles title-spaces))    " |\n"
         "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) title-spaces))    "-|\n"
         "  | " (join " | " (map #(format (str-fmt %2) %1) subtitles subtitle-spaces)) " |\n"
         "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) subtitle-spaces)) "-|\n"
         (join "\n" (for [[context results] (sort-by #((apply juxt c/context-cell-order) (key %))
                                                     grouped)]
                      (let [times (map (fn [filename] (->> results (filter #(= (:source %) filename)) first :time))
                                       filenames)
                            context-cells (map #(format (str-fmt %2) (get context %1 ""))
                                               c/context-cell-order unique-context-key-spaces)
                            measurement-cells (map (fn [measurements] (join " | " (map (fn [stat] (format dbl-fmt (stat measurements)))
                                                                                       [:median :mean :std])))
                                                   times)]
                        (str "  | " (join " | " (concat context-cells measurement-cells)) " |"))))
         "\n")))


(defn check-file-format [filename]
  (when-not (.endsWith filename ".edn")
    (throw (Exception. (str "File " filename "has an unsupported file type. Supported are only edn files.")))))

(defn report [benchmarks filenames]
  (let [grouped-benchmarks (->> benchmarks
                                (map #(assoc % :context
                                             (merge (dissoc (:context %) :execution)
                                                    {:dh-config (get-in % [:context :dh-config :name])}
                                                    (get (:context %) :execution))))
                                (group-by #(get-in % [:context :function])))
        output (str (when-let [conn-benchmarks (:connection grouped-benchmarks)]
                      (str "Connection Measurements (in s):\n"
                           (comparison-table conn-benchmarks filenames)
                           "\n"))
                    (when-let [tx-benchmarks (:transaction grouped-benchmarks)]
                      (str "Transaction Measurements (in s):\n"
                           (comparison-table tx-benchmarks filenames)
                           "\n"))
                    (when-let [query-benchmarks (->> (dissoc grouped-benchmarks :connection :transaction)
                                                     vals
                                                     (apply concat))]
                      (str "Query Measurements (in s):\n"
                           (comparison-table query-benchmarks filenames))))]
    (println output)))

(defn create-plots [data] ;; 1 plot per function and context
  (let [tags (join "," (distinct (map :tag data)))
        colors [Color/red Color/blue Color/green Color/cyan Color/magenta Color/orange Color/yellow]
        directory (str "plots-" tags)]
    
  (doall (for [function (distinct (map #(get-in % [:context :function]) data))
               config (distinct (map #(get-in % [:context :dh-config :name]) data))
               execution-details (distinct (map #(get-in % [:context :execution]) data))]
           (let [_ (log/debug (str "Processing " function " " config " " execution-details))
                 {:keys [tx-entities data-type data-in-db?]} execution-details
                 filename (str (name function)
                               "_config-" (name config)
                               (when tx-entities
                                (str "_tx-ents-" tx-entities))
                               (when data-type
                                (str "_type-" (name data-type)))
                               (when data-in-db?
                                 (str "_found-" data-in-db?))
                               "_tags-" tags
                               ".png")

                 plot-data (->> data
                                (filter #(and (= (get-in % [:context :function]) function)
                                              (= (get-in % [:context :dh-config :name]) config)
                                              (= (get-in % [:context :execution]) execution-details)))
                                (map #(assoc % :x (get-in % [:context :db-entities]))))]
              (when (seq plot-data)
                (let [filepath (str directory "/" filename)
                      all-y (apply concat (map #(get-in % [:time :observations]) plot-data))
                      ymin 0
                      ymax (apply max all-y)
                     ; ymax (quantile all-y :probs 0.9)
                      plot (charts/scatter-plot nil nil
                                                :title  (str "Execution time of function " function "\n"
                                                             "for configuration " (name config) " and\n"
                                                             "execution parameters " execution-details)
                                                :y-label "Time (s)"
                                                :x-label (str "Entities in database (1 entity = " (count c/schema) " datoms)")
                                                :legend true
                                                :series-label "")]
                (charts/set-stroke-color plot (java.awt.Color. 0 0 0 0) :dataset 0)
                (charts/set-y-range plot ymin ymax)
                (doall (map-indexed (fn [idx [tag group]]
                        (let [sorted (sort-by :x group)
                              entities (map :x sorted)
                              time (map :time sorted)
                              time-obs (apply concat (map :observations time))
                              entities-rep (apply concat (map #(repeat (count (:observations %2)) %1) entities time))]
                          
                        (charts/add-points plot entities-rep time-obs :series-label tag)
                        (charts/set-stroke-color plot (get colors idx) :dataset (+ 1 (* idx 2)))
                        (charts/add-lines plot entities (map :median time) :series-label (str tag " median")))
                        (charts/set-stroke-color plot (get colors idx) :dataset (+ 2 (* idx 2))))
                          (group-by :tag plot-data)))
                  (when-not (.exists (io/file directory))
                    (.mkdir (io/file directory)))
                  (ic/save plot filepath
                           :width 800
                           :height 600))))))))

(defn compare-benchmarks [filenames plots?]
  (run! check-file-format filenames)

  (let [benchmarks (->> filenames
                        (map (fn [filename]
                          (map #(assoc % :source filename)
                               (edn/read-string (slurp filename)))))
                        (apply concat))]
    (if plots?
      (create-plots benchmarks)
      (report benchmarks filenames))))
