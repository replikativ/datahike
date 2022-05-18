(ns benchmark.compare
  (:require [clojure.edn :as edn]
            [clojure.string :refer [join]]
            [incanter.core :as ic]
            [incanter.charts :as charts]
            [benchmark.config :as c]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
  (:import [java.awt Color]))

(defn comparison-table [group filenames time-stats]
  (let [col-mapping (zipmap (map :name c/csv-cols)
                            c/csv-cols)
        grouped (group-by :context group)
        context-sort-fn (fn [context]
                          (mapv #(get-in context (vec (rest (:path (% col-mapping))))) ;; rest since already in :context
                                c/comparison-context-cell-order))
        sorted-contexts (sort-by context-sort-fn (keys grouped))
        unique-context-key-spaces (map (fn [k]
                                         (let [{:keys [title path]} (k col-mapping)]
                                           (->> sorted-contexts
                                                (map (fn [context]
                                                       (let [x (get-in context (vec (rest path)))]
                                                         (count (str (if (keyword? x)
                                                                       (name x)
                                                                       x))))))
                                                (cons (count title))
                                                (apply max))))
                                       c/comparison-context-cell-order)
        num-space 8
        col-width (+ (* 3 (- (count time-stats) 1))
                     (* (count time-stats) num-space))
        titles (concat (map #(get-in col-mapping [% :title])
                            c/comparison-context-cell-order)
                       filenames)
        title-spaces (concat unique-context-key-spaces (repeat (count filenames) col-width))
        subtitles (concat (repeat (count c/comparison-context-cell-order) "")
                          (apply concat (repeat (count filenames) time-stats)))
        subtitle-spaces (concat unique-context-key-spaces
                                (repeat (* (count time-stats) (count filenames))
                                        num-space))
        dbl-fmt (str "%" num-space ".2f")
        str-fmt (fn [cw] (str "%-" cw "s"))]
    (str "  | " (join " | " (map #(format (str-fmt %2) %1)    titles title-spaces))    " |\n"
         "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) title-spaces))    "-|\n"
         "  | " (join " | " (map #(format (str-fmt %2) %1) subtitles subtitle-spaces)) " |\n"
         "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) subtitle-spaces)) "-|\n"
         (join "\n" (for [[context results] (sort-by #(context-sort-fn (key %)) grouped)]
                      (let [times (map (fn [filename] (->> results (filter #(= (:source %) filename)) first :time))
                                       filenames)
                            context-cells (map (fn [context-key col-width]
                                                 (let [context-val (get-in context (vec (rest (:path (context-key col-mapping)))) "")]
                                                   (format (str-fmt col-width)
                                                           (if (keyword? context-val)
                                                             (name context-val)
                                                             context-val))))
                                               c/comparison-context-cell-order
                                               unique-context-key-spaces)
                            measurement-cells (map (fn [measurements] (join " | " (map (fn [stat] (format dbl-fmt (stat measurements)))
                                                                                       time-stats)))
                                                   times)]
                        (str "  | " (join " | " (concat context-cells measurement-cells)) " |"))))
         "\n")))


(defn check-file-format [filename]
  (when-not (.endsWith filename ".edn")
    (throw (Exception. (str "File " filename "has an unsupported file type. Supported are only edn files.")))))

(defn report [benchmarks filenames time-stats]
  (let [grouped-benchmarks (group-by #(get-in % [:context :function]) benchmarks)
        output (str (when-let [conn-benchmarks (:connection grouped-benchmarks)]
                      (str "Connection Measurements (in s):\n"
                           (comparison-table conn-benchmarks filenames time-stats)
                           "\n"))
                    (when-let [tx-benchmarks (:transaction grouped-benchmarks)]
                      (str "Transaction Measurements (in s):\n"
                           (comparison-table tx-benchmarks filenames time-stats)
                           "\n"))
                    (when-let [query-benchmarks (->> (dissoc grouped-benchmarks :connection :transaction)
                                                     vals
                                                     (apply concat))]
                      (str "Query Measurements (in s):\n"
                           (comparison-table query-benchmarks filenames time-stats))))]
    (println output)))

(defn create-plots [data] ;; 1 plot per function and context
  (let [tags (join "," (distinct (map :tag data)))
        colors [Color/red Color/blue Color/green Color/cyan Color/magenta Color/orange Color/yellow]
        directory (str "plots-" tags)]
    
  (doall (for [function (distinct (map #(get-in % [:context :function]) data))
               config (distinct (map #(get-in % [:context :dh-config]) data))
               execution-details (distinct (map #(get-in % [:context :execution]) data))]
           (let [_ (log/debug (str "Processing " function " " config " " execution-details))
                 {:keys [tx-entities data-type data-in-db?]} execution-details
                 filename (str (name function)
                               "_config-" config
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
                                              (= (get-in % [:context :dh-config]) config)
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
                (charts/set-stroke-color plot (Color. 0 0 0 0) :dataset 0)
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

(defn compare-benchmarks [filenames plots? time-stats]
  (run! check-file-format filenames)

  (let [benchmarks (->> filenames
                        (map (fn [filename]
                          (map #(assoc % :source filename)
                               (edn/read-string (slurp filename)))))
                        (apply concat))]
    (if plots?
      (create-plots benchmarks)
      (report benchmarks filenames time-stats))))
