(ns benchmark.compare
  (:require [clojure.edn :as edn]
            [clojure.string :refer [join]]
            [benchmark.config :as c]))

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

(defn compare-benchmarks [filenames]
  (run! check-file-format filenames)

  (let [benchmarks (map (fn [filename]
                          (map #(assoc % :source filename)
                               (edn/read-string (slurp filename))))
                        filenames)
        grouped-benchmarks (->> (apply concat benchmarks)
                                (map #(assoc % :context
                                             (merge (dissoc (:context %) :execution)
                                                    {:dh-config (get-in % [:context :dh-config :name])}
                                                    (get (:context %) :execution))))
                                (group-by #(get-in % [:context :function])))
        output (str "Connection Measurements (in s):\n"
                    (comparison-table (:connection grouped-benchmarks) filenames)
                    "\n"
                    "Transaction Measurements (in s):\n"
                    (comparison-table (:transaction grouped-benchmarks) filenames)
                    "\n"
                    "Query Measurements (in s):\n"
                    (let [query-benchmarks (->> (dissoc grouped-benchmarks :connection :transaction)
                                                vals
                                                (apply concat))]
                      (comparison-table query-benchmarks filenames)))]
    (println output)))
