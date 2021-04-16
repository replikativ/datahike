(ns benchmark.compare
  (:require [clojure.edn :as edn]
            [clojure.string :refer [join]]))

(defn print-comparison [group filenames]
  (let [grouped (group-by :context group)
        unique-contexts (sort-by (juxt :function :db :db-size :tx-size) (keys grouped))
        unique-context-keys (->> (map keys unique-contexts) (apply concat) set vec)
        unique-context-key-spaces (map (fn [k] (max (count (name k))
                                                    (apply max (map #(count (str (get % k)))
                                                                    unique-contexts))))
                                       unique-context-keys)
        num-space 8
        col-width (+ 6 (* 3 num-space))
        titles (concat (map name unique-context-keys) filenames)
        title-spaces (concat unique-context-key-spaces (repeat (count filenames) col-width))
        subtitles (concat (repeat (count unique-context-keys) "")
                          (apply concat (repeat (count filenames)
                                                ["median" "mean" "std"])))
        subtitle-spaces (concat unique-context-key-spaces
                                (repeat (* 3 (count filenames)) num-space))
        dbl-fmt (str "%" num-space ".2f")
        str-fmt (fn [cw] (str "%-" cw "s"))]
    (println " Measurements (in s):")
    (println (str "  | " (join " | " (map #(format (str-fmt %2) %1)    titles title-spaces))    " |"))
    (println (str "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) title-spaces))    "-|"))
    (println (str "  | " (join " | " (map #(format (str-fmt %2) %1) subtitles subtitle-spaces)) " |"))
    (println (str "  |-" (join "-+-" (map (fn [s] (apply str (repeat s "-"))) subtitle-spaces)) "-|"))
    (dorun (for [[context results] (sort-by (fn [[{:keys [db db-size function tx-size]} _]] [function db-size tx-size db])
                                            grouped)]
             (let [times (map (fn [filename] (->> results (filter #(= (:source %) filename)) first :time))
                              filenames)]
               (println (str "  | " (join " | " (map #(format (str-fmt %2) (get context %1 "")) unique-context-keys unique-context-key-spaces))
                             " | " (join " | " (map #(str (format dbl-fmt (:median %)) " | " (format dbl-fmt (:mean %)) " | " (format dbl-fmt (:std %)))
                                                    times)) " |")))))
    (println "")))


(defn check-file-format [filename]
  (println "fn" filename)
  (when-not (.endsWith filename ".edn")
    (throw (Exception. (str "File " filename "has an unsupported file type. Supported are only edn files.")))))

(defn compare-benchmarks [filenames]
  (println "fns" filenames)
  (run! check-file-format filenames)

  (let [benchmarks (map (fn [filename]
                          (map #(assoc % :source filename)
                               (edn/read-string (slurp filename))))
                        filenames)

        grouped-benchmarks (->> (apply concat benchmarks)
                                (map #(assoc-in % [:context :db] (get-in % [:context :db :name])))
                                ; (group-by :context)
                                (group-by #(get-in % [:context :function])))]
    (println "gp" grouped-benchmarks)

    (println "Connection Statistics:")
    (print-comparison (:connection grouped-benchmarks) filenames)

    (println "Transaction Statistics:")
    (print-comparison (:transaction grouped-benchmarks) filenames)

    (println "Query Statistics:")
    (let [query-benchmarks (->> (dissoc grouped-benchmarks :connection :transaction)
                                vals
                                (apply concat))]
      (print-comparison query-benchmarks filenames))))
