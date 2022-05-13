#!/usr/bin/env bb
(require '[babashka.process :refer [process check]])
(require '[clojure.string :as str])

(def minimal-minor-version 4)

(defn write-with-version [version]
  (println "Using version" version)
  (-> (process ["clj" "-Sdeps" (str "{:deps {io.replikativ/datahike {:mvn/version \"" version "\"}} }") "-X:write-db" ":target-folder" "\"/home/konrad/data/datahike/compat\""] {:out :string})
      check
      :out
      println))

(defn write-versions []
  (let [lib "io.replikativ/datahike"
        lines (-> (process ["clj" "-X:deps" "find-versions" ":lib" lib] {:out :string})
                  check
                  :out
                  str/split-lines)
        releases (map (comp :mvn/version read-string) lines)
        ignored-versions #{"1497"}]
    (doall
     (doseq [release releases]
       (let [[_ minor patch] (str/split release #"\.")]
         (if (<= minimal-minor-version (parse-long minor))
           (if-not (contains? ignored-versions patch)
             (write-with-version release)
             (println "Ignored version:" release))
           (println "Outdated version:" release)))))))

(write-versions)
