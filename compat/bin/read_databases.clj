#!/usr/bin/env bb
(require '[babashka.process :refer [process check]])
(require '[clojure.string :as str])

(def minimal-minor-version 4)

(defn read-with-latest [version]
  (println (format "Reading version %s with latest release..." version))
  (-> (process ["clj" "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \"..\"}}}") "-X:read-db" ":target-folder" "\"/home/konrad/data/datahike/compat\"" ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-with-lib-version [version lib-version]
  (println (format "Reading version %s with latest release..." version))
  (-> (process ["clj" "-Sdeps" (format "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}" lib-version) "-X:read-db" ":target-folder" "\"/home/konrad/data/datahike/compat\"" ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-versions []
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
             #_(read-with-latest release)
             (read-with-lib-version release "0.4.1494")
             (println "Ignored version:" release))
           (println "Outdated version:" release)))))))

(read-versions)
