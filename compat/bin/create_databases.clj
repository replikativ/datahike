#!/usr/bin/env bb
(require '[babashka.process :refer [process check]])
(require '[clojure.string :as str])
(require '[babashka.fs :as fs])

(def default-minimal-minor-version "4")

(defn write-with-version [target-folder version]
  (println "Using version" version)
  (-> (process ["clj" "-Sdeps" (str "{:deps {io.replikativ/datahike {:mvn/version \"" version "\"}} }") "-X:write-db" ":target-folder" (format "\"%s\"\"" target-folder)] {:out :string})
      check
      :out
      println))

(defn write-versions [target-folder minimal-minor-version]
  (let [lib "io.replikativ/datahike"
        lines (-> (process ["clj" "-X:deps" "find-versions" ":lib" lib] {:out :string})
                  check
                  :out
                  str/split-lines)
        releases (map (comp :mvn/version read-string) lines)
        ignored-versions #{"1495"
                           "1496"
                           "1497"
                           "1498"
                           "1499"
                           "1500"
                           "1501"
                           "1502"
                           "1503"}]
    (doall
     (doseq [release releases]
       (let [[_ minor patch] (str/split release #"\.")]
         (if (<= (parse-long minimal-minor-version) (parse-long minor))
           (if-not (contains? ignored-versions patch)
             (write-with-version target-folder release)
             (println "Ignored version:" release))
           (println "Outdated version:" release)))))))

(let [[compat-folder minimal-version] *command-line-args*]
  (if (some? compat-folder)
    (if (fs/exists? compat-folder)
      (write-versions compat-folder (or minimal-version default-minimal-minor-version))
      (prn (format "Folder %s does not exist." compat-folder)))
    (prn "Please specify the compatibility folder.")))

