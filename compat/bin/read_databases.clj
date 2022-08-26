#!/usr/bin/env bb
(require '[babashka.process :refer [process check]])
(require '[clojure.string :as str])
(require '[babashka.fs :as fs])

(def minimal-minor-version 4)

(defn read-with-latest [version compat-folder]
  (println (format "Reading version %s with latest release..." version))
  (-> (process ["clj" "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \"..\"}}}") "-X:read-db" ":target-folder" (format "\"%s\"\"" compat-folder) ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-with-lib-version [version lib-version compat-folder]
  (println (format "Reading version %s with release %s..." version lib-version))
  (-> (process ["clj" "-Sdeps" (format "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}" lib-version) "-X:read-db" ":target-folder" (format "\"%s\"\"" compat-folder) ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-versions [compat-folder target-version]
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
       (let [[_ _ patch] (str/split release #"\.")]
         (if-not (contains? ignored-versions patch)
           (if (= "latest" target-version)
             (read-with-latest compat-folder release)
             (read-with-lib-version release target-version compat-folder))
           (println "Ignored version:" release)))))))

(let [[compat-folder target-version] *command-line-args*]
  (if (some? compat-folder)
    (if (fs/exists? compat-folder)
      (if (seq (fs/match compat-folder "regex:.*\\.export"))
        (read-versions compat-folder (or target-version "latest"))
        (prn (format "Folder %s does not contain any compat data." compat-folder)))
      (prn (format "Folder %s does not exist." compat-folder)))
    (prn "Please specify the compatibility folder.")))
