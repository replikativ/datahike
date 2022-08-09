#!/usr/bin/env bb
(require '[babashka.process :refer [process check]])
(require '[clojure.string :as str])
(require '[babashka.fs :as fs])

(def minimal-minor-version 4)

(defn read-with-latest [version]
  (println (format "Reading version %s with latest release..." version))
  (-> (process ["clj" "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \"..\"}}}") "-X:read-db" ":target-folder" "\"/home/konrad/data/datahike/compat\"" ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-with-lib-version [version lib-version]
  (println (format "Reading version %s with release %s..." version lib-version))
  (-> (process ["clj" "-Sdeps" (format "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}" lib-version) "-X:read-db" ":target-folder" "\"/home/konrad/data/datahike/compat\"" ":version" (format "\"%s\"" version)] {:out :string})
      check
      :out
      println))

(defn read-versions [compat-folder target-version]
  (let [releases (->> (fs/match compat-folder  "regex:.*\\.export")
                      (map #(.getName (fs/file %)))
                      (map #(last (str/split % #"_")))
                      (map #(str/join "." (butlast (str/split % #"\."))))
                      sort)
        ignored-versions #{"1497"}]
    (doall
     (doseq [release releases]
       (let [[_ _ patch] (str/split release #"\.")]
         (if-not (contains? ignored-versions patch)
           (if (= "latest" target-version)
             (read-with-latest release)
             (read-with-lib-version release target-version))
           (println "Ignored version:" release)))))))

(let [[compat-folder target-version] *command-line-args*]
  (if (some? compat-folder)
    (if (fs/exists? compat-folder)
      (if (seq (fs/match compat-folder "regex:.*\\.export"))
        (read-versions compat-folder (or target-version "latest"))
        (prn (format "Folder %s does not contain any compat data." compat-folder)))
      (prn (format "Folder %s does not exist." compat-folder)))
    (prn "Please specify the compatibility folder.")))
