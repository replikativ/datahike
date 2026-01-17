(ns tools.examples
  "Tools for managing example projects"
  (:require [clojure.string :as str]
            [tools.version :as version]))

(defn update-java-examples-version!
  "Update the datahike.version property in examples/java/pom.xml to match current version"
  [config]
  (let [pom-file "examples/java/pom.xml"
        current-version (version/string config)
        pom-content (slurp pom-file)
        updated-content (str/replace
                         pom-content
                         #"<datahike\.version>[^<]+</datahike\.version>"
                         (str "<datahike.version>" current-version "</datahike.version>"))]
    (when (not= pom-content updated-content)
      (println (str "Updating examples/java/pom.xml datahike.version to " current-version))
      (spit pom-file updated-content)
      true)))
