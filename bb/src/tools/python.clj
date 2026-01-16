(ns tools.python
  "Python package utilities."
  (:require [clojure.string :as str]
            [tools.version :as version]))

(defn update-python-version!
  "Generate Python version file from config.edn"
  [config python-package-path]
  (let [version-str (version/string config)
        version-file-path (str python-package-path "/src/datahike/_version.py")
        content (str "\"\"\"Auto-generated version file from config.edn.\"\"\"\n"
                     "__version__ = \"" version-str "\"\n")]
    (spit version-file-path content)
    (println (str "Generated " version-file-path " with version: " version-str))))
