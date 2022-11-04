(ns tasks.version
  "Update the project version."
  (:require [utils.shell :refer [git with-opts]]
            [tasks.settings :refer [update-in-settings get-in-settings]]))

(defn commit-nr []
  (with-opts {:out :string}
    (:out (git "rev-list" "HEAD" "--count"))))

(defn version-str []
  (let [{:keys [major minor]} (get-in-settings [:version])]
    (str major "." minor "." (commit-nr))))

(defn increment [s]
  (case (name s)
    "major" (update-in-settings [:version :major] inc)
    "minor" (update-in-settings [:version :minor] inc)))

(defn show []
  (println (version-str)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn as-tag []
  (str "v" (version-str)))

(defn -main [& [s]]
  (if (some? s)
    (increment s)
    (show)))
