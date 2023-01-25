(ns tools.version
  "Update the project version."
  (:refer-clojure :exclude [inc])
  (:require [babashka.process :as p]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]))

(defn read-edn-file [filename]
  (edn/read-string (slurp filename)))

(defn commit-nr []
  (b/git-count-revs {:dir "."}))

(defn sha []
  (-> (p/shell {:out :string} "git" "log" "-1") 
      :out
      (str/split #"\s+")
      second))

(defn string [config]
  (let [{:keys [major minor]} (:version config)]
    (str major "." minor "." (commit-nr))))

(defn- update-in-settings [config-file ks fn]
  (spit config-file (update-in (read-edn-file config-file) ks fn)))

(defn show
  "Show the version string of a library"
  [config]
  (println (string config)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn inc
  "Increment the library version.
   The version part can be 'major' or 'minor'."
  ([config-file]
   (show (read-edn-file config-file)))
  ([config-file part]
   (case (name part)
     "major" (update-in-settings config-file [:version :major] inc)
     "minor" (update-in-settings config-file [:version :minor] inc))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn as-tag [config]
  (str "v" (string config)))
