(ns tools.version
  "Update the project version."
  (:refer-clojure :exclude [inc])
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]))

(defn read-edn-file [filename]
  (edn/read-string (slurp filename)))

(defn commit-nr []
  (b/git-count-revs {:dir "."}))

(defn current-commit []
  (str/trim (b/git-process {:git-args "rev-parse HEAD"})))

(defn sha []
  (-> (b/git-process {:git-args "log -1"})
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

(defn inc
  "Increment the library version.
   The version part can be 'major' or 'minor'."
  ([config-file]
   (println "Current library version: ")
   (show (read-edn-file config-file))
   (println "Call this function with :major or :minor to increase the respective version part!"))
  ([config-file part]
   (case (name part)
     "major" (update-in-settings config-file [:version :major] inc)
     "minor" (update-in-settings config-file [:version :minor] inc))))

(defn as-tag [config]
  (str "v" (string config)))
