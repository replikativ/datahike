(ns tasks.settings
  (:require [clojure.edn :as edn]))

(def settings-file "settings.edn")

(defn load-settings []
  (edn/read-string (slurp settings-file)))

(defn update-in-settings [ks fn]
  (spit settings-file (update-in (load-settings) ks fn)))

(defn get-in-settings [ks]
  (get-in (load-settings) ks))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn show [& args]
  (println (if (seq args)
             (get-in-settings (vec args))
             (load-settings))))
