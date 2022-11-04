(ns tasks.lint
  (:require [pod.borkdude.clj-kondo :as clj-kondo]))

(defn -main []
  (clj-kondo/print! (clj-kondo/run! {:lint "."})))
