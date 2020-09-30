(ns datahike.test.utils
  (:require [datahike.api :as d]))

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))
