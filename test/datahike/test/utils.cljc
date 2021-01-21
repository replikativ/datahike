(ns datahike.test.utils
  (:require [datahike.api :as d]
            [clojure.core.async :refer [go <!]]))

(defn setup-db [cfg]
  #?(:cljs
     (go
       (<! (d/delete-database cfg))
       (<! (d/create-database cfg))
       (<! (d/connect cfg)))
     :clj
     (do
       (d/delete-database cfg)
       (d/create-database cfg)
       (d/connect cfg))))
