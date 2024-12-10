(ns ^:no-doc datahike.index
  (:refer-clojure :exclude [-persistent! -flush -count -seq])
  (:require [datahike.index.interface :as di]
            [datahike.index.persistent-set]
            #?(:clj [datahike.index.hitchhiker-tree])))

;; Aliases for protocol functions

(def -all di/-all)
(def -seq di/-seq)
(def -count di/-count)
(def -insert di/-insert)
(def -temporal-insert di/-temporal-insert)
(def -upsert di/-upsert)
(def -temporal-upsert di/-temporal-upsert)
(def -remove di/-remove)
(def -slice di/-slice)
(def -flush di/-flush)
(def -transient di/-transient)
(def -persistent! di/-persistent!)
(def -mark di/-mark)

;; Aliases for multimethods

(def empty-index di/empty-index)
(def init-index di/init-index)
(def add-konserve-handlers di/add-konserve-handlers)
(def konserve-backend di/konserve-backend)
(def default-index-config di/default-index-config)

;; Other functions

(def index-types (keys (methods empty-index)))
