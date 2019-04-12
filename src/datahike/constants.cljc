(ns datahike.constants)

;; Datom
(def ^:const e0    0)
(def ^:const tx0   0x20000000)
(def ^:const emax  0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)
(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}})

;; Hitchhiker Tree
(def ^:const br 300)
(def ^:const br-sqrt (long (Math/sqrt br)))
