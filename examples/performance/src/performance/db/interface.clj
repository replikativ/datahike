(ns performance.db.interface)


(defmulti connect (fn [lib _] lib))

(defmulti transact (fn [lib _ _] lib))

(defmulti release (fn [lib _] lib))

(defmulti db (fn [lib _] lib))

(defmulti q (fn [lib _ _] lib))

(defmulti init (fn [lib _ _] lib))