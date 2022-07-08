(ns datahike.db.interface)

;; Database Protocols

(defprotocol ISearch
  (-search [data pattern]))

(defprotocol IIndexAccess
  (-datoms [db index components])
  (-seek-datoms [db index components])
  (-rseek-datoms [db index components])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-system-entities [db])
  (-attrs-by [db property])
  (-max-tx [db])
  (-max-eid [db])
  (-temporal-index? [db])                                   ;;deprecated
  (-keep-history? [db])
  (-config [db])
  (-ref-for [db a-ident])
  (-ident-for [db a-ref]))

(defprotocol IHistory
  (-time-point [db])
  (-origin [db]))
