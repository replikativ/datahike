(ns datahike.db.interface)

;; Database Protocols

(defprotocol ISearch
  (-search-context [data])
  (-search [data pattern context]))

(defn search [data pattern]
  (-search data pattern (-search-context data)))

(defprotocol IIndexAccess
  (-datoms [db index components context])
  (-seek-datoms [db index components context])
  (-rseek-datoms [db index components context])
  (-index-range [db attr start end context]))

(defn datoms [db index components]
  (-datoms db index components (-search-context db)))

(defn seek-datoms [db index components]
  (-seek-datoms db index components (-search-context db)))

(defn rseek-datoms [db index components]
  (-rseek-datoms db index components (-search-context db)))

(defn index-range [db attr start end]
  (-index-range db attr start end (-search-context db)))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-system-entities [db])
  (-attrs-by [db property])
  (-max-tx [db])
  (-max-eid [db])
  (-temporal-index? [db]) ;;deprecated
  (-keep-history? [db])
  (-config [db])
  (-ref-for [db a-ident])
  (-ident-for [db a-ref]))

(defprotocol IHistory
  (-time-point [db])
  (-origin [db]))
