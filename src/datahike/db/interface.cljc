(ns datahike.db.interface)

;; Database Protocols

(defrecord SearchContext [historical temporal timepred xform currentdb])

(def base-context
  (map->SearchContext
   {:historical false
    :temporal false
    :timepred nil
    :xform nil
    :currentdb nil}))

(defn context-historical? [^SearchContext c]
  (.-historical c))

(defn context-temporal? [^SearchContext c]
  (.-temporal c))

(defn context-time-pred [^SearchContext c]
  (.-timepred c))

(defn context-xform [^SearchContext c]
  (.-xform c))

(defn context-current-db [^SearchContext c]
  (.-currentdb c))

(defn context-set-current-db-if-not-set [^SearchContext c db]
  (if (nil? (.-currentdb c))
    (SearchContext. (.-historical c)
                    (.-temporal c)
                    (.-timepred c)
                    (.-xform c)
                    db)
    c))

(defn- extend-pred [pred added-pred]
  (if (nil? pred)
    added-pred
    (fn [x] (and (pred x) (added-pred x)))))

(defn context-with-temporal-timepred [^SearchContext c timepred]
  (SearchContext. (.-historical c)
                  true
                  (extend-pred (.-timepred c) timepred)
                  (.-xform c)
                  (.-currentdb c)))

(defn nil-comp [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (comp a b)))

(defn context-with-xform-after [^SearchContext c xform]
  (SearchContext. (.-historical c)
                  (.-temporal c)
                  (.-timepred c)
                  (nil-comp (.-xform c) xform)
                  (.-currentdb c)))

(defn context-with-history [^SearchContext c]
  (SearchContext. true
                  true
                  (.-timepred c)
                  (.-xform c)
                  (.-currentdb c)))

(defprotocol ISearch
  (-search-context [data])
  (-search [data pattern context])
  (-batch-search [data pattern-mask batch-fn context]))

(defn search [data pattern]
  (-search data pattern (-search-context data)))

(defn batch-search [data pattern-mask batch-fn final-xform]
  (-batch-search data
                 pattern-mask
                 batch-fn
                 (context-with-xform-after (-search-context data)
                                           final-xform)))

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
