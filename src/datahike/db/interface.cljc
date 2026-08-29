(ns datahike.db.interface
  (:require [replikativ.logging :as log]))

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

(defprotocol ISecondaryView
  "Expose secondary generations through database views without treating those
   views as maps.

   `FilteredDB` deliberately rejects map lookup, while history/as-of/since
   wrappers do not own a `:secondary-indices` field.  Query execution must
   nevertheless preserve the wrapper's semantics instead of silently losing
   the index or reading the current generation as if it represented history.

   The returned map contains:

     :indices         the committed secondary generation map
     :system          {:mode :current|:history|:as-of|:since, ...}
     :filtered-depth  number of FilteredDB predicates around the source

   This protocol describes the database view only.  Whether an adapter can
   execute that view is decided by `datahike.index.secondary`."
  (-secondary-view [db]))

(defn ident-for [db a-ref missing-strategy]
  (or (-ident-for db a-ref)
      (case missing-strategy
        :error-on-missing (throw (ex-info "Attribute ref not found"
                                          {:ref a-ref}))
        :allow-missing nil)))

(defn ref-for
  "`a-ident` as this database stores it in a datom's attribute slot, with the
   caller stating what an UNKNOWN ident means to them.

   The mirror of `ident-for`, and it exists for the same reason: `-ref-for`
   answers `nil` for an ident this database has not installed, and `nil` is not a
   free value here. In a SEARCH PATTERN `nil` in the attribute slot means
   UNCONSTRAINED — match every attribute. So an unresolvable ident handed
   straight to a search does not match nothing, it matches EVERYTHING, and the
   caller cannot tell the two apart because they are the same value.

   That is not hypothetical. Under `:attribute-refs? true` it made
   `[?e :typo/attr ?v]` return the whole database and inverted `not-join` into
   excluding every result. Databases without `:attribute-refs?` were immune only
   by accident: there `-ref-for` is the identity and never produces a `nil`, so
   an unresolved keyword stays a keyword and matches nothing on its own.

   The strategies say what the CALLER means, which is the whole point — the bug
   was that nobody had to say:

     `:error-on-missing`  the attribute must exist; raise if it does not. What
                          the write paths want, and what 19 of the 23
                          `ident-for` call sites already ask for.
     `:no-match`          the attribute may not exist, and if it does not then
                          nothing can match it. What a QUERY wants: a clause over
                          an undeclared attribute yields no rows, which is what
                          this database has always done without `:attribute-refs?`
                          and is therefore what the two configs must agree on.
     `:allow-missing`     `nil`, the raw protocol answer, for a caller that has
                          its own handling. Named so that choosing it is visible.

   `:no-match` returns the IDENT itself. Under `:attribute-refs?` a stored
   attribute is an entity id, so a keyword is equal to none of them and orders
   against none of them — verified to match nothing across all six pattern shapes
   and both the current and temporal index families. Without `:attribute-refs?`
   the ident IS the attribute, so returning it is not a special case at all: the
   pattern is exactly what the caller wrote, and it matches nothing because no
   datom carries that attribute. One rule, both configs, no branch."
  [db a-ident missing-strategy]
  (or (-ref-for db a-ident)
      (case missing-strategy
        :error-on-missing (throw (ex-info "Attribute not found"
                                          {:error :db/unknown-attribute
                                           :attribute a-ident}))
        :no-match a-ident
        :allow-missing nil)))
