(ns datahike.bitemporal.platform
  "Cross-platform helpers for the valid-time read-side API.

   The bitemporal predicate machinery in `datahike.api.impl` and the
   write-side validation in `datahike.db.transaction` both rely on two
   JVM-only constructs:

     * `java.util.concurrent.ConcurrentHashMap` — a per-predicate
       memoisation cache that mk-vt-pred / mk-vt-overlap-pred /
       mk-vt-during-pred / find-eav-winner allocate and write into
       under heavy read.

     * `java.util.Date` `.before` / `.after` instance methods used for
       point-in-window checks.

   Neither has a direct CLJS counterpart: CLJS is single-threaded so a
   plain `(atom {})` is functionally equivalent to a ConcurrentHashMap
   for the same access pattern; and `js/Date` has `.getTime` returning
   epoch-millis, so `<`/`>` on those numbers reproduces `.before`/`.after`.

   This namespace centralises the dispatch so the per-callsite code in
   `api/impl.cljc` stays small and the rules for *what* the JVM/CLJS
   impls do live in one place. The fns are inline-callable from `.cljc`
   files without per-callsite reader conditionals."
  #?(:clj
     (:import [java.util.concurrent ConcurrentHashMap]
              [java.util Date])))

;; ---------------------------------------------------------------------------
;; Predicate-scoped mutable map
;;
;; Used by mk-vt-pred / mk-vt-overlap-pred / mk-vt-during-pred /
;; find-eav-winner to memoise tx-id → Boolean and [e a v] → Datom lookups
;; for the lifetime of one filter predicate. The CHM is the right shape
;; on the JVM where multiple query threads may share the same predicate
;; closure (the FilteredDB is shared, the predicate is captured in
;; meta). On CLJS the runtime is single-threaded; an atom-wrapped map is
;; functionally equivalent and avoids pulling in a concurrent-map shim.

(defn mutable-map
  "Allocate a new mutable map for predicate-scoped caches.
   On JVM a `java.util.concurrent.ConcurrentHashMap`; on CLJS an
   `(atom {})`."
  []
  #?(:clj  (ConcurrentHashMap.)
     :cljs (atom {})))

(defn mput!
  "Associate `k` → `v` in the mutable map `m` returned by `mutable-map`.
   Returns `m`."
  [m k v]
  #?(:clj  (do (.put ^ConcurrentHashMap m k v) m)
     :cljs (do (swap! m assoc k v) m)))

(defn mget
  "Look `k` up in the mutable map `m`. Returns the previously-`mput!`'d
   value, or `nil` if absent."
  [m k]
  #?(:clj  (.get ^ConcurrentHashMap m k)
     :cljs (get @m k)))

;; ---------------------------------------------------------------------------
;; Date comparison
;;
;; All vt-window checks reduce to ordering of two instants. On JVM the
;; idiomatic form is `(.before d1 d2)` / `(.after d1 d2)`; on CLJS the
;; native `js/Date` has only `.getTime` so we compare epoch-millis as
;; numbers. The helpers keep type hints on the JVM branch so the call
;; stays inlined under typed reflection.

(defn date-before?
  "True iff `d1` strictly precedes `d2`. Both args are platform-native
   instants: `java.util.Date` on JVM, `js/Date` on CLJS."
  [d1 d2]
  #?(:clj  (.before ^Date d1 ^Date d2)
     :cljs (< (.getTime d1) (.getTime d2))))

(defn date-after?
  "True iff `d1` strictly follows `d2`. Both args are platform-native
   instants: `java.util.Date` on JVM, `js/Date` on CLJS."
  [d1 d2]
  #?(:clj  (.after ^Date d1 ^Date d2)
     :cljs (> (.getTime d1) (.getTime d2))))
