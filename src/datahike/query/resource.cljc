(ns datahike.query.resource
  "Internal structural size guards for audited join/product/result builders.

   This namespace is an implementation seam, not a general query memory API.
   Scan buffers, grouping, sorting and recursive state are not all covered;
   binding this policy does not bound those structures or total query memory.

   A tuple retained in a collection costs its width plus one collection entry.
   Other builders explicitly charge their retained keys, bucket references and
   entries. These slots count references, not bytes or the recursive size of
   values. Separate accounts bound separate structures; their limits do not add
   up to a bound on the query's total live memory.

   Policy is inherited by nested queries, but mutable accounts belong only to
   one synchronous builder. Neither belongs in a cached plan. Reserve before
   allocation or insertion, and charge only new entries in deduplicating sinks."
  (:require [datahike.query.cancel :as cancel]))

(def max-slots
  "Largest structural count represented exactly on both supported platforms."
  9007199254740991)

(def ^:dynamic *max-intermediate-slots*
  "Internal inherited limit for audited builders, or nil for existing behavior.
   This is not a supported query option or a bound on every intermediate."
  nil)

(defn normalize-limit
  "Validate an optional positive integral limit. nil leaves policy unspecified."
  [n]
  (when (some? n)
    (when-not (and (integer? n) (pos? n) (<= n max-slots))
      (throw (ex-info "Intermediate slot limit must be a positive safe integer"
                      {:error :query/invalid-resource-limit
                       :resource :intermediate-slots})))
    n))

(defn effective-limit
  "A nested query may tighten the ambient limit, but cannot enlarge or remove it."
  [requested]
  (let [requested (normalize-limit requested)
        inherited (normalize-limit *max-intermediate-slots*)]
    (if (and requested inherited)
      (min requested inherited)
      (or requested inherited))))

(defn- valid-size! [n]
  (when-not (and (integer? n) (<= 0 n max-slots))
    (throw (ex-info "Intermediate size must be a nonnegative safe integer"
                    {:error :query/invalid-resource-size
                     :resource :intermediate-slots})))
  n)

(defn limit-exceeded?
  "Recognize the typed resource failure through standard exception causes.
   Messages do not identify failures. Cyclic cause chains terminate safely."
  [error]
  (loop [cause error, seen #{}]
    (cond
      (nil? cause) false
      (= :query/intermediate-limit (:error (ex-data cause))) true
      (contains? seen cause) false
      :else (recur (ex-cause cause) (conj seen cause)))))

(defn- exceeded! [operator limit retained requested]
  ;; A simultaneous cancellation keeps its existing classification. No user
  ;; values or tuples are included in the resource exception.
  (cancel/check!)
  (throw (ex-info "Query intermediate exceeds its structural size limit"
                  {:error :query/intermediate-limit
                   :resource :intermediate-slots
                   :operator operator
                   :limit limit
                   :retained retained
                   :requested requested})))

(defn checked-add
  "Add structural counts without overflowing the portable exact-integer range."
  [a b]
  (valid-size! a)
  (valid-size! b)
  (when (> b (- max-slots a))
    (exceeded! :size-arithmetic (or *max-intermediate-slots* max-slots) a b))
  (+ a b))

(defn checked-multiply
  "Multiply structural counts, rejecting overflow before multiplication."
  [a b]
  (valid-size! a)
  (valid-size! b)
  ;; Division is only a conservative preflight on JS doubles. Verify the exact
  ;; range again after multiplication so boundary rounding cannot admit 2^53.
  (when (and (pos? a) (> b (/ max-slots a)))
    (exceeded! :size-arithmetic (or *max-intermediate-slots* max-slots) 0 max-slots))
  (let [n (* a b)]
    (when (> n max-slots)
      (exceeded! :size-arithmetic (or *max-intermediate-slots* max-slots) 0 max-slots))
    n))

(defn tuple-slots
  "One retained tuple: its actual width plus one collection-entry reference."
  [width]
  (checked-add width 1))

(defn rows-slots
  "Known materialization capacity, checked before allocating rows or tuples."
  [rows width]
  (checked-multiply rows (tuple-slots width)))

(defn account
  "Fresh account for one synchronous builder, or nil when no limit is active."
  [operator]
  (when-some [limit *max-intermediate-slots*]
    {:limit limit :operator operator :retained (volatile! 0)}))

(defn reserve!
  "Reserve slots before growth. Rejection leaves the account unchanged.
   A nil account is a cheap no-op; accounts must not be shared between threads."
  [account slots]
  (when account
    (valid-size! slots)
    (let [used @(:retained account)
          limit (:limit account)]
      ;; Subtraction avoids constructing an overflowing prospective sum.
      (when (> slots (- limit used))
        (exceeded! (:operator account) limit used slots))
      (vreset! (:retained account) (+ used slots))))
  nil)

(defn check-size!
  "Check a known capacity before allocation without creating or changing an account."
  [operator slots]
  (when-some [limit *max-intermediate-slots*]
    (valid-size! slots)
    (when (> slots limit)
      (exceeded! operator limit 0 slots)))
  nil)
