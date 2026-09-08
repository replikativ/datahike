(ns datahike.dependency-tracking
  "Runtime-only dependency tokens for cached projections of database values.
   Enroll through the :track-dependencies transaction option. Tokens certify
   absence of selected mutations along a tracked lineage, not durable commits.
   They are intentionally lost on reload and wholesale database replacement."
  (:require [clojure.string :as str]))

(def ^:private state-key ::state)
(def ^:private max-groups 16)
(def ^:private max-terms 256)
(def ^:private max-term-length 512)

(defprotocol ITrackedSnapshot
  (-dependency-entry [db id]))

(defn state-entry
  "Internal plain-DB lookup; wrappers must not inherit this implementation."
  [db id]
  (get-in db [state-key id]))

(defn- fresh-token [] #?(:clj (Object.) :cljs (js-obj)))

(defn- invalid! [message]
  (throw (ex-info message {:error :transact/invalid-options :option :track-dependencies})))

(defn- selector! [selector]
  (when-not (and (map? selector)
                 (every? #{:attributes :namespaces :namespace-prefixes :exclude-attributes} (keys selector)))
    (invalid! "Dependency selector must contain only attributes, namespaces, namespace-prefixes, and exclude-attributes"))
  (let [selector (merge {:attributes #{} :namespaces #{} :namespace-prefixes #{}
                         :exclude-attributes #{}} selector)]
    (doseq [[kind terms] selector]
      (when-not (and (set? terms) (<= (count terms) max-terms))
        (invalid! "Dependency selector terms must be bounded sets"))
      (doseq [term terms]
        (when-not (and (if (#{:attributes :exclude-attributes} kind) (keyword? term) (string? term))
                       (<= 1 (count (str term)) max-term-length))
          (invalid! "Dependency selector has an invalid or oversized term"))))
    (when (> (reduce + (map count (vals selector))) max-terms)
      (invalid! "Dependency selector exceeds term limit"))
    selector))

(defn validate-options!
  "Validate enrollment options without changing any DB value."
  [groups]
  (when-not (and (map? groups) (<= (count groups) max-groups))
    (invalid! "Dependency enrollment must be a map of at most 16 groups"))
  (doseq [[id selector] groups]
    (when-not (and (keyword? id) (<= (count (str id)) max-term-length))
      (invalid! "Dependency group ids must be bounded keywords"))
    (when (some? selector) (selector! selector)))
  groups)

(defn enroll
  "Internal transaction seam. Merge named selectors into the DB's runtime
   state. Nil removes a group; absent options preserve existing groups.
   A new or changed selector always gets a fresh token."
  [db groups]
  (if (nil? groups)
    db
    (do
      (validate-options! groups)
      (let [existing (get db state-key {})
            next (reduce-kv
                  (fn [state id selector]
                    (if (nil? selector)
                      (dissoc state id)
                      (let [selector (selector! selector)]
                        (if (= selector (get-in state [id :selector]))
                          state
                          (assoc state id {:selector selector :token (fresh-token)})))))
                  existing groups)]
        (when (> (count next) max-groups)
          (invalid! "Combined dependency enrollment exceeds 16 groups"))
        (if (identical? existing next) db
            (if (seq next) (assoc db state-key next) (dissoc db state-key)))))))

(defn token
  "Return an opaque runtime token, or nil when this DB/view is untracked.
   Capture it with the projection derived from the SAME DB value. Tokens are
   not serialized, are not commit ids, and cannot prove arbitrary value equality.
   Pass expected-selector when other code can replace this named group's policy;
   the checked arity returns nil unless the canonical selector matches exactly."
  ([db id]
   (when (satisfies? ITrackedSnapshot db)
     (:token (-dependency-entry db id))))
  ([db id expected-selector]
   (let [selector (selector! expected-selector)]
     (when (satisfies? ITrackedSnapshot db)
       (let [entry (-dependency-entry db id)]
         (when (= selector (:selector entry)) (:token entry)))))))

(defn valid?
  "True only for a present, identical token. Two untracked values never prove
   validity. Snapshot-local enrollment and all ordinary mutations are tracked;
   unsupported reconstructed/temporal views have no certificate."
  ([captured db id]
   (and (some? captured) (identical? captured (token db id))))
  ([captured db id expected-selector]
   (and (some? captured) (identical? captured (token db id expected-selector)))))

(defn invalidate-all
  "Internal: refresh every enrolled token without changing selectors."
  [db]
  (if-let [state (get db state-key)]
    (assoc db state-key
           (reduce-kv (fn [m id entry] (assoc m id (assoc entry :token (fresh-token)))) {} state))
    db))

(defn forget
  "Internal: wholesale replacements must not inherit cache certificates."
  [db]
  (dissoc db state-key))

(defn- matches? [{:keys [attributes namespaces namespace-prefixes exclude-attributes]} attr]
  (let [n (namespace attr)]
    (and (not (contains? exclude-attributes attr))
         (or (contains? attributes attr)
             (contains? namespaces n)
             (and n (some #(str/starts-with? n %) namespace-prefixes))))))

(defn changed
  "Internal per-mutation seam. Schema/ident changes conservatively invalidate
   every group. No-match and untracked writes return the original DB value."
  [db attr schema?]
  (if-let [state (get db state-key)]
    (if (or schema? (not (keyword? attr)))
      (invalidate-all db)
      (let [next (reduce-kv
                  (fn [m id {:keys [selector] :as entry}]
                    (if (matches? selector attr)
                      (assoc m id (assoc entry :token (fresh-token))) m))
                  state state)]
        (if (identical? state next) db (assoc db state-key next))))
    db))

(defn schema-transition
  "Internal fallback for schema/ident changes outside ordinary datom paths."
  [before after]
  (if (and (get after state-key)
           (or (not= (:schema before) (:schema after))
               (not= (:ident-ref-map before) (:ident-ref-map after))
               (not= (:ref-ident-map before) (:ref-ident-map after))))
    (invalidate-all after)
    after))
