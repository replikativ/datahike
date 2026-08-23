(ns datahike.value-type
  "Process-local registry for scalar value types supplied by extensions.

  A descriptor has this shape:

    {:id      :vendor.type/name
     :type    RuntimeType
     :valid?  predicate
     :compare (fn [left right] signed-number)
     :wire    {:name    \"vendor/name\"
               :version 1
               :encode  (fn [value] portable-payload)
               :decode  (fn [version portable-payload] value)}}

  `:type` is matched exactly (not with inheritance). `:compare` must define a
  total order and return zero exactly when the values are equal. Wire adapters
  emit the descriptor's version and pass the encoded version to `:decode`, so
  compatibility policy belongs to the extension that owns the type.

  Registration is process-local: functions and runtime types cannot be
  persisted in database config. An extension must register before a database
  whose schema uses its value type is created or connected."
  (:require [clojure.string :as str]
            #?(:cljs [fress.impl.bigdec :as fbd]))
  #?(:clj (:import [clojure.lang IPersistentVector Keyword Symbol]
                   [java.util Date UUID])))

(defonce ^:private registry
  (atom {:by-id {} :by-type {} :by-wire {}}))

(defn- invalid! [message descriptor]
  (throw (ex-info message
                  {:error :value-type/invalid-descriptor
                   :descriptor descriptor})))

(defn- runtime-type? [candidate]
  #?(:clj (instance? Class candidate)
     ;; ClojureScript's `type` returns the constructor used as the exact lookup
     ;; key. There is no portable Class analogue to validate more narrowly.
     :cljs (some? candidate)))

(defn- builtin-runtime-type?
  "True when TYPE is already admitted by a built-in scalar schema. Custom
  comparator dispatch is global, so allowing one of these types would silently
  change ordering for ordinary attributes in every live database."
  [type]
  #?(:clj
     (or (.isAssignableFrom Number type)
         (.isAssignableFrom IPersistentVector type)
         (contains? #{String Boolean Date Keyword Symbol UUID
                      (Class/forName "[B") (Class/forName "[F") (Class/forName "[D")}
                    type))
     :cljs
     (contains? #{(cljs.core/type "")
                  (cljs.core/type true)
                  (cljs.core/type 0)
                  (cljs.core/type (js/BigInt 0))
                  (cljs.core/type (js/Date.))
                  (cljs.core/type :datahike/builtin)
                  (cljs.core/type 'datahike/builtin)
                  (cljs.core/type (uuid "00000000-0000-0000-0000-000000000000"))
                  (cljs.core/type [])
                  (cljs.core/type (js/Uint8Array. 0))
                  (cljs.core/type (js/Int8Array. 0))
                  (cljs.core/type (js/Float32Array. 0))
                  (cljs.core/type (js/Float64Array. 0))
                  (cljs.core/type (fbd/bigdec (js/BigInt 0) 0))}
                type)))

(defn- validate-descriptor! [{:keys [id type valid? compare wire] :as descriptor}]
  (when-not (qualified-keyword? id)
    (invalid! "Custom value type :id must be a qualified keyword" descriptor))
  (when (= "db.type" (namespace id))
    (invalid! "Custom value type :id must not use Datahike's reserved db.type namespace"
              descriptor))
  (when-not (runtime-type? type)
    (invalid! "Custom value type :type must be an exact runtime type" descriptor))
  (when (builtin-runtime-type? type)
    (invalid! "Custom value type :type must not be a built-in Datahike runtime type; use a dedicated wrapper type"
              descriptor))
  (when-not (ifn? valid?)
    (invalid! "Custom value type :valid? must be callable" descriptor))
  (when-not (ifn? compare)
    (invalid! "Custom value type :compare must be callable" descriptor))
  (when-not (map? wire)
    (invalid! "Custom value type :wire must be a map" descriptor))
  (when-not (and (string? (:name wire)) (not (str/blank? (:name wire))))
    (invalid! "Custom value type :wire :name must be a non-blank string" descriptor))
  (when-not (pos-int? (:version wire))
    (invalid! "Custom value type :wire :version must be a positive integer" descriptor))
  (when-not (ifn? (:encode wire))
    (invalid! "Custom value type :wire :encode must be callable (value -> payload)"
              descriptor))
  (when-not (ifn? (:decode wire))
    (invalid! "Custom value type :wire :decode must be callable (version payload -> value)"
              descriptor))
  descriptor)

(defn- conflict! [field value existing descriptor]
  (throw (ex-info (str "Custom value type " (name field) " already registered: "
                       (pr-str value))
                  {:error :value-type/registration-conflict
                   :field field
                   :value value
                   :existing existing
                   :descriptor descriptor})))

(defn register!
  "Registers and returns DESCRIPTOR.

  Re-registering an equal descriptor is idempotent. A reused id, exact runtime
  type, or wire name with a different descriptor is rejected."
  [descriptor]
  (validate-descriptor! descriptor)
  (let [{:keys [id type wire]} descriptor
        wire-name (:name wire)]
    (swap! registry
           (fn [{:keys [by-id by-type by-wire] :as state}]
             (doseq [[field value existing]
                     [[:id id (get by-id id)]
                      [:type type (get by-type type)]
                      [:wire-name wire-name (get by-wire wire-name)]]]
               (when (and existing (not= existing descriptor))
                 (conflict! field value existing descriptor)))
             (-> state
                 (assoc-in [:by-id id] descriptor)
                 (assoc-in [:by-type type] descriptor)
                 (assoc-in [:by-wire wire-name] descriptor))))
    descriptor))

(defn descriptor
  "Returns the descriptor registered for value-type ID, or nil."
  [id]
  (get-in @registry [:by-id id]))

(defn descriptor-for-value
  "Returns the descriptor for VALUE's exact runtime type, or nil."
  [value]
  (get-in @registry [:by-type (type value)]))

(defn descriptor-for-wire
  "Returns the descriptor registered for stable wire NAME, or nil."
  [name]
  (get-in @registry [:by-wire name]))

(defn descriptors
  "Returns an immutable id -> descriptor snapshot of the current registry."
  []
  (:by-id @registry))

(defn valid-value?
  "True when VALUE has DESCRIPTOR's exact runtime type and satisfies its
  predicate."
  [{registered-type :type valid? :valid?} value]
  (and (identical? registered-type (type value))
       (boolean (valid? value))))

(defn decode-value
  "Decode a versioned portable payload and reject a decoder that violates its
  own descriptor. This keeps malformed or stale extension code from admitting
  a value the schema and index cannot safely handle."
  [{:keys [id wire] :as descriptor} version payload]
  (let [value ((:decode wire) version payload)]
    (when-not (valid-value? descriptor value)
      (throw (ex-info (str "Custom value type decoder returned an invalid value for "
                           (pr-str id))
                      {:error :value-type/invalid-decoded-value
                       :value-type id
                       :version version
                       :value value})))
    value))

(defn reset-registry!
  "Clears all registrations. Intended only for isolated tests and REPL
  development; never call it while a database connection or store is live."
  []
  (reset! registry {:by-id {} :by-type {} :by-wire {}})
  nil)
