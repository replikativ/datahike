(ns ^:no-doc datahike.backfill.effects
  "Pure primary-index effects for a private AVET generation. Capture does not
   enable schema, publish cursors, or perform I/O. Writer admission owns those
   boundaries and must invalidate generations on conflicting schema changes."
  (:require [datahike.index.interface :as di]
            [datahike.datom :as dd]))

(def ^:private observer-defaults
  {:max-effects 65536 :max-bytes 16777216 :max-nodes 262144 :max-depth 64})

(defn- observer-error! [type message]
  (throw (ex-info message {:type type})))

(defn observer-options!
  "Validate private observation limits. Bytes are conservative structural
   accounting units, not a measurement of retained heap."
  [options]
  (when-not (and (or (nil? options) (map? options))
                 (every? (set (keys observer-defaults)) (keys options)))
    (observer-error! ::invalid-observer "Unknown observer options."))
  (let [options (merge observer-defaults options)]
    (doseq [[key value] options]
      (when-not (and (integer? value) (pos? value)
                     (<= value (if (= key :max-depth) 64 2147483647)))
        (observer-error! ::invalid-observer "Observer limits must be positive bounded integers.")))
    options))

(defn- charge-effect [observer effect]
  (let [{:keys [max-effects max-bytes max-nodes max-depth]} (:limits observer)
        used (volatile! (select-keys observer [:bytes :nodes]))]
    (when (>= (:count observer) max-effects)
      (observer-error! ::observer-budget "Observer effect limit exceeded."))
    (letfn [(charge! [n]
              (when (> (+ (:bytes @used) n) max-bytes)
                (observer-error! ::observer-budget "Observer structural byte limit exceeded."))
              (vswap! used update :bytes + n))
            (visit! [x depth]
              (when (or (> depth max-depth) (>= (:nodes @used) max-nodes))
                (observer-error! ::observer-budget "Observer nesting or node limit exceeded."))
              (vswap! used update :nodes inc)
              (charge! 64)
              (when-let [metadata (meta x)] (visit! metadata (inc depth)))
              (cond
                (nil? x) nil
                (string? x) (charge! (* 2 (count x)))
                (or (keyword? x) (symbol? x))
                (do (visit! (name x) (inc depth)) (visit! (namespace x) (inc depth)))
                (dd/datom? x)
                (doseq [v [(:e x) (:a x) (:v x) (dd/datom-tx x) (dd/datom-added x)]]
                  (visit! v (inc depth)))
                #?@(:clj
                    [(instance? java.math.BigInteger x)
                     (charge! (+ 32 (quot (+ 7 (.bitLength ^java.math.BigInteger x)) 8)))
                     (instance? clojure.lang.BigInt x)
                     (visit! (.toBigInteger ^clojure.lang.BigInt x) (inc depth))
                     (instance? java.math.BigDecimal x)
                     (visit! (.unscaledValue ^java.math.BigDecimal x) (inc depth))
                     (ratio? x)
                     (do (visit! (numerator x) (inc depth)) (visit! (denominator x) (inc depth)))
                     (.isArray (class x))
                     (let [n (java.lang.reflect.Array/getLength x)]
                       (charge! (* 8 n))
                       (when-not (.isPrimitive (.getComponentType (class x)))
                         (dotimes [i n] (visit! (java.lang.reflect.Array/get x i) (inc depth)))))
                     (instance? java.net.URI x) (visit! (str x) (inc depth))]
                    :cljs
                    [(array? x) (do (charge! (* 8 (alength x)))
                                    (doseq [v x] (visit! v (inc depth))))
                     (js/ArrayBuffer.isView x) (charge! (.-byteLength x))])
                (or #?(:clj (or (instance? Long x) (instance? Integer x)
                                (instance? Short x) (instance? Byte x)
                                (instance? Double x) (instance? Float x))
                       :cljs (number? x))
                    (boolean? x) (uuid? x)
                    #?(:clj (or (char? x) (instance? java.util.Date x))
                       :cljs (inst? x))) nil
                (map? x) (doseq [[k v] x] (visit! k (inc depth)) (visit! v (inc depth)))
                (or (sequential? x) (set? x)) (doseq [v x] (visit! v (inc depth)))
                :else (observer-error! ::invalid-observer "Unsupported observed value.")))]
      (visit! effect 0)
      (merge observer @used {:count (inc (:count observer))}))))

(defn- observe-effect [db effect]
  ;; Charge before retaining the effect. Failure leaves the input DB and its
  ;; immutable budget untouched, including when a transaction retries.
  (let [observer (charge-effect (::observer db) effect)]
    (-> db (assoc ::observer observer) (update ::observations (fnil conj []) effect))))

(defn schema-change
  "Mark an active generation unusable after a schema mutation. Sticky even if
   later operations restore the original schema: intermediate ident changes
   can alter which writes the generation captures. Does not reject the tx."
  [db]
  (if (:avet-build db) (assoc db :avet-build-invalidated? true) db))

(defn enabled?
  "Whether exact primary effects are needed by a build or private admission."
  [db]
  (or (:avet-build db) (::observer db)))

(defn observe
  "Attach a pure private mutation observer to an unpublished activation DB.
   Does not authorize schema changes or uniqueness deferral. The separately
   validated admission context owns those decisions. No I/O or mutable sink."
  ([db attrs] (observe db attrs nil))
  ([db attrs options]
   (when-not (and (set? attrs) (every? keyword? attrs))
     (throw (ex-info "Observer attributes must be canonical idents." {:type ::invalid-observer})))
   (when (::observer db)
     (throw (ex-info "An admission observer is already attached." {:type ::invalid-observer})))
   (-> db (assoc ::observer {:attrs attrs :limits (observer-options! options)
                             :count 0 :bytes 0 :nodes 0})
       (dissoc ::observations))))

(defn observations [db] (get db ::observations []))

(defn unobserve [db] (dissoc db ::observer ::observations))

(defn emit
  "Append one native index operation when the active generation covers ident.
   :attrs contains resolved attribute idents, including existing indexed attrs
   when the generation replaces the complete AVET family. The supplied datoms
   are primary values (possibly hashes), not adapter notifications. Historical
   removals cover every attribute: a full replacement AVET retains disabled
   historical slices too, but must not retain their purged datoms."
  [db ident family operation datom old-datom]
  (let [effect [family operation datom old-datom]]
    (cond-> db
      (and (:avet-build db)
           (or (contains? (get-in db [:avet-build :attrs]) ident)
               (and (= family :temporal) (= operation :remove))))
      (update :avet-build-effects (fnil conj []) effect)
      (contains? (get-in db [::observer :attrs]) ident)
      (observe-effect effect))))

(defn replay
  "Apply exact operations to {:current pss :temporal pss}. Only PSS is supported:
   its operations ignore op-count. The caller owns private storage and flushing."
  [trees [family operation datom old-datom]]
  (when-not (contains? (case family
                         :current #{:insert :remove :upsert}
                         :temporal #{:temporal-insert :remove :temporal-upsert}
                         #{}) operation)
    (throw (ex-info "Invalid AVET effect family or operation."
                    {:type ::invalid-effect :family family :operation operation})))
  (update trees family
          (fn [tree]
            (when-not tree
              (throw (ex-info "AVET effect has no target tree." {:type ::invalid-effect :family family})))
            (case operation
              :insert (di/-insert tree datom :avet 0)
              :remove (di/-remove tree datom :avet 0)
              :upsert (di/-upsert tree datom :avet 0 old-datom)
              :temporal-insert (di/-temporal-insert tree datom :avet 0)
              :temporal-upsert (di/-temporal-upsert tree datom :avet 0 old-datom)
              (throw (ex-info "Unknown AVET effect operation."
                              {:type ::invalid-effect :operation operation}))))))
