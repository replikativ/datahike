(ns datahike.gc-reference
  "Internal read-only edge expansion. Reference kinds describe immutable
   dependencies; they never publish roots, create leases, or authorize sweep."
  (:require [clojure.set :as set]))

(defmulti decode-reference
  "Decode a durable, versioned reference value to an edge descriptor."
  first)

(defmulti expand-reference
  "Expand one typed edge in a collector-owned context. Returns contributions
   under :reachable, :store-refs, :external-secondary-roots and :records.
   :records entries name required immutable records; absent required targets
   must abort traversal. Implementations must report the complete dependency set."
  (fn [reference _context] (:type reference)))

(defmethod decode-reference :default [value]
  (throw (ex-info "Unknown durable reference type or encoding."
                  {:type :datahike/gc-reference-unsupported :value value})))

(defmethod expand-reference :default [reference _]
  (throw (ex-info "No reachability marker is registered for this reference."
                  {:type :datahike/gc-reference-unsupported :reference reference})))

(defmethod decode-reference :datahike/snapshot [value]
  (when-not (and (vector? value) (= 4 (count value)) (= 1 (nth value 1))
                 (uuid? (nth value 2)) (uuid? (nth value 3)))
    (throw (ex-info "Malformed snapshot reference."
                    {:type :datahike/gc-reference-invalid :value value})))
  {:type :snapshot :store-id (nth value 2) :record-key (nth value 3)})

(defmethod expand-reference :snapshot [{:keys [store-id record-key] :as reference} context]
  (when-not (= store-id (:store-id context))
    (throw (ex-info "Snapshot references currently require the same store."
                    {:type :datahike/gc-reference-store-mismatch :reference reference
                     :expected-store-id (:store-id context)})))
  {:records [{:key record-key :parents? false :required? true}]})

(defmethod expand-reference :object [{:keys [key]} _]
  ;; Legacy store-ref ids may name external blobs. Keeping the key here does
  ;; not imply that a matching object must exist in the primary store.
  {:store-refs #{key}})

(defn valid-reference? [value]
  (try
    (let [reference (decode-reference value)]
      (and (map? reference)
           (not= (get-method expand-reference (:type reference))
                 (get-method expand-reference :default))))
    (catch #?(:clj Exception :cljs :default) _ false)))

(defn snapshot-reference
  "A versioned same-store snapshot descriptor. This constructs a VALUE only;
   it does not pin or validate the target's existence. Publication must validate
   the target under the deployment's publication/GC coordination contract."
  [store-id commit-id]
  (let [value [:datahike/snapshot 1 store-id commit-id]]
    (decode-reference value)
    value))

(defn merge-contributions
  "Union complete marker results while retaining further record edges."
  ([] {:reachable #{} :store-refs #{} :external-secondary-roots #{} :records []})
  ([a b]
   (when-not (and (map? b) (seq b)
                  (every? #{:reachable :store-refs :external-secondary-roots :records} (keys b))
                  (every? #(or (not (contains? b %)) (and (set? (get b %)) (every? some? (get b %))))
                          [:reachable :store-refs :external-secondary-roots])
                  (or (not (contains? b :records))
                      (and (vector? (:records b))
                           (every? #(and (map? %) (every? #{:key :parents? :required?} (keys %)) (some? (:key %))
                                         (false? (:parents? %)) (true? (:required? %)))
                                   (:records b)))))
     (throw (ex-info "Invalid reachability marker result; refusing to collect."
                     {:type :datahike/gc-reference-invalid-marker :result b})))
   (-> a
       (update :reachable set/union (or (:reachable b) #{}))
       (update :store-refs set/union (or (:store-refs b) #{}))
       (update :external-secondary-roots set/union (or (:external-secondary-roots b) #{}))
       (update :records into (:records b)))))
