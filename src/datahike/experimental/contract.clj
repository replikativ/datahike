(ns datahike.experimental.contract
  "A bounded JVM Datalog profile. Clause order and cursor order are part of v1."
  (:require [datalog.parser :as parser]
            [datahike.db]
            [datahike.db.interface :as dbi])
  (:import [datahike.db DB]))

(def version :datahike/contract-v1)
(def version-2 :datahike/contract-v2)
(def transition-profile :datahike/contract-transition)
(def ^:private transition-sources '#{$before $after})
(def ^:private predicates {'= = 'not= not= '< < '<= <= '> > '>= >=})
(def ^:private events #{:input :predicate :cursor :advance :project :aggregate :branch})

(defn- reject [form reason]
  (throw (ex-info reason {:error :contract/unsupported :form form})))

(defn- stop! [reason] (throw (ex-info "Contract execution stopped" {::stop reason})))

(defn- spend! [meter event]
  (let [{:keys [used limit]} @meter]
    (when (= used limit)
      (stop! :out-of-fuel))
    (vswap! meter #(-> % (update :used inc) (update-in [:events event] inc)))))

(defn- run [profile limit evaluate]
  (let [meter (volatile! {:used 0 :limit limit :events (zipmap (if (#{version-2 transition-profile} profile) (conj events :function :union) events) (repeat 0))})]
    (try
      (let [result (evaluate meter)]
        (assoc @meter :status :ok :rows result))
      (catch clojure.lang.ExceptionInfo e
        (if-let [reason (::stop (ex-data e))]
          (assoc @meter :status reason)
          (throw e))))))

(defn- scalar? [x]
  (or (int? x)
      (and (or (keyword? x) (string? x)) (<= (count (str x)) 128))))

(defn- variable? [x]
  (and (simple-symbol? x) (.startsWith (name x) "?")))

(defn- known? [vars t]
  (or (scalar? t) (contains? vars t)))

(defn- term? [x] (or (= x '_) (variable? x) (scalar? x)))

(defn check-form!
  "Check size, depth and scalar limits before hashing or parsing a submitted query."
  [form]
  (let [nodes (volatile! 0)]
    (letfn [(visit [x depth]
              (when (or (> (vswap! nodes inc) 128) (> depth 12))
                (reject nil "Contract syntax limit exceeded"))
              (cond
                (or (list? x) (vector? x)) (doseq [v x] (visit v (inc depth)))
                (or (scalar? x) (and (simple-symbol? x) (<= (count (name x)) 64))) nil
                :else (reject x "Unsupported contract literal")))]
      (visit form 0))))

(declare compile-clauses)

(defn- index-pattern [profile clause]
  (when (vector? clause)
    (if (= profile transition-profile)
      (when (and (= 4 (count clause)) (transition-sources (first clause))
                 (every? term? (subvec clause 1)))
        clause)
      (when (and (= 3 (count clause)) (every? term? clause))
        (into ['$] clause)))))

(defn- compile-clause [profile attributes vars clause]
  (cond
    (and (vector? clause) (= 1 (count clause)) (list? (first clause)))
    (let [[op x y :as call] (first clause)]
      (when-not (and (= 3 (count call)) (predicates op) (every? #(known? vars %) [x y]))
        (reject clause "Unsupported predicate or unbound argument"))
      [vars [:predicate op x y]])

    (and (#{version-2 transition-profile} profile) (vector? clause) (= 2 (count clause)) (list? (first clause)))
    (let [[[op arg :as call] out] clause]
      (when-not (and (#{'inc 'dec} op) (= 2 (count call)) (known? vars arg)
                     (variable? out) (not (vars out)))
        (reject clause "Expected a checked unary integer function and a fresh output"))
      [(conj vars out) [:function op arg out]])

    (and (list? clause) (#{'or-join 'not-join} (first clause)))
    (let [[kind args & body] clause
          outputs? (and (#{version-2 transition-profile} profile) (= kind 'or-join) (vector? args) (some #(not (vars %)) args))]
      (when-not (and (vector? args) (seq args) (seq body)
                     (if outputs? (and (every? variable? args) (apply distinct? args)) (every? vars args)))
        (reject clause "Expected a supported correlation vector"))
      (let [scope (set (filter vars args))
            branches (if (= kind 'not-join) [body]
                         (map #(if (and (list? %) (= 'and (first %))) (rest %) [%]) body))
            plans (mapv #(compile-clauses profile attributes scope %) branches)]
        (when (and outputs? (some (fn [[bound]] (not-every? bound args)) plans))
          (reject clause "Every branch must bind every declared output"))
        (if outputs?
          [(into vars args) [:union scope args (mapv second plans)]]
          [vars [:exists (= kind 'not-join) args (mapv second plans)]])))

    :else (reject clause "Clause has no metered operator")))

(defn- compile-clauses [profile attrs vars clauses]
  (loop [vars vars ops [] [clause & more] clauses]
    (if-not clause
      [vars ops]
      ;; A bounded interval is written as an indexed pattern followed by its
      ;; inclusive lower and upper predicates. Both endpoints must be bound.
      (let [[lo hi] more
            [source e a v] (index-pattern profile clause)]
        (cond
          (and source (attrs a) (term? e) (variable? v)
               (not (vars v)) (not (known? vars e))
               (every? #(and (vector? %) (= 1 (count %)) (list? (first %))
                             (= 3 (count (first %))) (= v (second (first %)))
                             (known? vars (last (first %)))) [lo hi])
               (= '>= (ffirst lo)) (= '<= (ffirst hi)))
          (recur (into vars (filter variable? [e v]))
                 (conj ops [:range e a v (last (first lo)) (last (first hi)) source]) (drop 2 more))

          source
          (let [index (cond (known? vars e) :eavt (known? vars v) :avet)]
            (when-not (and (attrs a) index (or (variable? e) (= '_ e) (pos-int? e)))
              (reject clause "Expected a declared attribute and a bound index prefix"))
            (recur (into vars (filter variable? [e v]))
                   (conj ops [:lookup index e a v source]) more))

          :else
          (let [[bound op] (compile-clause profile attrs vars clause)]
            (recur bound (conj ops op) more)))))))

(defn- lower
  "Validate every clause and projection. attrs is a trusted set of readable
  attribute keywords. width is the arity of the signed delta relation."
  [profile form attrs width]
  (check-form! form)
  (when-not (and (vector? form) (= :find (first form)) (set? attrs)
                 (every? qualified-keyword? attrs) (<= 1 width 8))
    (reject form "Expected a relation query and declared inputs"))
  (let [[finds tail] (split-with #(not (#{:in :with} %)) (rest form))
        [with tail] (if (= :with (first tail))
                      (split-with #(not= :in %) (rest tail)) [[] tail])
        header (if (= profile transition-profile) '(:in $before $after $delta :where) '(:in $ $delta :where))
        [input & clauses] (drop (count header) tail)
        terms (when (and (vector? input) (seq input)) (subvec input 1))]
    (when-not (and (= header (take (count header) tail))
                   (vector? input) (= '$delta (first input)) (= width (count terms))
                   (every? term? terms) (seq finds))
      (reject form "Expected profile source declaration and a leading delta pattern"))
    (parser/parse form)
    (let [[vars ops] (compile-clauses profile attrs (set (filter variable? terms)) clauses)
          projection (mapv (fn [x]
                             (cond
                               (vars x) [:value x]
                               (and (list? x) (= 2 (count x)) (#{'sum 'count} (first x))
                                    (vars (second x))) [(keyword (name (first x))) (second x)]
                               :else (reject x "Unsupported projection"))) finds)]
      (when-not (every? vars with) (reject with "Unbound :with variable"))
      {:terms terms :ops ops :projection projection :with (vec with)})))

(defn- value [env x] (if (variable? x) (get env x) x))

(defn- match-term [env term x]
  (cond
    (= '_ term) env
    (and (variable? term) (not (contains? env term))) (assoc env term (if (int? x) (long x) x))
    (= x (value env term)) env))

(defn- scan [charge! cursor accept]
  (charge! :cursor)
  (loop [advance cursor]
    (charge! :advance)
    (when-let [s (seq (advance))]
      (or (accept (first s)) (recur #(rest s))))))

(defn- matches-index-type? [schema attribute x]
  (case (get-in schema [attribute :db/valueType])
    :db.type/long (int? x)
    :db.type/string (string? x)
    :db.type/keyword (keyword? x)
    false))

(defn- walk [sources charge! ops env emit]
  (if-let [[operator & operands] (first ops)]
    (let [proceed (fn [row] (walk sources charge! (next ops) row emit))
          accept (fn [datom entity-term value-term]
                   (let [x (nth datom 2)]
                     (when-not (scalar? x) (stop! :value-limit))
                     (when-let [row (match-term env entity-term (nth datom 0))]
                       (when-let [row (match-term row value-term x)] (proceed row)))))]
      (case operator
        :predicate
        (let [[op left right] operands x (value env left) y (value env right)]
          (charge! :predicate)
          (when-not (or (#{'= 'not=} op) (and (int? x) (int? y))) (stop! :type-error))
          (when ((predicates op) x y) (proceed env)))

        :function
        (let [[op arg out] operands
              x (value env arg)]
          (charge! :function)
          (when-not (int? x) (stop! :type-error))
          (let [result (try (if (= op 'inc) (Math/incrementExact (long x)) (Math/decrementExact (long x)))
                            (catch ArithmeticException _ (stop! :numeric-overflow)))]
            (proceed (assoc env out result))))

        :union
        (let [[inputs args branches] operands
              scope (select-keys env inputs)
              seen (volatile! #{})
              forward (fn [row]
                        (charge! :union)
                        (let [entry (mapv row args)]
                          (when-not (contains? @seen entry)
                            (when (>= (count @seen) 128) (stop! :relation-limit))
                            (vswap! seen conj entry)
                            (proceed (into env (map vector args entry))))))]
          (some (fn [branch]
                  (charge! :branch)
                  (walk sources charge! branch scope forward))
                branches))

        :exists
        (let [[negated? vars branches] operands
              scope (select-keys env vars)
              found (some (fn [branch]
                            (charge! :branch)
                            (walk sources charge! branch scope (constantly true))) branches)]
          (when (= (not negated?) (boolean found)) (proceed env)))

        :lookup
        (let [[index entity attr val-term source] operands
              db (get sources source)
              avet? (= index :avet)
              key (value env (if avet? val-term entity))]
          (when-not (if avet? (matches-index-type? (:schema db) attr key) (int? key))
            (stop! :type-error))
          (scan charge! #(when (or avet? (pos-int? key))
                           (dbi/datoms db index (if avet? [attr key] [key attr])))
                #(accept % entity val-term)))

        :range
        (let [[entity attr val-term lower upper source] operands
              db (get sources source)
              lo (value env lower) hi (value env upper)]
          (when-not (and (int? lo) (int? hi)) (stop! :type-error))
          (scan charge! #(when (<= lo hi) (dbi/index-range db attr lo hi))
                #(accept % entity val-term)))))
    (emit env)))

(defn- validate-indexes! [schemas ops]
  (doseq [[op a b c :as step] ops]
    (case op
      (:exists :union) (doseq [branch c] (validate-indexes! schemas branch))
      (:lookup :range)
      (let [attr (if (= op :lookup) c b)
            spec (get-in schemas [(peek step) attr])]
        (when-not (and (#{:db.type/keyword :db.type/string :db.type/long} (:db/valueType spec))
                       (or (and (= op :lookup) (= a :eavt)) (:db/unique spec) (:db/index spec))
                       (or (not= op :range) (= :db.type/long (:db/valueType spec))))
          (reject step "Unsupported index or attribute schema")))
      nil)))

(defn admit
  "Lower a bounded program using its index schema. For transition-profile
  supply a map from the symbols $before and $after to their respective schemas."
  ([form attrs width schema] (admit version form attrs width schema))
  ([profile form attrs width schema]
   (when-not (#{version version-2 transition-profile} profile) (reject profile "Unknown contract profile"))
   (when (and (= profile transition-profile)
              (not (and (map? schema) (= transition-sources (set (keys schema)))
                        (every? map? (vals schema)))))
     (reject nil "Expected schemas for both transition snapshots"))
   (let [plan (lower profile form attrs width)]
     (validate-indexes! (if (= profile transition-profile) schema {'$ schema}) (:ops plan))
     plan)))

(defn- accumulate [projection env totals]
  (mapv (fn [[op variable] acc]
          (let [x (env variable)]
            (case op
              :value x
              :count (inc (or acc 0))
              :sum (do (when-not (int? x) (stop! :type-error))
                       (try (Math/addExact (long (or acc 0)) (long x))
                            (catch ArithmeticException _ (stop! :numeric-overflow)))))))
        projection (or totals (repeat nil))))

(defn- evaluate [{:keys [terms ops projection with]} sources rows charge!]
  (let [fields (mapv second projection)
        aggregated? (some #(not= :value (first %)) projection)
        basis (volatile! #{})
        groups (volatile! {})
        emit (fn [env]
               (charge! :project)
               (let [entry (into (mapv env fields) (map env with))]
                 (when-not (@basis entry)
                   (when (>= (count @basis) 128) (stop! :relation-limit))
                   (vswap! basis conj entry)
                   (when aggregated?
                     (charge! :aggregate)
                     (let [key (mapv (fn [[op v]] (when (= op :value) (env v))) projection)]
                       (vswap! groups update key #(accumulate projection env %))))))
               nil)]
    (doseq [row rows]
      (charge! :input)
      (when-let [env (reduce (fn [env [term x]] (or (match-term env term x) (reduced nil)))
                             {} (map vector terms row))]
        (walk sources charge! ops env emit)))
    (if aggregated? (set (vals @groups))
        (into #{} (map #(subvec % 0 (count fields))) @basis))))

(defn execute
  "Compile the signed form and run it with bounded input and projection sets.
  :rows is returned only on success. Fuel excludes admission and schema checks."
  ([form attrs width db rows fuel] (execute version form attrs width db rows fuel))
  ([profile form attrs width db rows fuel]
   (let [transition? (= profile transition-profile)
         sources (if transition? db {'$ db})]
     (when-not (and (map? sources) (= (if transition? transition-sources '#{$}) (set (keys sources)))
                    (every? #(instance? DB %) (vals sources)) (vector? rows) (<= (count rows) 32)
                    (every? #(and (vector? %) (= width (count %)) (every? scalar? %)) rows)
                    (nat-int? fuel) (<= fuel 4096))
       (reject nil "Input or budget outside profile"))
     (let [schema (if transition? (update-vals sources :schema) (:schema db))
           plan (admit profile form attrs width schema)]
       (run profile fuel (fn [m] (evaluate plan sources rows #(spend! m %))))))))
