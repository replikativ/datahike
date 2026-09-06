(ns datahike.test.codegen-or-args-test
  "`[:or …]` in argument position, and what the Java emitter is allowed to do
   with it.

   An `[:or A B]` argument is ONE Clojure parameter admitting two shapes. Java
   has no such type. Collapsing it to `Object` loses the `List` overload that
   carries `Util.normalizeCollections`; expanding it into one overload per Java
   type can DELETE an existing signature and break callers at compile time.

   `expand-or-args` expands only when that is purely additive. This pins both
   sides of the gate, because getting either wrong is silent: the schemas would
   still compile, the suite would still pass, and the damage would only appear
   in someone else's build."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.codegen.java :as j]
            [datahike.api.specification :refer [api-specification]]))

(defn- sigs
  "Java signatures the emitter would produce, as sets of type vectors."
  [args-schema]
  (set (map #(mapv :type %) (j/expand-or-args args-schema))))

(defn- spec-sigs [op] (sigs (:args (get api-specification op))))

(deftest additive-expansion-happens
  (testing "`transact` accepts either a transaction vector or an arg-map. Both
            overloads must exist: the `List` one so Java collections marshal
            through `Util.normalizeCollections`, the `Object` one so the
            arg-map form is callable at all."
    (is (= #{["Object" "List"] ["Object" "Object"]} (spec-sigs 'transact)))
    (is (= #{["Object" "List"] ["Object" "Object"]} (spec-sigs 'transact!)))
    (is (= #{["Object" "List"] ["Object" "Object"]} (spec-sigs 'db-with))))

  (testing "`with` keeps the original three overloads and adds the transaction
            options arity without changing how the existing calls marshal."
    (is (= #{["Object" "List"] ["Object" "Object"] ["Object" "List" "Object"]
             ["Object" "List" "Object" "Object"]}
           (spec-sigs 'with)))))

(deftest non-additive-expansion-is-refused
  (testing "`q`/`explain`/`query-stats` declare `[:or [:vector :any] :map
            :string]`, which maps to THREE DIFFERENT Java types. Expanding
            replaces the `Object` overload rather than adding to it, and every
            caller holding a variable declared `Object` stops compiling. The
            collapsed rendering must survive."
    (is (contains? (spec-sigs 'q) ["Object" "Object"]))
    (is (contains? (spec-sigs 'explain) ["Object" "Object"]))
    (is (contains? (spec-sigs 'query-stats) ["Object" "Object"]))
    (testing "and nothing narrower is emitted beside it"
      (is (= #{["Object"] ["Object" "Object"]} (spec-sigs 'q))))))

(deftest branches-agreeing-in-java-collapse-to-one-overload
  (testing "`entity`'s `[:or :datahike/SEId :any]` maps both branches to
            `Object`. Two identical signatures are a duplicate method, not an
            overload — Java would refuse to compile the class."
    (is (= #{["Object" "Object"]} (spec-sigs 'entity)))))

(deftest no-operation-emits-a-duplicate-signature
  (testing "across the WHOLE specification, since a duplicate anywhere breaks
            the build for everyone rather than just its own operation"
    (let [dupes (for [[op {:keys [args]}] api-specification
                      :let [all (map #(mapv :type %) (j/expand-or-args args))]
                      :when (not= (count all) (count (set all)))]
                  op)]
      (is (empty? dupes) (str "duplicate Java signatures: " (pr-str (vec dupes)))))))

(deftest expansion-is-additive-for-every-operation
  (testing "the gate itself, stated as the invariant rather than checked per
            operation: whatever `expand-or-args` returns, it never drops a
            signature the collapsed rendering would have produced"
    (let [lost (for [[op {:keys [args]}] api-specification
                     :let [collapsed (if (= :multi-arity (j/extract-params-from-schema args))
                                       (j/extract-multi-arity-params args)
                                       [(j/extract-params-from-schema args)])
                           got (sigs args)
                           missing (remove got (map #(mapv :type %) collapsed))]
                     :when (seq missing)]
                 [op (vec missing)])]
      (is (empty? lost) (str "signatures dropped: " (pr-str (vec lost)))))))
