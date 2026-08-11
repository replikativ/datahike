(ns datahike.test.malli-instrumentation-test
  "The API's malli schemas, enforced.

   `datahike.api.specification` has carried `[:=> …]` function schemas for every
   public operation for a long time, but nothing ever checked them: `emit-api`
   used them only to derive `:arglists` and `:doc`, so they were never registered
   and `malli.instrument/instrument!` had nothing to instrument. The `specs` test
   tier meanwhile ran the whole suite a third time under `kaocha.plugin/orchestra`
   — a *clojure.spec* `fdef` instrumenter — against **zero** fdefs. Measured:

       instrumentable fdef'd vars: 0
       registered specs (s/def):   145   ; all DATA specs, in config/schema

   So the declared API contract and the real one had drifted with nothing to
   notice. When they were first registered and instrumented, 7 of 48 operations
   were broken:

     * `datoms` / `seek-datoms` / `rseek-datoms` — two `:function` branches BOTH
       accepting 2 args, so malli took the arg-map one and the canonical
       `(d/datoms db :eavt)` reported `:malli.core/invalid-input`. The declared
       contract rejected the documented call.
     * `as-of` / `since` / `gc-storage` — referenced `types/time-point?`, a bare
       SYMBOL, where the registry key is `:datahike/time-point?`. Uncompilable.
     * `with` — two 2-arity branches, `:malli.core/duplicate-arities`. This one
       is NOT fixed: those two branches are the Java binding's two overloads,
       one of which marshals through `Util.normalizeCollections`, and merging
       them deletes it from the generated source. The binding wins; `with` is
       excluded by name, in `datahike.api/uninstrumentable`.

   That matters past the tests: this same specification generates the Java API,
   the TypeScript definitions, the HTTP routes and the CLI.

   ## Registration is not instrumentation

   `datahike.api/register-api-schemas!` runs at load and only records the
   schemas. Nothing is wrapped and nothing is validated, so a user who never
   calls malli's instrumenter sees exactly the behaviour they always did — which
   the first test here pins. Instrumentation is opt-in, here and for users."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.api :as d :refer [uninstrumentable]]
            [datahike.api.specification :refer [api-specification]]
            [malli.core :as m]
            [malli.instrument :as mi]
            [datahike.test.utils :as utils]))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(def ^:private violations (atom []))

(defn- with-instrumentation
  "Instrument for the duration, collecting violations instead of throwing, then
   un-instrument. `mi/unstrument!` restores the original vars, so nothing here
   leaks into the rest of the suite."
  [f]
  (reset! violations [])
  (mi/instrument! {:report (fn [type data]
                             (swap! violations conj {:type type :fn (:fn-name data)}))})
  (try (f) (finally (mi/unstrument!))))

(use-fixtures :each (fn [t] (with-instrumentation t)))

;; ---------------------------------------------------------------------------

(deftest registration-alone-changes-nothing
  (testing "the schemas are registered at load, but datahike does not instrument
            itself — a user who never opts in must see the untouched functions"
    ;; This test runs INSIDE the fixture, so unstrument first to observe the
    ;; default state, then let the fixture's finally re-run harmlessly.
    (mi/unstrument!)
    (let [conn (utils/setup-db (mem-cfg))]
      (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:n 1}])
      (is (= 1 (d/q '[:find ?n . :where [?e :n ?n]] @conn)))
      ;; a violation that WOULD be caught when instrumented passes through here
      (is (thrown? Exception (d/datoms @conn :not-an-index))
          "still fails, but from the implementation rather than the schema")
      (is (empty? @violations) "and nothing was reported by malli")
      (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))))

(deftest every-api-schema-compiles
  (testing "an uncompilable schema is silently unenforced — and four of them
            were, which is how they stayed wrong. This is the cheap guard."
    (let [bad (for [[n {:keys [args]}] api-specification
                    :when (not (contains? uninstrumentable n))
                    :let [err (try (m/schema args (m/-registry)) nil
                                   (catch Exception e (:type (ex-data e))))]
                    :when err]
                [n err])]
      (is (empty? bad) (str "uncompilable API schemas: " (pr-str (vec bad))))))
  (testing "and the exclusions are a SHORT, named list — if this grows, the
            registration is quietly checking less than it appears to"
    (is (= #{'with} uninstrumentable))))

(deftest the-schemas-are-registered-for-anyone-who-instruments
  (testing "registration is global, so a user running their own
            `(malli.instrument/instrument!)` gets datahike's API checked too —
            that is the whole reason to register rather than keep them private"
    (let [registered (get (m/function-schemas) 'datahike.api)]
      (is (= (- (count api-specification) (count uninstrumentable)) (count registered))
          "every specified operation except the named exclusions is registered")
      (is (contains? registered 'datoms))
      (is (contains? registered 'transact)))))

(deftest the-ordinary-api-does-not-violate-its-own-contract
  (testing "THE case. `(d/datoms db :eavt)` is the documented call and the
            declared schema rejected it — instrumented, correct usage reported
            `:malli.core/invalid-input`. Every operation this exercises must now
            pass its own schema."
    (let [conn (utils/setup-db (mem-cfg))]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/unique :db.unique/identity}
                        {:db/ident :n :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:name "a" :n 1} {:name "b" :n 2}])
      (let [db @conn]
        (d/q '[:find ?n :where [?e :n ?n]] db)
        (d/datoms db :eavt)                    ; the call that used to violate
        (d/datoms db :eavt 1)
        (d/datoms db {:index :eavt})           ; the arg-map form, same op
        (d/seek-datoms db :eavt)
        (d/rseek-datoms db :eavt)
        (d/entity db [:name "a"])
        (d/pull db '[*] [:name "a"])
        (d/with db [{:n 3}])                   ; excluded from instrumentation
        (d/as-of db (java.util.Date.))
        (d/since db (java.util.Date.))
        (d/history db)
        (d/schema db)
        (d/metrics db))
      (is (empty? @violations)
          (str "correct usage violated its own schema: " (pr-str @violations)))
      (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))))

(deftest instrumentation-catches-a-wrong-argument
  (testing "and the point of all this — a bad index is reported by the schema,
            by name, before the implementation has to deal with it"
    (let [conn (utils/setup-db (mem-cfg))]
      (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (try (d/datoms @conn :not-an-index) (catch Exception _ nil))
      (is (some #(= 'datahike.api/datoms (:fn %)) @violations)
          "the violation names the function")
      (is (some #(= :malli.core/invalid-input (:type %)) @violations))
      (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))))
