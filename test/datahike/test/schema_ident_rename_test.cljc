(ns datahike.test.schema-ident-rename-test
  "Renaming an attribute: when it is allowed, and what it means when it is.

   Two independent things are pinned here.

   `update-schema` must be TOTAL on a repeated `:db/ident` assertion. It stores a
   POINTER at `[:schema e] -> ident`, and its `:db/ident` branch used to `merge`
   into `(schema e)` without following it — so a second `:db/ident` for the same
   entity met a keyword where a map belongs and threw ClassCastException. That
   made the outcome depend on the CALLER's datom ORDER, which no caller controls:
   a retraction clears the pointer, so retract-before-assert survived and
   assert-before-retract did not.

   Whether a rename is ALLOWED is separate, and turns on `:attribute-refs?`,
   because that is what decides whether a datom names its attribute by keyword or
   by entity id — i.e. whether the data can follow the rename at all.

   `.cljc` but deliberately NOT listed in `nodejs_test.cljs`, and that is a
   claim rather than an oversight: unlike the binding-seam law — where a CLJS
   merge kernel was a genuine twin that kept the old behaviour while every JVM
   run stayed green — neither fix here has a platform arm. `update-schema`'s
   pointer resolution is a map lookup and `validate-ident-renames!` is a config
   read plus an index slice, identical on both runtimes. If a cljs arm ever
   appears in either, this namespace should be converted to `deftest-async` and
   registered."
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.api :as d]))

(defn- cfg [attribute-refs?]
  {:store {:backend :memory :id #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))}
   :keep-history? true
   :schema-flexibility :write
   :attribute-refs? attribute-refs?})

(defn- with-conn [attribute-refs? f]
  (let [c (cfg attribute-refs?)]
    (d/create-database c)
    (let [conn (d/connect c)]
      (try (f conn) (finally (d/release conn))))))

(defn- declare-attr! [conn]
  (d/transact conn [{:db/ident :test/name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])
  (:db/id (d/entity @conn :test/name)))

(defn- rename-tx [aid order]
  (case order
    :add     [[:db/add aid :db/ident :test/renamed]]
    :ret-add [[:db/retract aid :db/ident :test/name]
              [:db/add aid :db/ident :test/renamed]]
    :add-ret [[:db/add aid :db/ident :test/renamed]
              [:db/retract aid :db/ident :test/name]]))

(defn- root-ex-data
  "The ex-data of the DEEPEST cause that carries any — the writer wraps a
   transaction failure in its own `:datahike/write-error`, so the outermost
   ex-data describes the wrapper rather than the refusal."
  [e]
  #?(:clj  (loop [x e, found (ex-data e)]
             (if-let [c (.getCause x)]
               (recur c (or (ex-data c) found))
               (or (ex-data x) found)))
     :cljs (ex-data e)))

(defn- rename-error
  "nil when the rename succeeded, else the ex-data of the refusal."
  [conn aid order]
  (try (d/transact conn (rename-tx aid order)) nil
       (catch #?(:clj Exception :cljs :default) e
         (or (root-ex-data e) {:error :unknown}))))

(deftest rename-always-reaches-a-decision
  (testing "every datom order either succeeds or REFUSES; none fails incidentally"
    (doseq [refs? [false true]
            order [:add :ret-add :add-ret]]
      (with-conn refs?
        (fn [conn]
          (let [aid (declare-attr! conn)]
            (d/transact conn [{:db/id 1000 :test/name "hello"}])
            ;; Asserted on the CONTRACT rather than on an exception class: a
            ;; refusal is an ex-info carrying `:error :transact/schema`, while
            ;; the bug was a ClassCastException escaping `merge` — which carries
            ;; no ex-data at all. That distinction is the one that matters and it
            ;; is the same on both runtimes, where the exception CLASS is not
            ;; (cljs raises a protocol-dispatch error, not a cast error).
            (let [outcome (try (d/transact conn (rename-tx aid order)) ::ok
                               (catch #?(:clj Exception :cljs :default) e
                                 (:error (root-ex-data e))))]
              (is (contains? #{::ok :transact/schema} outcome)
                  (str "attribute-refs? " refs? ", order " order
                       " failed incidentally rather than deciding: "
                       (pr-str outcome))))))))))

(deftest rename-refused-without-attribute-refs
  (testing "the attribute keyword IS the storage key, so a rename would split the data"
    (doseq [order [:add :ret-add :add-ret]]
      (with-conn false
        (fn [conn]
          (let [aid (declare-attr! conn)]
            (d/transact conn [{:db/id 1000 :test/name "hello"}])
            (let [err (rename-error conn aid order)]
              (is (some? err) (str "order " order " should be refused"))
              (is (= :transact/schema (:error err))))))))))

(deftest rename-of-an-unused-attribute-is-allowed
  (testing "nothing to split when no datom carries the old keyword"
    (with-conn false
      (fn [conn]
        (let [aid (declare-attr! conn)]
          (is (nil? (rename-error conn aid :add)))
          (d/transact conn [{:db/id 1000 :test/renamed "ok"}])
          (is (= #{["ok"]} (d/q '[:find ?v :where [1000 :test/renamed ?v]] @conn))))))))

(deftest rename-carries-the-data-under-attribute-refs
  (testing "datoms name the attribute by ENTITY id, so the rename is meaningful"
    (doseq [order [:add :ret-add :add-ret]]
      (with-conn true
        (fn [conn]
          (let [aid (declare-attr! conn)]
            (d/transact conn [{:db/id 1000 :test/name "hello"}])
            (is (nil? (rename-error conn aid order))
                (str "order " order " should be allowed under :attribute-refs?"))
            (is (= #{["hello"]}
                   (d/q '[:find ?v :where [1000 :test/renamed ?v]] @conn))
                (str "order " order " — existing data must answer to the new name"))
            ;; the attribute entity is unchanged; only its name moved
            (is (= aid (:db/id (d/entity @conn :test/renamed))))))))))
