(ns datahike.test.cljs-pattern-scan-test
  "Regression tests for the CLJS bug fixed by this PR.

   In CLJS, `execute-pattern-scan` previously materialized each datom
   as a Clojure vector `[(.-e d) (.-a d) (.-v d) (.-tx d) true]`. The
   downstream join machinery in `query.cljc` / `query/relation.cljc`
   read tuple elements via `da/aget`, which on CLJS expands to
   `(arr[i])`. PersistentVectors don't expose elements as
   integer-indexed JS properties, so every tuple access returned nil.

   Symptom: multi-clause queries such as
       [:find [?var ...] :where [?a ...] [?b ?join ?a]]
   would join on nil keys (cross-product), and FindColl extracts
   would return `[nil]` instead of the expected results.

   The fix has two parts:
     1) execute-pattern-scan now produces JS arrays on CLJS.
     2) tuple access sites use `get` (works uniformly on JS arrays,
        Object[], and PersistentVectors) instead of `da/aget`.

   On CLJ this code path was masked by `*force-legacy* = true` (the
   new planner is opt-in via DATAHIKE_QUERY_PLANNER); on CLJS the new
   planner is the default."
  (:require
   #?(:clj  [clojure.test :as t :refer [is]]
      :cljs [cljs.test :as t :refer-macros [is]])
   [clojure.core.async :as a :refer [<!]]
   [datahike.api :as d]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

(defn- mem-cfg []
  {:store {:backend :memory
           :id #?(:clj (java.util.UUID/randomUUID)
                  :cljs (random-uuid))}
   :keep-history? false
   :schema-flexibility :read})

(defn- setup [cfg]
  (a/go
    #?(:clj  (do (d/create-database cfg)
                 (d/connect cfg))
       :cljs (do (<! (d/create-database cfg))
                 (<! (d/connect cfg {:sync? false}))))))

(defn- teardown [conn cfg]
  (a/go
    #?(:clj  (do (d/release conn)
                 (d/delete-database cfg))
       :cljs (do (d/release conn)
                 (<! (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; The failing query from the PR description:
;;   [:find [?var ...] :where [?a ...] [?b ?join ?a]]
;; Before the fix, FindColl returns [nil] on CLJS.

(deftest-async multi-clause-find-coll
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    ;; A handful of entities with two attributes; the query joins
    ;; on a value that's also an attribute name elsewhere.
    (<! (d/transact! conn [{:db/id -1 :a 1 :b 10}
                           {:db/id -2 :a 2 :b 20}
                           {:db/id -3 :a 3 :b 30}]))
    (let [;; Simplest multi-clause FindColl: project entity ids whose
          ;; :a value matches some entity's :b. Triggers hash-join
          ;; over scan-produced tuples.
          result (d/q '[:find [?e ...]
                        :where
                        [?e :a ?v]
                        [_ :b ?v]]
                      @conn)]
      ;; Pre-fix on CLJS: result was [nil] (single nil from
      ;; cross-product fed through nil-extracting join).
      ;; Post-fix: the query has no matches (no :b value equals
      ;; any :a value in this dataset) → result is empty.
      (is (not (= [nil] result))
          (str "Multi-clause FindColl must not return [nil]; got: "
               (pr-str result))))
    (<! (teardown conn cfg))))

;; ---------------------------------------------------------------------------
;; A positive case: matched join. Pre-fix: produced [nil] (or wrong
;; cardinality) due to nil tuple keys. Post-fix: returns the matched
;; entity ids.

(deftest-async two-clause-join-finds-matches
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    (<! (d/transact! conn [{:db/id -1 :name "Alice" :friend "Bob"}
                           {:db/id -2 :name "Bob"   :friend "Carol"}
                           {:db/id -3 :name "Carol" :friend "Dave"}]))
    (let [;; Find names of entities someone is a friend of.
          result (set (d/q '[:find [?name ...]
                             :where
                             [?e :friend ?fname]
                             [?f :name ?fname]
                             [?f :name ?name]]
                           @conn))]
      (is (= #{"Bob" "Carol"} result)
          (str "Expected {Bob, Carol}; got: " (pr-str result))))
    (<! (teardown conn cfg))))

;; ---------------------------------------------------------------------------
;; Single-clause FindColl as a control: this path doesn't exercise
;; hash-join, so it should pass even before the fix. Acts as a
;; sanity check that the test infrastructure itself is correct.

(deftest-async single-clause-find-coll-works
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    (<! (d/transact! conn [{:db/id -1 :name "Ivan"}
                           {:db/id -2 :name "Petr"}]))
    (let [result (set (d/q '[:find [?name ...]
                             :where [_ :name ?name]]
                           @conn))]
      (is (= #{"Ivan" "Petr"} result)))
    (<! (teardown conn cfg))))

(comment
  ;; CLJ REPL:
  (require 'datahike.test.cljs-pattern-scan-test :reload)
  (clojure.test/run-tests 'datahike.test.cljs-pattern-scan-test)
  ;; CLJS REPL (after `npx shadow-cljs watch cljs-tests`):
  ;; (cljs.test/run-tests 'datahike.test.cljs-pattern-scan-test)
  )
