(ns datahike.test.external-engine-query-spec-test
  "The external-engine executor lets a secondary index own its query-spec format
   via an optional `:query-spec-fn` in the `:datahike/external-engine` metadata,
   with a backward-compatible `{:query :field}` default."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.query.execute]))

(def ^:private build @#'datahike.query.execute/external-query-spec)

(deftest default-query-spec-is-backward-compatible
  (testing "no :query-spec-fn → the legacy {:query <arg0> :field <arg1>} shape"
    (is (= {:query :a :field :b} (build {} [:a :b])))
    (is (= {:query :a :field :b} (build {} [:a :b :c])) "extra args ignored by default")
    (is (= {:query :a :field nil} (build {} [:a])) "missing field is nil")
    (is (= {:query :a :field :b} (build {:some :other-meta} [:a :b])))))

(deftest custom-query-spec-fn-is-honored
  (testing ":query-spec-fn builds an arbitrary, arity-free opaque spec"
    (let [em {:query-spec-fn (fn [args]
                               {:pattern (nth args 0)
                                :depth (nth args 1)
                                :flags (nth args 2)})}]
      (is (= {:pattern :p :depth 3 :flags :f} (build em [:p 3 :f]))
          "index receives its own >2-arity spec, not {:query :field}"))
    (testing "the fn receives a vector of the args"
      (is (vector? (build {:query-spec-fn identity} '(:x :y)))))))
