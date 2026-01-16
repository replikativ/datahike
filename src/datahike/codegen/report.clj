(ns datahike.codegen.report
  "Generate coverage reports for all code generation bindings."
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.validation :as validation]
            [datahike.codegen.native :refer [native-operations native-excluded-operations]]
            [datahike.codegen.python :refer [python-operations python-excluded-operations]]
            [datahike.codegen.naming :refer [js-skip-list]]
            [datahike.codegen.cli :refer [cli-excluded-operations]]))

(defn generate-report
  "Generate and print coverage report for all bindings."
  []
  (let [;; Validate each binding
        java-result {:binding "Java"
                     :total (count api-specification)
                     :implemented (count api-specification)
                     :excluded 0
                     :missing 0
                     :missing-ops []
                     :coverage-pct 100}

        ;; TypeScript - all ops except js-skip-list
        typescript-ops (into {} (remove #(contains? js-skip-list (first %)) api-specification))
        typescript-excluded (zipmap js-skip-list (repeat "ClojureScript incompatible"))
        typescript-result (validation/validate-coverage
                           "TypeScript"
                           typescript-ops
                           typescript-excluded)

        ;; CLI - all ops except cli-excluded-operations
        cli-ops (into {} (remove #(contains? cli-excluded-operations (first %)) api-specification))
        cli-result (validation/validate-coverage
                    "CLI"
                    cli-ops
                    cli-excluded-operations)

        native-result (validation/validate-coverage
                       "Native"
                       native-operations
                       native-excluded-operations)

        python-result (validation/validate-coverage
                       "Python"
                       python-operations
                       python-excluded-operations)

        all-results [java-result typescript-result cli-result native-result python-result]]

    ;; Print report
    (validation/print-coverage-report all-results)))

(defn -main
  "Generate and print coverage report."
  [& _args]
  (generate-report))
