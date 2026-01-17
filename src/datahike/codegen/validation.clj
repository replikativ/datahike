(ns datahike.codegen.validation
  "Validation utilities for code generation coverage.

  Ensures all API operations are either implemented or explicitly excluded
  across all language bindings."
  (:require [datahike.api.specification :refer [api-specification]]
            [clojure.string :as str]
            [clojure.set :as set]))

;; =============================================================================
;; Coverage Validation
;; =============================================================================

(defn validate-coverage
  "Validate that all operations are either implemented or excluded.

  Args:
    binding-name - String name for display (e.g., 'Native', 'Python')
    operations - Map of implemented operations
    excluded - Map or set of excluded operations

  Returns:
    Map with :missing, :implemented, :excluded, :total"
  [binding-name operations excluded]
  (let [all-ops (set (keys api-specification))
        implemented (set (keys operations))
        ;; Handle both maps (with reasons) and sets (just symbols)
        excluded-ops (if (map? excluded)
                       (set (keys excluded))
                       (set excluded))
        missing (set/difference all-ops implemented excluded-ops)
        total (count all-ops)
        impl-count (count implemented)
        excl-count (count excluded-ops)
        miss-count (count missing)]

    ;; Print warnings for missing operations
    (when (seq missing)
      (println (str "\n⚠️  WARNING: " binding-name " bindings missing operations:"))
      (doseq [op (sort missing)]
        (println (str "  - " op)))
      (println (str "\n  Either add to " (str/lower-case binding-name) "-operations"
                    " or " (str/lower-case binding-name) "-excluded-operations\n")))

    {:binding binding-name
     :total total
     :implemented impl-count
     :excluded excl-count
     :missing miss-count
     :missing-ops (sort missing)
     :coverage-pct (int (* 100 (/ (+ impl-count excl-count) total)))}))

;; =============================================================================
;; Coverage Reporting
;; =============================================================================

(defn print-coverage-line
  "Print a single line of the coverage table."
  [{:keys [binding total implemented excluded missing coverage-pct]}]
  (let [status (if (zero? missing) "✅" "⚠️ ")
        impl-excl (if (zero? excluded)
                    (format "%2d" implemented)
                    (format "%2d+%d" implemented excluded))
        coverage-str (format "%3d%%" coverage-pct)]
    (printf "%-12s %s/%2d (%s) %s%s\n"
            (str binding ":")
            impl-excl
            total
            coverage-str
            status
            (if (zero? excluded)
              ""
              (format " (%d excluded)" excluded)))))

(defn print-coverage-report
  "Print coverage report for all bindings.

  Args:
    results - Sequence of validation results from validate-coverage"
  [results]
  (println "\nCode Generation Coverage Report")
  (println "================================\n")
  (printf "Total operations in api-specification: %d\n\n"
          (:total (first results)))

  (doseq [result (sort-by :binding results)]
    (print-coverage-line result))

  ;; Print missing operations detail
  (let [missing-any (filter #(pos? (:missing %)) results)]
    (when (seq missing-any)
      (println "\nMissing Operations:")
      (doseq [{:keys [binding missing-ops]} missing-any]
        (println (str "\n" binding ":"))
        (doseq [op missing-ops]
          (println (str "  - " op))))))

  (println))

(defn validate-exclusion-reasons
  "Validate that all exclusions have documented reasons.

  Args:
    binding-name - String name for display
    excluded - Map of excluded operations to reason strings, or set (skips validation)

  Returns true if all have reasons (or is a set), prints warnings otherwise"
  [binding-name excluded]
  ;; Skip validation for sets (they don't have reasons)
  (if-not (map? excluded)
    true
    (let [missing-reasons (filter (fn [[_ reason]]
                                    (or (nil? reason)
                                        (str/blank? reason)))
                                  excluded)]
      (when (seq missing-reasons)
        (println (str "\n⚠️  WARNING: " binding-name " exclusions missing reasons:"))
        (doseq [[op _] missing-reasons]
          (println (str "  - " op)))
        (println))
      (empty? missing-reasons))))

;; =============================================================================
;; Overlay Validation
;; =============================================================================

(defn validate-overlay-completeness
  "Validate that overlay provides required config for each operation.

  Args:
    binding-name - String name for display
    operations - Map of operation overlays
    required-keys - Vector of required keys in overlay (e.g., [:pattern :java-call])

  Prints warnings for incomplete overlays"
  [binding-name operations required-keys]
  (let [incomplete (filter (fn [[op-name config]]
                             (not-every? #(contains? config %) required-keys))
                           operations)]
    (when (seq incomplete)
      (println (str "\n⚠️  WARNING: " binding-name " incomplete overlay configs:"))
      (doseq [[op-name config] incomplete]
        (let [missing (filter #(not (contains? config %)) required-keys)]
          (println (str "  - " op-name ": missing " (str/join ", " missing)))))
      (println))))
