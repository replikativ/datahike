(ns datahike.codegen.native
  "Generate GraalVM C entry points from api.specification.

  This namespace generates LibDatahike.java containing @CEntryPoint methods
  that expose the Datahike API to native callers via GraalVM Native Image.

  Key design decisions:
  - Callback-based API (OutputReader) to avoid shared mutable memory
  - String-based data exchange (EDN/JSON/CBOR) for safety
  - Input format strings for temporal variants (history, since, asof)
  - No exposed connection objects - connections are internal"
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :as naming]
            [datahike.codegen.validation :as validation]
            [clojure.string :as str]
            [clojure.java.io :as io]))

;; =============================================================================
;; Native Operation Configuration
;; =============================================================================

(def native-operations
  "Operations exposed via native C API.
   Maps operation names to their binding pattern and extra parameters."
  '{;; Database lifecycle - take config, return result
    database-exists?
    {:pattern :config-query
     :java-call "Datahike.databaseExists(readConfig(db_config))"}

    create-database
    {:pattern :config-mutation
     :java-call "Datahike.createDatabase(readConfig(db_config))"}

    delete-database
    {:pattern :config-mutation
     :java-call "Datahike.deleteDatabase(readConfig(db_config))"}

    ;; Query - variable inputs
    q
    {:pattern :query
     :java-call "Datahike.q(CTypeConversion.toJavaString(query_edn), loadInputs(num_inputs, input_formats, raw_inputs))"}

    ;; Transaction
    transact
    {:pattern :transact
     :java-call "((java.util.Map)Datahike.transact(Datahike.connect(readConfig(db_config)), (java.util.List)loadInput(tx_format, tx_data))).get(Util.kwd(\":tx-meta\"))"}

    ;; Pull API
    pull
    {:pattern :db-selector-eid
     :java-call "Datahike.pull(loadInput(input_format, raw_input), CTypeConversion.toJavaString(selector_edn), eid)"}

    pull-many
    {:pattern :db-selector-eids
     :java-call "Datahike.pullMany(loadInput(input_format, raw_input), CTypeConversion.toJavaString(selector_edn), parseIterable(eids_edn))"}

    ;; Entity
    entity
    {:pattern :db-eid
     :java-call "entityToMap(Datahike.entity(loadInput(input_format, raw_input), eid))"}

    ;; Index operations
    datoms
    {:pattern :db-index
     :java-call "datomsToVecs((Iterable<?>)Datahike.datoms(loadInput(input_format, raw_input), parseKeyword(index_edn)))"}

    seek-datoms
    {:pattern :db-index
     :c-name "seek_datoms"
     :java-call "datomsToVecs((Iterable<?>)Datahike.seekDatoms(loadInput(input_format, raw_input), parseKeyword(index_edn)))"}

    index-range
    {:pattern :db-index-range
     :c-name "index_range"
     :java-call "datomsToVecs((Iterable<?>)Datahike.indexRange(loadInput(input_format, raw_input), Util.map(Util.kwd(\":attrid\"), parseKeyword(attrid_edn), Util.kwd(\":start\"), libdatahike.parseEdn(CTypeConversion.toJavaString(start_edn)), Util.kwd(\":end\"), libdatahike.parseEdn(CTypeConversion.toJavaString(end_edn)))))"}

    ;; Schema
    schema
    {:pattern :db-only
     :java-call "Datahike.schema(loadInput(input_format, raw_input))"}

    reverse-schema
    {:pattern :db-only
     :c-name "reverse_schema"
     :java-call "Datahike.reverseSchema(loadInput(input_format, raw_input))"}

    ;; Diagnostics
    metrics
    {:pattern :db-only
     :java-call "Datahike.metrics(loadInput(input_format, raw_input))"}

    ;; Maintenance
    gc-storage
    {:pattern :config-timestamp
     :c-name "gc_storage"
     :java-call "Datahike.gcStorage(Datahike.connect(readConfig(db_config)), new Date(before_tx_unix_time_ms))"}})

(def native-excluded-operations
  "Operations explicitly excluded from native C API with documented reasons.

  Each entry maps operation symbol to exclusion reason string."
  '{connect "Connection lifecycle managed internally per C API call"
    release "Connections automatically released after each operation"
    db "Database dereferencing handled internally via input_format parameter"
    listen "Requires persistent callbacks across FFI boundary - not supported"
    unlisten "Requires persistent callbacks across FFI boundary - not supported"
    as-of "Returns DB object - use input_format='asof:timestamp_ms' instead"
    since "Returns DB object - use input_format='since:timestamp_ms' instead"
    history "Returns DB object - use input_format='history' instead"
    filter "Returns DB object - cannot be serialized across FFI boundary"
    with "Pure function better implemented client-side"
    db-with "Pure function better implemented client-side"
    tempid "Temporary IDs only useful within transaction context"
    entity-db "Returns DB from entity - limited utility in FFI context"
    is-filtered "Returns boolean about DB state - limited utility in FFI context"
    transact! "Alias for transact - only need one binding"
    load-entities "Batch loading better done via repeated transact calls"
    query-stats "Query statistics not yet exposed in native API"})

;; =============================================================================
;; Naming Conventions
;; =============================================================================

(defn clj-name->c-name
  "Convert Clojure kebab-case to C snake_case."
  [op-name]
  (-> (name op-name)
      (str/replace #"[!?]$" "")
      (str/replace #"-" "_")))

(defn get-c-name
  "Get C function name, using override if specified."
  [op-name config]
  (or (:c-name config) (clj-name->c-name op-name)))

;; =============================================================================
;; Documentation Formatting
;; =============================================================================

(defn format-c-doc
  "Format operation documentation as C-style comment.

  Includes:
  - Main documentation text
  - Examples from specification (kept as Clojure syntax)

  Args:
    doc - Documentation string from api-specification
    examples - Example vector from api-specification

  Returns formatted C doc comment string"
  [doc examples]
  (let [doc-lines (str/split (or doc "No documentation available.") #"\n")
        example-text (when (seq examples)
                       (str "\n   * \n   * Examples:\n"
                            (str/join "\n"
                                      (for [{:keys [desc code]} examples]
                                        (str "   *   " desc ":\n"
                                             "   *     " code)))))]
    (str "    /**\n"
         (str/join "\n" (map #(str "     * " %) doc-lines))
         (or example-text "")
         "\n     */")))

;; =============================================================================
;; Code Generation Templates
;; =============================================================================

(defn generate-config-query
  "Generate entry point for config-based query operations.
   Pattern: (db_config) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-config-mutation
  "Generate entry point for config-based mutation operations.
   Pattern: (db_config) -> void (returns empty string)"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            %s;
            output_reader.call(toOutput(output_format, \"\"));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-query
  "Generate entry point for query operation.
   Pattern: (query, num_inputs, input_formats[], inputs[]) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer query_edn,
            long num_inputs,
            @CConst CCharPointerPointer input_formats,
            @CConst CCharPointerPointer raw_inputs,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-transact
  "Generate entry point for transact operation.
   Pattern: (db_config, tx_format, tx_data) -> tx-meta"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            @CConst CCharPointer tx_format,
            @CConst CCharPointer tx_data,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-only
  "Generate entry point for db-only operations.
   Pattern: (input_format, raw_input) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-eid
  "Generate entry point for db + eid operations.
   Pattern: (input_format, raw_input, eid) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            long eid,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-selector-eid
  "Generate entry point for pull operation.
   Pattern: (input_format, raw_input, selector, eid) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer selector_edn,
            long eid,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-selector-eids
  "Generate entry point for pull-many operation.
   Pattern: (input_format, raw_input, selector, eids) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer selector_edn,
            @CConst CCharPointer eids_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-index
  "Generate entry point for index operations.
   Pattern: (input_format, raw_input, index) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer index_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-db-index-range
  "Generate entry point for index-range operation.
   Pattern: (input_format, raw_input, attrid, start, end) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer attrid_edn,
            @CConst CCharPointer start_edn,
            @CConst CCharPointer end_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

(defn generate-config-timestamp
  "Generate entry point for config + timestamp operations.
   Pattern: (db_config, timestamp_ms) -> result"
  [op-name {:keys [java-call doc-comment] :as config}]
  (let [c-name (get-c-name op-name config)]
    (format "%s
    @CEntryPoint(name = \"%s\")
    public static void %s(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            long before_tx_unix_time_ms,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, %s));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }"
            (or doc-comment "") c-name c-name java-call)))

;; =============================================================================
;; Entry Point Generation
;; =============================================================================

(defn generate-entry-point
  "Generate a single @CEntryPoint method.

  Args:
    op-name - Operation symbol from api-specification
    spec - Full specification from api-specification
    overlay - Native-specific overlay configuration

  Returns generated Java code string"
  [op-name spec overlay]
  (let [;; Merge spec and overlay, adding formatted documentation
        doc-comment (format-c-doc (:doc spec) (:examples spec))
        config (assoc overlay :doc-comment doc-comment)
        ;; Select appropriate generator based on pattern
        generator (case (:pattern overlay)
                    :config-query generate-config-query
                    :config-mutation generate-config-mutation
                    :query generate-query
                    :transact generate-transact
                    :db-only generate-db-only
                    :db-eid generate-db-eid
                    :db-selector-eid generate-db-selector-eid
                    :db-selector-eids generate-db-selector-eids
                    :db-index generate-db-index
                    :db-index-range generate-db-index-range
                    :config-timestamp generate-config-timestamp
                    (throw (ex-info (str "Unknown pattern: " (:pattern overlay))
                                    {:op-name op-name :overlay overlay})))]
    (generator op-name config)))

(defn generate-all-entry-points
  "Generate all @CEntryPoint methods using overlay model.

  Iterates over api-specification, checking each operation against:
  - native-operations (overlay config)
  - native-excluded-operations (exclusions)

  Operations must be either implemented or excluded."
  []
  (let [all-ops api-specification
        implemented (keys native-operations)
        excluded (keys native-excluded-operations)]
    (->> all-ops
         (keep (fn [[op-name spec]]
                 (cond
                   ;; Explicitly excluded - skip
                   (contains? (set excluded) op-name)
                   nil

                   ;; Has overlay config - generate it
                   (contains? native-operations op-name)
                   (let [overlay (get native-operations op-name)]
                     (generate-entry-point op-name spec overlay))

                   ;; Missing overlay - warn (but continue)
                   :else
                   (do
                     (println (str "⚠️  WARNING: No native overlay for operation: " op-name))
                     nil))))
         (str/join "\n"))))

;; =============================================================================
;; File Generation
;; =============================================================================

(def file-header
  "package datahike.impl;

import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CConst;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import java.util.Date;

/**
 * Generated C entry points for libdatahike.
 *
 * This file is auto-generated from datahike.api.specification.
 * DO NOT EDIT MANUALLY - changes will be overwritten.
 *
 * To regenerate: bb codegen-native
 *
 * All entry points use callback-based return (OutputReader) to:
 * - Avoid shared mutable memory between native and JVM
 * - Support multiple output formats (json, edn, cbor)
 * - Enable proper exception handling
 *
 * Input format strings for temporal queries:
 * - \"db\" : Current database state
 * - \"history\" : Full history including retractions
 * - \"since:{timestamp_ms}\" : Database since timestamp
 * - \"asof:{timestamp_ms}\" : Database as-of timestamp
 */
public final class LibDatahike extends LibDatahikeBase {
")

(def file-footer
  "
    // Note: history/since/as-of are handled via input format strings
    // e.g., input_format=\"history\" or \"since:1234567890\"

    // seekdatoms returns lazy sequence - results are fully realized
    // release not exposed - connections are created internally per call
}
")

(defn generate-file-content
  "Generate complete LibDatahike.java content."
  []
  (str file-header
       (generate-all-entry-points)
       file-footer))

(defn write-libdatahike!
  "Write generated LibDatahike.java to output directory."
  [output-dir]
  (let [output-path (str output-dir "/datahike/impl/LibDatahike.java")
        content (generate-file-content)]
    (io/make-parents output-path)
    (spit output-path content)
    (println "Generated:" output-path)))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  "Generate native bindings with coverage validation.
   Usage: clojure -M -m datahike.codegen.native <output-dir>"
  [& args]
  (let [output-dir (or (first args) "libdatahike/src-generated")]
    (println "Generating native C entry points from specification...")

    ;; Validate coverage
    (validation/validate-coverage "Native" native-operations native-excluded-operations)
    (validation/validate-exclusion-reasons "Native" native-excluded-operations)
    (validation/validate-overlay-completeness "Native" native-operations [:pattern :java-call])

    ;; Generate bindings
    (write-libdatahike! output-dir)
    (println "Done.")))
