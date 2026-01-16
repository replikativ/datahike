(ns datahike.codegen.python
  "Generate Python bindings from api.specification.

  This namespace generates Python code with:
  - Full type annotations (PEP 484)
  - Docstrings from specification
  - ctypes FFI calls to libdatahike"
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :as naming]
            [datahike.codegen.validation :as validation]
            [clojure.string :as str]
            [clojure.java.io :as io]))

;; =============================================================================
;; Python Operation Configuration
;; =============================================================================

(def python-operations
  "Operations exposed via Python API.
   Maps operation names to their binding configuration."
  '{;; Database lifecycle
    database-exists?
    {:pattern :config-query
     :return-type "bool"}

    create-database
    {:pattern :config-mutation
     :return-type "None"}

    delete-database
    {:pattern :config-mutation
     :return-type "None"}

    ;; Query
    q
    {:pattern :query
     :return-type "Any"}

    ;; Transaction
    transact
    {:pattern :transact
     :return-type "Dict[str, Any]"}

    ;; Pull API
    pull
    {:pattern :db-selector-eid
     :return-type "Optional[Dict[str, Any]]"}

    pull-many
    {:pattern :db-selector-eids
     :return-type "List[Dict[str, Any]]"}

    ;; Entity
    entity
    {:pattern :db-eid
     :return-type "Dict[str, Any]"}

    ;; Index operations
    datoms
    {:pattern :db-index
     :return-type "List[List[Any]]"}

    seek-datoms
    {:pattern :db-index
     :c-name "seek_datoms"
     :return-type "List[List[Any]]"}

    index-range
    {:pattern :db-index-range
     :c-name "index_range"
     :return-type "List[List[Any]]"}

    ;; Schema
    schema
    {:pattern :db-only
     :return-type "Dict[str, Any]"}

    reverse-schema
    {:pattern :db-only
     :c-name "reverse_schema"
     :return-type "Dict[str, Any]"}

    ;; Diagnostics
    metrics
    {:pattern :db-only
     :return-type "Dict[str, Any]"}

    ;; Maintenance
    gc-storage
    {:pattern :config-timestamp
     :c-name "gc_storage"
     :return-type "Any"}})

(def python-excluded-operations
  "Operations explicitly excluded from Python FFI bindings with documented reasons.

  Each entry maps operation symbol to exclusion reason string.
  Same exclusions as Native bindings since both use libdatahike FFI."
  '{connect "Connection lifecycle managed internally per FFI call"
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
    query-stats "Query statistics not yet exposed in Python FFI"})

;; =============================================================================
;; Type Derivation: Malli → Python
;; =============================================================================

(defn malli->python-type
  "Convert Malli schema to Python type annotation string.

  Handles common Malli schemas and semantic types from api.specification.
  Falls back to 'Any' for complex or unknown schemas."
  [schema]
  (cond
    ;; Keyword schemas (primitives)
    (keyword? schema)
    (case schema
      :boolean "bool"
      :string "str"
      :int "int"
      :long "int"
      :double "float"
      :number "float"
      :keyword "str"
      :symbol "str"
      :any "Any"
      :nil "None"
      :map "Dict[str, Any]"
      :vector "List[Any]"
      :sequential "List[Any]"
      :set "Set[Any]"
      "Any")

    ;; Symbol schemas (type references from api.types)
    (symbol? schema)
    (let [schema-name (name schema)]
      (case schema-name
        ;; Semantic Datahike types
        "SConnection" "Any"  ; Connection not exposed in FFI
        "SDB" "Any"          ; DB objects handled via input_format
        "SEntity" "Dict[str, Any]"
        "STransactionReport" "Dict[str, Any]"
        "SSchema" "Dict[str, Any]"
        "SMetrics" "Dict[str, Any]"
        "SDatoms" "List[List[Any]]"
        "SEId" "int"
        "SPullPattern" "str"
        "SConfig" "str"  ; EDN string in Python FFI
        "STransactions" "str"  ; EDN/JSON string
        "SQueryArgs" "Any"
        ;; Default
        "Any"))

    ;; Vector schemas (compound types)
    (vector? schema)
    (let [[op & args] schema]
      (case op
        ;; [:or Type1 Type2] → Union[Type1, Type2] or Any if too complex
        :or
        (let [python-types (map malli->python-type args)]
          (if (every? #(not= % "Any") python-types)
            (str "Union[" (str/join ", " python-types) "]")
            "Any"))

        ;; [:maybe Type] → Optional[Type]
        :maybe
        (str "Optional[" (malli->python-type (first args)) "]")

        ;; [:sequential Type] → List[Type]
        :sequential
        (str "List[" (malli->python-type (first args)) "]")

        ;; [:vector Type] → List[Type]
        :vector
        (str "List[" (malli->python-type (first args)) "]")

        ;; [:set Type] → Set[Type]
        :set
        (str "Set[" (malli->python-type (first args)) "]")

        ;; [:map ...] → Dict
        :map "Dict[str, Any]"

        ;; [:function ...] or [:=> ...] - extract return type
        (:function :=>)
        (if (= op :=>)
          (malli->python-type (nth schema 2))  ; [:=> input output]
          (malli->python-type (second schema))) ; [:function [:=> ...]]

        ;; Default
        "Any"))

    ;; Default
    :else "Any"))

;; =============================================================================
;; Naming Conventions
;; =============================================================================

(defn clj-name->python-name
  "Convert Clojure kebab-case to Python snake_case."
  [op-name]
  (-> (name op-name)
      (str/replace #"[!?]$" "")
      (str/replace #"-" "_")))

(defn get-c-name
  "Get C function name for FFI call."
  [op-name config]
  (or (:c-name config) (clj-name->python-name op-name)))

;; =============================================================================
;; Documentation Formatting
;; =============================================================================

(defn format-python-docstring
  "Format operation documentation as Python docstring.

  Includes:
  - Main documentation text
  - Examples from specification (kept as Clojure syntax)

  Args:
    doc - Documentation string from api-specification
    examples - Example vector from api-specification

  Returns formatted Python docstring"
  [doc examples]
  (let [main-doc (or doc "No documentation available.")
        example-text (when (seq examples)
                       (str "\n\n    Examples:\n"
                            (str/join "\n"
                                      (for [{:keys [desc code]} examples]
                                        (str "        " desc ":\n"
                                             "            " code)))))]
    (str main-doc example-text)))

;; =============================================================================
;; Code Generation Templates
;; =============================================================================

(defn generate-config-query
  "Generate Python function for config-based query operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-config-mutation
  "Generate Python function for config-based mutation operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        output_format: Output format ('json', 'edn', or 'cbor')
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    get_result()  # Check for exceptions
"
            py-name return-type doc c-name)))

(defn generate-query
  "Generate Python function for query operation."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    query: str,
    inputs: List[Tuple[str, str]],
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        query: Datalog query as EDN string
        inputs: List of (format, value) tuples. Formats:
            - 'db': Current database (value is config EDN)
            - 'history': Full history database
            - 'since:{timestamp_ms}': Database since timestamp
            - 'asof:{timestamp_ms}': Database as-of timestamp
            - 'json': JSON data
            - 'edn': EDN data
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Query result

    Example:
        >>> query('[:find ?e :where [?e :name \"Alice\"]]', [('db', config)])
    '''
    n, formats, values = prepare_query_inputs(inputs)
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        query.encode('utf8'),
        n,
        formats,
        values,
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc c-name)))

(defn generate-transact
  "Generate Python function for transact operation."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    tx_data: str,
    input_format: str = 'json',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        tx_data: Transaction data (format depends on input_format)
        input_format: Input data format ('json', 'edn', or 'cbor')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Transaction metadata

    Example:
        >>> transact(config, '[{\"name\": \"Alice\", \"age\": 30}]')
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        config.encode('utf8'),
        input_format.encode('utf8'),
        tx_data.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc c-name)))

(defn generate-db-only
  "Generate Python function for db-only operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-db-eid
  "Generate Python function for db + eid operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    eid: int,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        eid: Entity ID
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        eid,
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-db-selector-eid
  "Generate Python function for pull operation."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    selector: str,
    eid: int,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        selector: Pull pattern as EDN string (e.g., '[:db/id :name :age]' or '[*]')
        eid: Entity ID
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s

    Example:
        >>> pull(config, '[*]', 1)
        {':db/id': 1, ':name': 'Alice', ':age': 30}
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        selector.encode('utf8'),
        eid,
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-db-selector-eids
  "Generate Python function for pull-many operation."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    selector: str,
    eids: List[int],
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        selector: Pull pattern as EDN string
        eids: List of entity IDs
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    eids_edn = '[' + ' '.join(str(e) for e in eids) + ']'
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        selector.encode('utf8'),
        eids_edn.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-db-index
  "Generate Python function for index operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    index: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        index: Index keyword as EDN string (':eavt', ':aevt', ':avet', ':vaet')
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s (list of [e a v tx added?] vectors)
    '''
    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        index.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-db-index-range
  "Generate Python function for index-range operation."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    attrid: str,
    start: Any,
    end: Any,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        attrid: Attribute keyword as EDN string (e.g., ':age')
        start: Start value (will be converted to EDN)
        end: End value (will be converted to EDN)
        input_format: Database input format
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    # Convert Python values to EDN representation
    def to_edn(v):
        if isinstance(v, str):
            return f'\"{v}\"'
        elif isinstance(v, bool):
            return 'true' if v else 'false'
        elif v is None:
            return 'nil'
        else:
            return str(v)

    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        attrid.encode('utf8'),
        to_edn(start).encode('utf8'),
        to_edn(end).encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

(defn generate-config-timestamp
  "Generate Python function for config + timestamp operations."
  [op-name {:keys [return-type doc] :as config}]
  (let [py-name (clj-name->python-name op-name)
        c-name (get-c-name op-name config)]
    (format "
def %s(
    config: str,
    before_timestamp_ms: Optional[int] = None,
    output_format: str = 'cbor'
) -> %s:
    '''%s

    Args:
        config: Database configuration as EDN string
        before_timestamp_ms: Unix timestamp in milliseconds (optional)
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        %s
    '''
    import time
    if before_timestamp_ms is None:
        before_timestamp_ms = int(time.time() * 1000)

    callback, get_result = make_callback(output_format)
    _dll.%s(
        _isolatethread,
        config.encode('utf8'),
        before_timestamp_ms,
        output_format.encode('utf8'),
        callback
    )
    return get_result()
"
            py-name return-type doc return-type c-name)))

;; =============================================================================
;; Function Generation
;; =============================================================================

(defn generate-function
  "Generate a single Python function using overlay model.

  Args:
    op-name - Operation symbol from api-specification
    spec - Full specification from api-specification
    overlay - Python-specific overlay configuration

  Returns generated Python code string"
  [op-name spec overlay]
  (let [;; Derive or use return type
        return-type (or (:return-type overlay)
                        (malli->python-type (:ret spec)))
        ;; Format documentation
        docstring (format-python-docstring (:doc spec) (:examples spec))
        ;; Merge into config
        config (assoc overlay
                      :return-type return-type
                      :doc docstring)
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

(defn generate-all-functions
  "Generate all Python functions using overlay model.

  Iterates over api-specification, checking each operation against:
  - python-operations (overlay config)
  - python-excluded-operations (exclusions)

  Operations must be either implemented or excluded."
  []
  (let [all-ops api-specification
        implemented (keys python-operations)
        excluded (keys python-excluded-operations)]
    (->> all-ops
         (keep (fn [[op-name spec]]
                 (cond
                   ;; Explicitly excluded - skip
                   (contains? (set excluded) op-name)
                   nil

                   ;; Has overlay config - generate it
                   (contains? python-operations op-name)
                   (let [overlay (get python-operations op-name)]
                     (generate-function op-name spec overlay))

                   ;; Missing overlay - warn (but continue)
                   :else
                   (do
                     (println (str "⚠️  WARNING: No Python overlay for operation: " op-name))
                     nil))))
         (str/join "\n"))))

;; =============================================================================
;; File Generation
;; =============================================================================

(def file-header
  "\"\"\"Generated Datahike Python bindings.

This file is auto-generated from datahike.api.specification.
DO NOT EDIT MANUALLY - changes will be overwritten.

To regenerate: bb codegen-python

All functions use callback-based FFI to libdatahike with:
- Multiple output formats (json, edn, cbor)
- Proper exception handling
- Type annotations (PEP 484)

Temporal query variants via input_format parameter:
- 'db': Current database state
- 'history': Full history including retractions
- 'since:{timestamp_ms}': Database since timestamp
- 'asof:{timestamp_ms}': Database as-of timestamp
\"\"\"
from typing import Any, Dict, List, Tuple, Optional
from ._native import (
    _dll,
    _isolatethread,
    make_callback,
    prepare_query_inputs,
    DatahikeException,
)

__all__ = [
    'DatahikeException',
    'database_exists',
    'create_database',
    'delete_database',
    'q',
    'transact',
    'pull',
    'pull_many',
    'entity',
    'datoms',
    'seek_datoms',
    'index_range',
    'schema',
    'reverse_schema',
    'metrics',
    'gc_storage',
]
")

(def file-footer
  "

# Re-export exception
DatahikeException = DatahikeException
")

(defn generate-file-content
  "Generate complete generated.py content."
  []
  (str file-header
       (generate-all-functions)
       file-footer))

(defn write-python-bindings!
  "Write generated Python bindings to output directory."
  [output-dir]
  (let [output-path (str output-dir "/generated.py")
        content (generate-file-content)]
    (io/make-parents output-path)
    (spit output-path content)
    (println "Generated:" output-path)))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  "Generate Python bindings with coverage validation.
   Usage: clojure -M -m datahike.codegen.python <output-dir>"
  [& args]
  (let [output-dir (or (first args) "pydatahike/src/datahike")]
    (println "Generating Python bindings from specification...")

    ;; Validate coverage
    (validation/validate-coverage "Python" python-operations python-excluded-operations)
    (validation/validate-exclusion-reasons "Python" python-excluded-operations)
    (validation/validate-overlay-completeness "Python" python-operations [:pattern])

    ;; Generate bindings
    (write-python-bindings! output-dir)
    (println "Done.")))
