(ns datahike.codegen.ts-client
  "Generate the hand-written TypeScript thin client's API forwarding layer."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datahike.api.specification :refer [->url api-specification]]
            [datahike.codegen.naming :refer [assert-unique-js-names!
                                             clj-name->js-name
                                             remote-js-exports]]
            [datahike.codegen.typescript :as typescript]))

(def ^:private imported-types
  ["Connection" "ConnectOptions" "Database" "DatabaseConfig" "DatahikeUuid"
   "Datom" "EntityId" "GcOptions" "IndexLookupArgs" "IndexRangeArgs"
   "Keyword" "Metrics" "PullOptions" "QueryArgs" "Schema" "Transaction"
   "TransactionReport" "UuidValue" "VersionRef" "WithArgs"])

(defn- jsdoc [doc]
  (when doc
    (str "/**\n"
         (str/join "\n" (map #(str " * " %) (str/split-lines (str/replace doc "*/" "*\\/"))))
         "\n */")))

(defn- function-source [fn-name]
  (let [{:keys [doc referentially-transparent?] :as spec-data}
        (get api-specification fn-name)
        {:keys [signatures]} (typescript/generate-function-signatures [fn-name spec-data])
        ts-name (clj-name->js-name fn-name)]
    (str (jsdoc doc) "\n"
         (str/join "\n" signatures) "\n"
         "export function " ts-name "(...args: unknown[]): Promise<any> {\n"
         "  return request(\"" (->url fn-name) "\", "
         (if referentially-transparent? "true" "false") ", args, "
         (if (= fn-name 'create-database) "true" "false") ");\n"
         "}")))

(defn generate-api-source []
  (let [exports (remote-js-exports api-specification)]
    (assert-unique-js-names! exports)
    (str "// Generated from datahike.api.specification. DO NOT EDIT.\n"
         "import { request } from \"./core.js\";\n"
         "import type { " (str/join ", " imported-types) " } from \"./core.js\";\n\n"
         (str/join "\n\n" (map function-source exports))
         "\n")))

(defn write-api! [output-path]
  (io/make-parents output-path)
  (spit output-path (generate-api-source))
  (println "TypeScript thin-client API written to:" output-path))

(defn -main [& [output-path]]
  (write-api! (or output-path "ts-client/src/api.generated.ts")))
