(ns datahike.test.codegen-js-names-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :as naming]
            [datahike.codegen.typescript :as ts]
            [datahike.codegen.ts-client :as ts-client]
            [datahike.codegen.esm :as esm]
            [datahike.codegen.cli :as cli]
            [datahike.js.api-macros]))

(deftest writer-barrier-has-one-async-javascript-export
  (let [exports (remove naming/js-skip-list (keys api-specification))
        remote (set (naming/remote-js-exports api-specification))
        expansion (macroexpand-1 '(datahike.js.api-macros/emit-js-api))
        symbols (set (filter symbol? (tree-seq coll? seq expansion)))
        signature (first (:signatures
                          (ts/generate-function-signatures
                           ['writer-barrier! (get api-specification 'writer-barrier!)])))]
    (is (= ['writer-barrier!]
           (filterv #(= "writerBarrier" (naming/clj-name->js-name %)) exports)))
    (is (nil? (naming/assert-unique-js-names! exports)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"JavaScript API name collision"
                          (naming/assert-unique-js-names! ['writer-barrier 'writer-barrier!])))
    (is (contains? symbols 'datahike.api.impl/writer-barrier!))
    (is (not (contains? symbols 'datahike.api.impl/writer-barrier)))
    (is (contains? symbols 'datahike.js.api/maybe-chan->promise))
    (is (re-find #"writerBarrier\(.*Connection.*\): Promise<Database>;" signature))
    (is (= 1 (count (re-seq #"export var writerBarrier =" (esm/generate-esm-wrapper)))))
    (is (contains? remote 'writer-barrier))
    (is (not (contains? remote 'writer-barrier!)))
    (let [source (ts-client/generate-api-source)]
      (is (= 1 (count (re-seq #"export function writerBarrier\(\.\.\.args:" source))))
      (is (re-find #"writerBarrier\(.*Connection.*\): Promise<Database>;" source))
      (is (re-find #"return request\(\"writer-barrier\", false, args, false\)" source)))
    (is (nil? (naming/assert-unique-js-names! remote)))))

(deftest writer-barrier-has-one-synchronous-cli-command
  (let [exports (remove cli/cli-excluded-operations (keys api-specification))]
    (is (= ['writer-barrier]
           (filterv #(= ["writer-barrier"] (cli/->cli-command % nil)) exports)))
    (is (= 'writer-barrier (get (cli/build-command-index) "writer-barrier")))))
