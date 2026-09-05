(ns datahike.test.codegen-js-names-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :as naming]
            [datahike.codegen.typescript :as ts]
            [datahike.codegen.esm :as esm]
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
    (is (= 1 (count (re-seq #"export var writerBarrier =" (esm/generate-remote-esm-wrapper)))))
    (is (some #{'datahike.http.client/writer-barrier}
              (tree-seq coll? seq
                        (macroexpand-1 '(datahike.js.api-macros/emit-js-remote-api)))))
    (is (nil? (naming/assert-unique-js-names! remote)))))
