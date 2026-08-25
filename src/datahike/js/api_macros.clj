(ns datahike.js.api-macros
  "Macros for generating JavaScript API."
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :refer [assert-unique-js-names!
                                             js-skip-list clj-name->js-name]]))

(defmacro emit-js-api
  "Generate JavaScript API functions from api-specification.
  This macro must be in a .clj file since it's used by ClojureScript."
  []
  (let [exports (for [[clj-fn-name _] api-specification
                      :when (not (contains? js-skip-list clj-fn-name))]
                  clj-fn-name)]
    (assert-unique-js-names! exports)
    `(do
       ~@(for [clj-fn-name exports
               :let [{:keys [doc impl]} (get api-specification clj-fn-name)]
               :let [js-fn-name (symbol (clj-name->js-name clj-fn-name))
                     impl-fn (symbol (namespace impl) (name impl))]]
           `(defn ~(with-meta js-fn-name {:export true :doc doc})
              [& args#]
              (let [clj-args# (map datahike.js.api/js->clj-recursive args#)
                    result# (apply ~impl-fn clj-args#)]
                (-> result#
                    datahike.js.api/maybe-chan->promise
                    (.then datahike.js.api/clj->js-recursive))))))))
