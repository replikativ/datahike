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
                     impl-fn (symbol (namespace impl) (name impl))
                     args-sym (gensym "args")
                     clj-args-sym (gensym "clj-args")
                     result-sym (gensym "result")]]
           `(defn ~(with-meta js-fn-name {:export true :doc doc})
              [& ~args-sym]
              (let [~clj-args-sym (datahike.js.api/js->clj-api-args
                                   ~(name clj-fn-name) ~args-sym)
                      ;; JavaScript already exposes every operation as a Promise.
                      ;; Use that boundary to acquire a globally fresh snapshot
                      ;; from async browser stores while leaving the CLJ/CLJS `db`
                      ;; and synchronous query APIs unchanged.
                    ~result-sym ~(if (= clj-fn-name 'db)
                                   `(apply datahike.connector/db-async ~clj-args-sym)
                                   `(apply ~impl-fn ~clj-args-sym))]
                (-> ~result-sym
                    datahike.js.api/maybe-chan->promise
                    (.then datahike.js.api/clj->js-recursive))))))))
