(ns datahike.test.middleware.utils-test
  (:require [clojure.test :refer [deftest is]]
            #?(:cljs [datahike.cljs :refer [Throwable]])
            [datahike.middleware.utils :as utils]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn test-handler-inc-first [handler] (fn [a b] (handler (+ a 5) b)))
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn test-handler-inc-second [handler] (fn [a b] (handler a (+ b 10))))

(deftest apply-middlewares-should-combine-symbols-to-new-function
  (let [combined (utils/apply-middlewares ['datahike.test.middleware.utils-test/test-handler-inc-first
                                           'datahike.test.middleware.utils-test/test-handler-inc-second]
                                          (fn [a b] (+ a b)))]
    (is (= 15
           (combined 0 0)))))

(deftest apply-middlewares-should-throw-exception-on-non-resolved-symbols
  (is (thrown-with-msg? Throwable #"Invalid middleware.😱"
                        (utils/apply-middlewares ['is-not-there
                                                  'datahike.middleware.utils-test/test-handler-inc-second]
                                                 (fn [a b] (+ a b))))))
