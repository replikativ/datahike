(ns datahike.test.specification-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api.specification :refer [spec-args->argslist]]))

(deftest spec-to-argslist-translation
  (testing "Testing core cases of spec to argslist translator."
    (is (= (spec-args->argslist '(s/alt :config (s/cat :config spec/SConfig)
                                        :nil (s/cat)))
           '[[config] []]))

    (is (= (spec-args->argslist '(s/cat :conn spec/SConnection :txs spec/STransactions))
           '[[conn txs]]))

    (is (= (spec-args->argslist '(s/alt :argmap (s/cat :map spec/SQueryArgs)
                                        :with-params (s/cat :q (s/or :vec vector? :map map?) :args (s/* any?))))
           '[[map] [q & args]]))

    (is (= (spec-args->argslist '(s/alt :simple (s/cat :db spec/SDB :opts spec/SPullOptions)
                                        :full (s/cat :db spec/SDB :selector coll? :eid spec/SEId)))
           '[[db opts] [db selector eid]]))

    (is (= (spec-args->argslist '(s/alt :map (s/cat :db spec/SDB :args spec/SIndexLookupArgs)
                                        :key (s/cat :db spec/SDB :index keyword? :components (s/* any?))))
           '[[db args] [db index & components]]))))
