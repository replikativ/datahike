(ns datahike.integration-test.config-record-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

(def config {})

(defn config-record-test-fixture [f]
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once config-record-test-fixture)

(deftest ^:integration config-record-test []
  (it/integration-test config))
