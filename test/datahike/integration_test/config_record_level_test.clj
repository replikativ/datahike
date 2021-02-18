(ns datahike.integration-test.config-record-level-test
  (:require [clojure.test :refer :all]
            [datahike-leveldb.core :as dl]
            [datahike.integration-test :as it]))

(def config {:store {:backend :level :path "/tmp/level-test"}})

(defn config-record-level-fixture [f]
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once config-record-level-fixture)

(deftest ^:integration config-record-level-test
  (it/integration-test config))
