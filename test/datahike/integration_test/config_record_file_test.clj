(ns datahike.integration-test.config-record-file-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

(def config {:store {:backend :file :path "/tmp/file-test-1" :id #uuid "f11e0001-0000-0000-0000-000000000001"}})

(defn config-record-file-test-fixture [f]
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once config-record-file-test-fixture)

(deftest ^:integration config-record-file-test []
  (it/integration-test config))
