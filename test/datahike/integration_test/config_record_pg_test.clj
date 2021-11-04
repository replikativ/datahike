(ns datahike.integration-test.config-record-pg-test
  (:require [clojure.test :refer :all]
            [datahike-jdbc.core]
            [datahike.integration-test :as it]))

(def config {:store {:backend :jdbc
                     :dbtype "postgresql"
                     :host "localhost"
                     :port 5432
                     :user "alice"
                     :password "foo"
                     :dbname "config-test"}})

(defn config-record-pg-test-fixture [f]
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once config-record-pg-test-fixture)

(deftest ^:integration config-record-pg-test []
  (it/integration-test config))
