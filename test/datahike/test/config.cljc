(ns datahike.test.config
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
    [datahike.config :refer [uri->config]]))

(deftest config-test
  (are [x y] (= x (uri->config y))
    {:storage-type :mem :storage-config {:host "config-test"}}
    "datahike:mem://config-test"

    {:storage-type :mem :storage-config {:host "config-test"} :temporal-index false}
    "datahike:mem://config-test?temporal-index=false"

    {:storage-type :mem :storage-config {:host "config-test"} :schema-on-read true}
    "datahike:mem://config-test?schema-on-read=true"

    {:storage-type :mem :storage-config {:host "config-test"} :schema-on-read true :temporal-index false}
    "datahike:mem://config-test?schema-on-read=true&temporal-index=false"

    {:storage-type :file :storage-config {:path "/tmp/config-test"}}
    "datahike:file:///tmp/config-test"

    {:storage-type :file :storage-config {:path "/tmp/config-test"} :temporal-index false}
    "datahike:file:///tmp/config-test?temporal-index=false"

    {:storage-type :file :storage-config {:path "/tmp/config-test"} :temporal-index false :schema-on-read true}
    "datahike:file:///tmp/config-test?temporal-index=false&schema-on-read=true"

    {:storage-type :level :storage-config {:path "/tmp/config-test"}}
    "datahike:level:///tmp/config-test"

    {:storage-type :pg :storage-config {:host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"}}
    "datahike:pg://alice:foo@localhost:5432/config-test"

    {:storage-type :pg :storage-config {:host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"} :schema-on-read true}
    "datahike:pg://alice:foo@localhost:5432/config-test?schema-on-read=true"

    {:storage-type :pg :storage-config {:host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"} :schema-on-read true :temporal-index false}
    "datahike:pg://alice:foo@localhost:5432/config-test?schema-on-read=true&temporal-index=false"
    ))
