(ns datahike.test.config
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
    [datahike.config :refer [uri->config]]))

(deftest config-test
  (let [mem-uri "datahike:mem://config-test"
        file-uri "datahike:file:///tmp/config-test"
        level-uri "datahike:level:///tmp/config-test"
        pg-uri "datahike:pg://alice:foo@localhost:5432/config-test"]

    (are [x y] (= x (uri->config y))
      {:backend :mem :host "config-test" :uri mem-uri}
      mem-uri

      {:backend :file :path "/tmp/config-test" :uri file-uri}
      file-uri

      {:backend :level :path "/tmp/config-test" :uri level-uri}
      level-uri

      {:backend :pg
        :host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"
       :uri pg-uri}
      pg-uri)))
