(ns datahike.test.scratch-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.test.scratch :as scratch]
            [datahike.test.utils :as utils]))

(deftest failing-test-removes-its-directory
  (let [directory (atom nil)]
    (is (thrown-with-msg?
         Exception #"intentional"
         (scratch/fixture
          #(do (reset! directory (scratch/path))
               (spit (scratch/path "leftover") "data")
               (throw (ex-info "intentional" {}))))))
    (is (not (.exists (io/file @directory))))))

(deftest with-db-cleans-up-when-body-throws
  (scratch/fixture
   #(let [cfg {:store {:backend :file :path (scratch/path "db") :id (random-uuid)}
               :schema-flexibility :read}]
      (is (thrown-with-msg? Exception #"intentional"
                            (utils/with-db cfg (fn [] (throw (ex-info "intentional" {}))))))
      (is (false? (d/database-exists? cfg))))))

(deftest with-db-cleans-up-when-setup-throws
  (scratch/fixture
   #(let [cfg {:store {:backend :file :path (scratch/path "db") :id (random-uuid)}
               :schema-flexibility :write}]
      (is (thrown? Exception (utils/with-db cfg [{:undefined/attribute "bad"}] (fn []))))
      (is (false? (d/database-exists? cfg))))))
