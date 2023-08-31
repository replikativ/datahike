(ns datahike.test.listen-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datahike.api :as d]
   [datahike.datom :as dd]
   [datahike.constants :as const]
   [datahike.test.utils :as du]
   [datahike.test.core-test]))

(deftest test-listen!
  (let [conn    (du/setup-db)
        reports (atom [])]
    (d/transact conn {:tx-data [[:db/add -1 :name "Alex"]
                                [:db/add -2 :name "Boris"]]})
    (d/listen conn :test #(swap! reports conj %))
    (d/transact conn {:tx-data [[:db/add -1 :name "Dima"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Evgeny"]]
                      :tx-meta {:some-metadata 1}})
    (d/transact conn {:tx-data [[:db/add -1 :name "Fedor"]
                                [:db/add 1 :name "Alex2"]         ;; should update
                                [:db/retract 2 :name "Not Boris"] ;; should be skipped
                                [:db/retract 4 :name "Evgeny"]]})
    (d/unlisten conn :test)
    (d/transact conn {:tx-data [[:db/add -1 :name "Georgy"]]})

    (is (= [(dd/datom (+ const/tx0 2) :some-metadata 1 (+ const/tx0 2) true)
            (dd/datom 3 :name "Dima"   (+ const/tx0 2) true)
            (dd/datom 3 :age 19        (+ const/tx0 2) true)
            (dd/datom 4 :name "Evgeny" (+ const/tx0 2) true)]
           (rest (:tx-data (first @reports)))))
    (is (= {:some-metadata 1}
           (dissoc (:tx-meta (first @reports)) :db/txInstant :db/commitId)))
    (is (= [(dd/datom 5 :name "Fedor"  (+ const/tx0 3) true)
            (dd/datom 1 :name "Alex2"  (+ const/tx0 3) true)
            (dd/datom 4 :name "Evgeny" (+ const/tx0 3) false)]
           (rest (:tx-data (second @reports)))))
    (is (= (dissoc (:tx-meta (second @reports)) :db/txInstant
                   :db/commitId)
           {}))
    (d/release conn)))
