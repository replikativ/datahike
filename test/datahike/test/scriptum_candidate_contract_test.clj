(ns datahike.test.scriptum-candidate-contract-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.datom :as datom]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.scriptum]
   [datahike.migrate.fs :as fs]))

(defn- add-docs!
  [index docs]
  (doseq [[eid value] docs]
    (sec/-transact! index
                    {:datom (datom/datom eid :doc/body value)
                     :added? true})))

(defn- error-data
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo failure
      (ex-data failure))))

(deftest transient-scriptum-filters-before-top-n
  (testing "the mutable generation never drops a late allowed entity after LIMIT"
    (let [index (sec/create-index :scriptum
                                  {:attrs #{:doc/body}
                                   :path (fs/temp-dir! "scriptum-transient-filter-")}
                                  nil)
          transient-index (sec/-as-transient index)
          entity-filter (es/entity-bitset-from-longs [3])]
      (try
        (add-docs! transient-index
                   [[1 "common token"]
                    [2 "common token"]
                    [3 "common token"]])
        (is (= [3]
               (vec
                (es/entity-bitset-seq
                 (sec/-search transient-index
                              {:query "common" :field :value :limit 1}
                              entity-filter)))))
        (is (= [3]
               (mapv :entity-id
                     (sec/-slice-ordered transient-index
                                         {:query "common" :field :value}
                                         entity-filter nil :desc 1))))
        (finally
          (sec/-abort-transient! transient-index)
          (.close ^java.io.Closeable index))))))

(deftest persistent-scriptum-cursors-bind-query-and-entity-filter
  (let [index (sec/create-index :scriptum
                                {:attrs #{:doc/body}
                                 :path (fs/temp-dir! "scriptum-cursor-identity-")}
                                nil)
        transient-index (sec/-as-transient index)]
    (try
      (add-docs! transient-index
                 [[1 "common one"]
                  [2 "common two"]
                  [3 "common three"]])
      (let [persistent-index (sec/-persistent! transient-index)]
        (try
          (testing "the actual query is checked even when a caller label is reused"
            (let [page (sec/-candidate-page
                        persistent-index
                        {:query "common" :field :value :query-id :stable}
                        nil {:limit 1})]
              (is (= :scriptum/continuation-mismatch
                     (:type
                      (error-data
                       #(sec/-candidate-page
                         persistent-index
                         {:query "one" :field :value :query-id :stable}
                         nil
                         {:limit 1 :continuation (:continuation page)})))))))
          (testing "an entity set is part of cursor identity before collection"
            (let [filter-a (es/entity-bitset-from-longs [1 2])
                  page (sec/-candidate-page
                        persistent-index
                        {:query "common" :field :value :query-id :stable}
                        filter-a {:limit 1})
                  resumed (sec/-candidate-page
                           persistent-index
                           {:query "common" :field :value :query-id :stable}
                           (es/entity-bitset-from-longs [1 2])
                           {:limit 1 :continuation (:continuation page)})]
              (is (= [2] (mapv :entity-id (:candidates resumed)))
                  "an equivalent reconstructed filter resumes")
              (is (= :scriptum/continuation-mismatch
                     (:type
                      (error-data
                       #(sec/-candidate-page
                         persistent-index
                         {:query "common" :field :value :query-id :stable}
                         (es/entity-bitset-from-longs [2 3])
                         {:limit 1 :continuation (:continuation page)})))))))
          (finally
            (.close ^java.io.Closeable persistent-index))))
      (finally
        (sec/-abort-transient! transient-index)
        (.close ^java.io.Closeable index)))))
