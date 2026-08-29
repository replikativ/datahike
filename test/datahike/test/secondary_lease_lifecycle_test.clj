(ns datahike.test.secondary-lease-lifecycle-test
  (:require
   [clojure.core.async :as async]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [datahike.datom :as datom]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.scriptum]))

(def ^:private proximum-available?
  (try
    (require 'datahike.index.secondary.proximum)
    true
    (catch Throwable _ false)))

(deftest scriptum-preparation-retains-its-snapshot
  (testing "an old DB index can close without invalidating the prepared DB"
    (let [base (sec/create-index
                :scriptum
                {:attrs #{:doc/body}
                 :path (str "/tmp/datahike-scriptum-lease-" (random-uuid))}
                nil)
          source* (atom nil)
          prepared* (atom nil)
          preparation* (atom nil)]
      (try
        (let [source (sec/-transact
                      base
                      {:datom (datom/datom 1 :doc/body "overlapping snapshots")
                       :added? true})
              _ (reset! source* source)
              preparation (async/<!! (sec/-sec-prepare source {}))
              _ (reset! preparation* preparation)
              prepared (sec/-sec-generation-index preparation)
              _ (reset! prepared* prepared)]
          (.close ^java.io.Closeable source)
          (is (= #{1}
                 (set (es/entity-bitset-seq
                       (sec/-search prepared
                                    {:query "overlapping" :field :value}
                                    nil))))
              "closing db-before releases only its logical snapshot lease")
          (is (true? (async/<!!
                      (sec/-sec-release preparation {:status :committed}))))
          (is (= #{1}
                 (set (es/entity-bitset-seq
                       (sec/-search prepared
                                    {:query "snapshots" :field :value}
                                    nil))))
              "publication cleanup does not close db-after's reader"))
        (finally
          (when-let [preparation @preparation*]
            (async/<!! (sec/-sec-release preparation {:status :aborted})))
          (when-let [prepared @prepared*]
            (.close ^java.io.Closeable prepared))
          (when-let [source @source*]
            (.close ^java.io.Closeable source))
          (.close ^java.io.Closeable base))))))

(deftest proximum-preparation-retains-its-generation-view
  (if-not proximum-available?
    (is (not proximum-available?) "SKIP: Proximum requires Java 22+")
    (testing "the last adapter lease, not db-before, owns the mmap lifetime"
      (let [base (sec/create-index
                  :proximum
                  {:attrs #{:doc/embedding}
                   :dim 2
                   :distance :cosine
                   :store-config {:backend :memory :id (random-uuid)}}
                  nil)
            source* (atom nil)
            prepared* (atom nil)
            preparation* (atom nil)]
        (try
          (let [source (sec/-transact
                        base
                        {:datom (datom/datom
                                 1 :doc/embedding (float-array [1.0 0.0]))
                         :added? true})
                _ (reset! source* source)
                preparation (async/<!! (sec/-sec-prepare source {}))
                _ (reset! preparation* preparation)
                prepared (sec/-sec-generation-index preparation)
                _ (reset! prepared* prepared)
                mmap-path (-> source :generation :mmap-path)]
            (is (true? (async/<!!
                        (sec/-sec-release preparation {:status :committed}))))
            (.close ^java.io.Closeable source)
            (is (.exists (io/file mmap-path))
                "db-after's retained lease keeps the shared mmap alive")
            (is (= #{1}
                   (set (es/entity-bitset-seq
                         (sec/-search prepared
                                      {:vector (float-array [1.0 0.0]) :k 1}
                                      nil))))
                "closing db-before cannot unmap db-after's native query state")
            (.close ^java.io.Closeable prepared)
            (is (not (.exists (io/file mmap-path)))
                "the last adapter lease closes the handle and removes its mmap"))
          (finally
            (when-let [preparation @preparation*]
              (async/<!! (sec/-sec-release preparation {:status :aborted})))
            (when-let [prepared @prepared*]
              (.close ^java.io.Closeable prepared))
            (when-let [source @source*]
              (.close ^java.io.Closeable source))
            (.close ^java.io.Closeable base)))))))
