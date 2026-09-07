(ns datahike.test.backfill-journal-test
  (:require [clojure.test :refer [deftest is]]
            [boring.core :as boring]
            [datahike.backfill.journal :as journal]
            [datahike.datom :as dd]
            [datahike.array :as arr])
  (:import [java.io RandomAccessFile]
           [java.nio.file Files Path]))

(defn- error-type [f]
  (try (f) nil (catch Exception e (:type (ex-data e)))))

(deftest exact-prefix-roundtrip-and-cleanup
  (let [a (journal/create! {})]
    (try
      (let [notification {:datom (dd/datom 100 :value (byte-array [1 2 3]) 42)
                          :added? true :value-hash (random-uuid)
                          :tx-meta {:db/txInstant (java.util.Date. 1234)
                                    :tuple [(float-array [1 2]) :tag]}}
            b (journal/append! a [notification])
            c (journal/append! b [{:second true}])
            first-value (first (journal/reduce-journal b conj []))]
        (is (= [] (journal/reduce-journal a conj [])))
        (is (= 1 (journal/reduce-journal b (fn [n _] (inc n)) 0)))
        (is (= 2 (journal/reduce-journal c (fn [n _] (inc n)) 0)))
        (is (= 100 (:e (:datom first-value))))
        (is (arr/a= (:v (:datom notification)) (:v (:datom first-value))))
        (is (= (:value-hash notification) (:value-hash first-value)))
        (is (= (get-in notification [:tx-meta :db/txInstant])
               (get-in first-value [:tx-meta :db/txInstant])))
        (is (arr/a= (get-in notification [:tx-meta :tuple 0])
                    (get-in first-value [:tx-meta :tuple 0])))
        (is (= :stopped (journal/reduce-journal c (fn [_ _] (reduced :stopped)) nil)))
        (is (thrown? Exception (journal/reduce-journal c (fn [_ _] (throw (Exception. "stop"))) nil))))
      (finally (journal/dispose! a)))
    (is (nil? (journal/dispose! a)))
    (is (not (Files/exists (Path/of (:path a) (make-array String 0)) (make-array java.nio.file.LinkOption 0))))))

(deftest byte-limits-and-rollback
  (let [a (journal/create! {:max-frame-bytes 64 :max-bytes 80})]
    (try
      (let [b (journal/append! a [:kept])]
        (is (= :backfill.journal/frame-too-large
               (error-type #(journal/append! b [(apply str (repeat 1000 "x"))]))))
        (is (= :backfill.journal/frame-too-large
               (error-type #(journal/append! b [(range 1000)]))))
        (is (= :backfill.journal/quota-exceeded
               (error-type #(journal/append! b (repeat 100 :small)))))
        (with-open [file (RandomAccessFile. (:path b) "r")]
          (is (= (:end b) (.length file))))
        (is (= [:kept] (journal/reduce-journal b conj [])))
        (let [c (journal/append! b [:next])]
          (is (= [:kept :next] (journal/reduce-journal c conj []))))
        ;; A caller retrying its accepted cursor discards an invisible tail.
        (journal/append! b [:discarded])
        (let [c (journal/append! b [:replacement])]
          (is (= [:kept :replacement] (journal/reduce-journal c conj [])))))
      (finally (journal/dispose! a)))))

(deftest codec-failure-rolls-back-complete-frames-too
  (let [a (journal/create! {})]
    (try
      (let [b (journal/append! a [:before])]
        (is (thrown? Exception (journal/append! b [:intermediate (Object.)])))
        (with-open [file (RandomAccessFile. (:path b) "r")]
          (is (= (:end b) (.length file))))
        (is (= [:before :after]
               (journal/reduce-journal (journal/append! b [:after]) conj []))))
      (finally (journal/dispose! a)))))

(deftest invalid-limits-and-descriptor
  (doseq [options [true [] {:unknown 1} {:directory 3} {:directory ""}
                   {:max-bytes nil} {:max-bytes 0} {:max-frame-bytes -1} {:max-bytes 1.5}]]
    (is (= :backfill.journal/invalid-options (error-type #(journal/create! options)))))
  (let [a (journal/create! {})]
    (try
      (is (= :backfill.journal/invalid-descriptor
             (error-type #(journal/append! (assoc a :max-bytes 999) [:bad]))))
      (is (= :backfill.journal/invalid-descriptor
             (error-type #(journal/dispose! (assoc a :path "/tmp/unowned")))))
      (finally (journal/dispose! a)))))

(deftest ratio-components-are-bounded-before-encoding
  (let [a (journal/create! {:max-frame-bytes 64})
        large (.shiftLeft java.math.BigInteger/ONE 4096)
        encoded? (atom false)]
    (try
      (doseq [value [(/ large 3) (/ 3 large)]]
        (with-redefs [boring/write-to! (fn [& _] (reset! encoded? true))]
          (is (= :backfill.journal/frame-too-large
                 (error-type #(journal/append! a [{:value value}]))))))
      (is (false? @encoded?))
      (is (= [] (journal/reduce-journal a conj [])))
      (finally (journal/dispose! a)))))

(deftest streaming-cap-and-truncated-prefix
  (let [a (journal/create! {:max-frame-bytes 64})]
    (try
      (is (= :backfill.journal/frame-too-large
             (with-redefs [boring/write-to! (fn [_ _ out]
                                              (dotimes [_ 65] (.write ^java.io.OutputStream out (int 1))))]
               (error-type #(journal/append! a [:value])))))
      (is (= [] (journal/reduce-journal a conj [])))
      (let [b (journal/append! a [:kept])]
        (with-open [file (RandomAccessFile. (:path a) "rw")]
          (.setLength file (dec (:end b))))
        (is (= :backfill.journal/corrupt
               (error-type #(journal/reduce-journal b conj [])))))
      (finally (journal/dispose! a)))))
