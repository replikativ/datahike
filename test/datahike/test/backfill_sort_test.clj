(ns datahike.test.backfill-sort-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.backfill.sort :as sort]
            [datahike.migrate.cbor :as cbor]
            [datahike.migrate.sort :as msort]
            [boring.core :as boring])
  (:import [java.io DataInputStream ByteArrayInputStream]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- with-directory [f]
  (let [directory (Files/createTempDirectory "backfill-sort-test-" (make-array FileAttribute 0))]
    (try
      (f directory)
      (with-open [entries (Files/newDirectoryStream directory)]
        (is (empty? (iterator-seq (.iterator entries))) "owned scratch removed"))
      (finally (Files/delete directory)))))

(defn- error-type [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(deftest bounded-sort-matches-reference
  (with-directory
    (fn [directory]
      (doseq [records [[] [[1 :value 1 10 true]]
                       (vec (for [i (range 200)] [(- 200 i) :value (mod i 7) 10 true]))]
              window [1 7 1000]]
        (is (= (clojure.core/sort msort/by-sort-key records)
               (sort/reduce-sorted! records msort/by-sort-key
                                    {:directory directory :window-records window :fan-in 3}
                                    conj [])))))))

(deftest stable-comparator-ties-survive-many-passes
  (let [records (mapv #(vector % :value % 10 true) (range 100))]
    (is (= records (sort/reduce-sorted! records (constantly 0)
                                        {:window-records 2 :fan-in 3} conj [])))))

(deftest byte-window-and-value-roundtrip
  (with-directory
    (fn [directory]
      (let [records [[3 :value (byte-array [1 2 3]) 10 true]
                     [2 :value (float-array [1.0 2.0]) 10 false]
                     [1 :value [(double-array [3.0]) :tuple] 10 true]]
            result (sort/reduce-sorted! records #(compare (first %1) (first %2))
                                        {:directory directory :window-bytes 512 :fan-in 2} conj [])]
        (is (= [1 2 3] (mapv first result)))
        (is (= [1 2 3] (vec (nth (nth result 2) 2))))
        (is (= [1.0 2.0] (vec (nth (nth result 1) 2))))
        (is (= [3.0] (vec (first (nth (first result) 2)))))))))

(deftest resource-scope-covers-early-exit-and-exceptions
  (doseq [consumer [(fn [rs] (first rs))
                    (fn [rs] (reduce (fn [_ r] (reduced r)) nil rs))
                    (fn [rs] (first rs) (throw (ex-info "consumer" {:type ::consumer})))]]
    (with-directory
      (fn [directory]
        (try
          (sort/with-sorted-records! (map #(vector % :value % 10 true) (range 100))
            msort/by-sort-key
            {:directory directory :window-records 1 :fan-in 3} consumer)
          (catch clojure.lang.ExceptionInfo e (is (= ::consumer (:type (ex-data e)))))))))
  (with-directory
    (fn [directory]
      (is (= ::comparison
             (error-type #(sort/reduce-sorted! [[2 :v 1 10 true] [1 :v 1 10 true]]
                                               (fn [_ _] (throw (ex-info "comparison" {:type ::comparison})))
                                               {:directory directory :window-records 1} conj [])))))))

(deftest invalid-limits-and-quota-fail-cleanly
  (doseq [opts [{:fan-in 1} {:fan-in 129} {:window-bytes 0}
                {:max-record-bytes -1} {:max-bytes 1.2} {:unknown true}]]
    (is (= :datahike.backfill.sort/invalid-options
           (error-type #(sort/validate-options! opts)))))
  (with-directory
    (fn [directory]
      (is (= :datahike.backfill.sort/quota-exceeded
             (error-type #(sort/reduce-sorted! [[1 :v "value" 10 true]] msort/by-sort-key
                                               {:directory directory :max-bytes 1} conj []))))))
  (with-directory
    (fn [directory]
      (let [encoded? (atom false)]
        (with-redefs [boring/write-to! (fn [& _] (reset! encoded? true))]
          (is (= :datahike.backfill.sort/record-too-large
                 (error-type #(sort/reduce-sorted! [[1 :v (apply str (repeat 1000 "x")) 10 true]]
                                                   msort/by-sort-key
                                                   {:directory directory :max-record-bytes 512} conj [])))))
        (is (false? @encoded?) "reject oversized scalar before codec allocation")))))

(deftest codec-output-is-capped-before-buffer-growth
  (with-directory
    (fn [directory]
      (with-redefs [boring/write-to! (fn [_ _ out]
                                       (dotimes [_ 10] (.write ^java.io.OutputStream out (byte-array 256))))]
        (is (= :datahike.backfill.sort/record-too-large
               (error-type #(sort/reduce-sorted! [[1 :v 1 10 true]] msort/by-sort-key
                                                 {:directory directory :max-record-bytes 512} conj []))))))))

(deftest fan-in-and-exception-close-every-reader
  (let [reader-var (ns-resolve 'datahike.backfill.sort 'open-reader)
        original @reader-var
        live (atom 0)
        peak (atom 0)
        opens (atom 0)
        tracked (fn [path]
                  (let [in (original path)]
                    (swap! opens inc)
                    (swap! peak max (swap! live inc))
                    (proxy [DataInputStream] [in]
                      (close [] (try (.close ^DataInputStream in)
                                     (finally (swap! live dec)))))))]
    (with-redefs-fn {reader-var tracked}
      (fn []
        (with-directory
          (fn [directory]
            (is (= 100 (sort/reduce-sorted! (map #(vector % :v % 10 true) (range 100))
                                            msort/by-sort-key
                                            {:directory directory :window-records 1 :fan-in 3}
                                            (fn [n _] (inc n)) 0)))))
        (is (zero? @live))
        (is (<= @peak 3))
        (is (> @opens 100))
        (with-directory
          (fn [directory]
            (is (= ::merge-error
                   (error-type #(sort/reduce-sorted! [[1 :v 1 10 true] [2 :v 2 10 true]]
                                                     (fn [_ _] (throw (ex-info "merge" {:type ::merge-error})))
                                                     {:directory directory :window-records 1 :fan-in 3}
                                                     conj []))))))
        (is (zero? @live))))))

(deftest byte-budget-causes-spills-and-input-failure-cleans-up
  (let [spill-var (ns-resolve 'datahike.backfill.sort 'spill!)
        original @spill-var
        spills (atom [])]
    (with-redefs-fn {spill-var (fn [directory run window cmp usage opts]
                                 (swap! spills conj (reduce + (map :charge window)))
                                 (original directory run window cmp usage opts))}
      (fn []
        (sort/reduce-sorted! (repeat 20 [1 :v 1 10 true]) msort/by-sort-key
                             {:window-bytes 512 :window-records 1000} (fn [n _] (inc n)) 0)
        (is (> (count @spills) 1) "byte limit spills before record limit")
        (is (every? #(<= % 512) @spills)))))
  (with-directory
    (fn [directory]
      (is (= ::input
             (error-type #(sort/reduce-sorted!
                           (concat [[1 :v 1 10 true] [2 :v 2 10 true]]
                                   (lazy-seq (throw (ex-info "input" {:type ::input}))))
                           msort/by-sort-key {:directory directory :window-records 1} conj [])))))))

(deftest merge-quota-and-frame-corruption
  (with-directory
    (fn [directory]
      (let [record [1 :v 1 10 true]
            frame-bytes (+ 4 (alength ^bytes (cbor/encode-record record)))]
        (is (= :datahike.backfill.sort/quota-exceeded
               (error-type #(sort/reduce-sorted! [record record] msort/by-sort-key
                                                 {:directory directory :window-records 1
                                                  :max-bytes (dec (* 3 frame-bytes))}
                                                 conj [])))))))
  (let [read-frame (ns-resolve 'datahike.backfill.sort 'read-frame!)]
    (doseq [bytes [[0 0 0] [0 0 0 100] [127 -1 -1 -1]]]
      (with-open [in (DataInputStream. (ByteArrayInputStream. (byte-array bytes)))]
        (is (= :datahike.backfill.sort/corrupt (error-type #(read-frame in 128))))))))

(deftest ratio-scalars-are-preflighted
  (let [large (.add (.shiftLeft java.math.BigInteger/ONE 10000) java.math.BigInteger/ONE)
        ratio (/ large 3)
        encoded? (atom false)]
    (is (ratio? ratio))
    (with-directory
      (fn [directory]
        (with-redefs [boring/write-to! (fn [& _] (reset! encoded? true))]
          (is (= :datahike.backfill.sort/record-too-large
                 (error-type #(sort/reduce-sorted! [[1 :v ratio 10 true]] msort/by-sort-key
                                                   {:directory directory :max-record-bytes 512}
                                                   conj [])))))
        (is (false? @encoded?))))))
