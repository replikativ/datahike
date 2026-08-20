(ns datahike.test.datom-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.datom :as d :refer [datom]]))

(deftest datom-impl
  (let [d (datom 123 :foo/bar "foobar")]
    (is (= [:e 123]
           (find d :e)))
    (is (= {:e 123
            :a :foo/bar
            :v "foobar"}
           (select-keys d [:e :a :v])))
    (is (= 123 (d :e)))))

(deftest prefix-comparators
  (testing "Prefix comparators match on e,a,v ignoring tx"
    (let [d1 (datom 1 :name "Alice" 100)
          d2 (datom 1 :name "Alice" 200)  ; same e,a,v but different tx
          d3 (datom 1 :name "Bob" 100)]    ; different v

      (testing "eavt prefix"
        (is (= 0 (d/cmp-datoms-eavt-prefix d1 d2))
            "Should match datoms with same e,a,v but different tx")
        (is (not= 0 (d/cmp-datoms-eavt-prefix d1 d3))
            "Should not match datoms with different v"))

      (testing "aevt prefix"
        (is (= 0 (d/cmp-datoms-aevt-prefix d1 d2))
            "Should match datoms with same a,e,v but different tx")
        (is (not= 0 (d/cmp-datoms-aevt-prefix d1 d3))
            "Should not match datoms with different v"))

      (testing "avet prefix"
        (is (= 0 (d/cmp-datoms-avet-prefix d1 d2))
            "Should match datoms with same a,v,e but different tx")
        (is (not= 0 (d/cmp-datoms-avet-prefix d1 d3))
            "Should not match datoms with different v"))))

  (testing "index-type->cmp-prefix returns correct comparator"
    (is (= d/cmp-datoms-eavt-prefix (d/index-type->cmp-prefix :eavt)))
    (is (= d/cmp-datoms-aevt-prefix (d/index-type->cmp-prefix :aevt)))
    (is (= d/cmp-datoms-avet-prefix (d/index-type->cmp-prefix :avet)))))

;; ---------------------------------------------------------------------------
;; A datom's equality has to agree with the INDEX's. `compare-value` already
;; calls two equal-content arrays one value, so if `=`/`hash` disagree then
;; `distinct`, a set of datoms, and a database's `:hash` — which is a SUM of
;; datom hashes — all disagree with the tree the datoms came out of.

(defn- bytes-of [xs]
  #?(:clj (byte-array xs) :cljs (js/Int8Array. (clj->js xs))))

(deftest datom-equality-follows-the-index
  (let [b1 (bytes-of [1 2 3])
        b2 (bytes-of [1 2 3])          ; equal content, distinct object
        b3 (bytes-of [9])
        d1 (datom 1 :ba b1 100)
        d2 (datom 1 :ba b2 100)
        d3 (datom 1 :ba b3 100)]
    (testing "the index calls them one value"
      (is (zero? (d/compare-value b1 b2))))
    (testing "so the datoms are equal, and hash equal"
      (is (= d1 d2))
      (is (= (hash d1) (hash d2))))
    (testing "which is what makes set and distinct agree with the tree"
      (is (= 1 (count (set [d1 d2]))))
      (is (= 1 (count (distinct [d1 d2])))))
    (testing "and a genuinely different value stays different"
      (is (not= d1 d3))
      (is (= 2 (count (set [d1 d3])))))
    (testing "scalar datoms hash exactly as before — no stored :hash moves"
      (is (= (hash (datom 1 :s "x" 100)) (hash (datom 1 :s "x" 100))))
      (is (= (hash (datom 1 :n 42 100)) (hash (datom 1 :n 42 100))))
      (is (not= (hash (datom 1 :s "x" 100)) (hash (datom 1 :s "y" 100)))))))
