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

(defn combinations [len items]
  (if (= 0 len)
    [[]]
    (for [x (combinations (dec len) items)
          i items]
      (conj x i))))

(defn order [i]
  (cond
    (neg? i) '<
    (pos? i) '>
    :else '=))

(deftest combinatorial-comparator-test
  (doseq [idx [:eavt ;; Works
               :aevt ;; Works
               ;;:avet ;; Broken
               ]
          :let [cmp-quick (d/index-type->cmp-quick idx)
                cmp-replace (d/index-type->cmp-replace idx)]
          [e0 a0 v0 t0 e1 a1 v1 t1] (combinations 8 [0 1 2]) 
          :let [datom0 (datom e0 a0 v0 t0)
                datom1 (datom e1 a1 v1 t1)

                cmp-quick-01 (order (cmp-quick datom0 datom1))
                cmp-replace-01 (order (cmp-replace datom0 datom1))]]
    ;; Whenever `cmp-quick` indicates strict inequality, we expect
    ;; `cmp-replace` to indicate indicate either (i) the same inequality or (ii) equality.
    ;; If `cmp-quick` indicates equality, `cmp-replace` must indicate equality too.
    (is (contains? '#{[< <]
                      [< =]
                      [> >]
                      [> =]
                      [= =]}
                   [cmp-quick-01 cmp-replace-01]))

    ;; Swapping the arguments swaps the relation.
    (is (= cmp-quick-01 (order (- (cmp-quick datom1 datom0)))))
    (is (= cmp-replace-01 (order (- (cmp-replace datom1 datom0)))))))
