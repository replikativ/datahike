(ns datahike.test.index-opts-test
  "The IIndex opts arities (-slice/-rslice/-lookup …opts): the opts arity
   with {:sync? true} (or no :sync?) must behave exactly like the legacy
   arity; {:sync? false} returns a partial-cps async expression on cljs
   (resolving synchronously when everything is warm — the property the
   async engine's sync mode rests on) and throws on the JVM."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.datom :as dd]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.index.interface :as di]
   #?(:cljs [is.simm.partial-cps.sequence :as aseq])
   #?(:cljs [org.replikativ.persistent-sorted-set.btset :as btset])
   [datahike.test.core-test]))

(def test-db
  (delay
    (d/db-with (db/empty-db)
               (mapv (fn [i] {:db/id (inc i) :name (str "n" i) :score (mod i 7)})
                     (range 200)))))

(defn- full-range []
  [(dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax)])

#?(:cljs
   (defn- resolve-now!
     "Invoke a partial-cps async expr and assert it resolved synchronously
      (all-warm). Returns the resolved value."
     [expr]
     (let [result (atom ::pending)]
       (expr (fn [v] (reset! result v))
             (fn [e] (throw e)))
       (let [v @result]
         (is (not= ::pending v) "warm async expr must resolve before returning")
         v))))

#?(:cljs
   (defn- drain-chunks
     "Drain an AsyncSeq via achunk-next (warm, synchronous) into a flat vector."
     [aseq0]
     (loop [s aseq0 acc []]
       (if (nil? s)
         acc
         (let [chunk (resolve-now! (btset/achunk-next s))]
           (if (nil? chunk)
             acc
             (let [[keys start end next] chunk]
               (recur next
                      (loop [i start v acc]
                        (if (< i end) (recur (inc i) (conj v (aget keys i))) v))))))))))

(deftest opts-arity-equals-legacy-arity
  (let [eavt (:eavt @test-db)
        [from to] (full-range)]
    (testing "-slice: opts arity with :sync? true ≡ legacy arity"
      (is (= (vec (di/-slice eavt from to :eavt))
             (vec (di/-slice eavt from to :eavt {:sync? true}))
             (vec (di/-slice eavt from to :eavt {})))))
    (testing "-rslice: opts arity ≡ legacy arity"
      (is (= (vec (di/-rslice eavt to from :eavt))
             (vec (di/-rslice eavt to from :eavt {:sync? true})))))
    (testing "-lookup: opts arity ≡ legacy arity"
      (let [d (first (di/-slice eavt from to :eavt))
            cmp (dd/index-type->cmp-quick :eavt)]
        (is (= (di/-lookup eavt d cmp)
               (di/-lookup eavt d cmp {:sync? true})))))))

#?(:clj
   (deftest async-opts-throw-on-jvm
     (let [eavt (:eavt @test-db)
           [from to] (full-range)]
       (is (thrown-with-msg? Exception #"async index access"
                             (di/-slice eavt from to :eavt {:sync? false})))
       (is (thrown-with-msg? Exception #"async index access"
                             (di/-rslice eavt to from :eavt {:sync? false})))
       (is (thrown-with-msg? Exception #"async index access"
                             (di/-lookup eavt (first (di/-slice eavt from to :eavt))
                                         (dd/index-type->cmp-quick :eavt)
                                         {:sync? false}))))))

#?(:cljs
   (deftest async-slice-chunk-parity
     (testing "warm {:sync? false} slice resolves synchronously and chunk-drains
               to the same datoms as the sync slice"
       (let [eavt (:eavt @test-db)
             [from to] (full-range)
             sync-datoms (vec (di/-slice eavt from to :eavt))
             aslice (resolve-now! (di/-slice eavt from to :eavt {:sync? false}))
             async-datoms (drain-chunks aslice)]
         (is (pos? (count sync-datoms)))
         (is (= sync-datoms async-datoms))))))

#?(:cljs
   (deftest async-lookup-warm
     (let [eavt (:eavt @test-db)
           [from to] (full-range)
           d (first (di/-slice eavt from to :eavt))
           cmp (dd/index-type->cmp-quick :eavt)]
       (is (= (di/-lookup eavt d cmp)
              (resolve-now! (di/-lookup eavt d cmp {:sync? false})))))))
