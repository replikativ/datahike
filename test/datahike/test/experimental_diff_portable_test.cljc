(ns datahike.test.experimental-diff-portable-test
  "`datahike.experimental.diff` on BOTH runtimes, from one source.

   The JVM-only suite in `experimental-diff-test` carries the exhaustive oracle
   and the read-count measurement. This one exists to run the SAME contract
   through the cljs arm, where the index reads are awaited and the result comes
   back on a channel — the half that `async+sync` emits but that a JVM-only test
   can never execute."
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [datahike.api :as d]
                     [datahike.experimental.diff :as xd]
                     [datahike.test.async :refer [deftest-async]]
                     [clojure.core.async :as a :refer [<! go]])
     :cljs (:require [cljs.test :refer [deftest is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.experimental.diff :as xd]
                     [datahike.test.async :refer-macros [deftest-async]]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(defn- mk-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? true
   :index :datahike.index/persistent-set})

(defn- setup []
  (go
    (let [cfg  (mk-cfg)
          _    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
          conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          _    (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                       :db/cardinality :db.cardinality/one
                                       :db/unique :db.unique/identity}
                                      {:db/ident :age :db/valueType :db.type/long
                                       :db/cardinality :db.cardinality/one}]))]
      conn)))

(defn- tuple [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])

;; The sync arm on the JVM returns a value; the async arm on cljs returns a
;; channel. `<!` of a value wrapped in `go` is the value, so one call site
;; serves both — which is the property `async+sync` exists to give.
(defn- <diff [before after]
  #?(:clj  (go (xd/diff before after))
     :cljs (xd/diff before after {:sync? false})))

(defn- <tx-range [before after]
  #?(:clj  (go (xd/tx-range before after))
     :cljs (xd/tx-range before after {:sync? false})))

(deftest-async diff-finds-the-delta-on-both-runtimes
  (let [conn (<! (setup))]
    (<! (d/transact! conn [{:name "a" :age 1} {:name "b" :age 2}]))
    (let [before @conn]
      (<! (d/transact! conn [{:name "a" :age 99}]))
      (<! (d/transact! conn [{:name "c" :age 3}]))
      (let [after @conn
            {:keys [added removed]} (<! (<diff before after))
            av (fn [ds] (set (map (juxt :a :v) ds)))]
        (is (contains? (av added) [:age 99]) "the new card-one value")
        (is (contains? (av added) [:name "c"]) "the new entity")
        (is (contains? (av removed) [:age 1]) "the superseded value")
        (is (not (contains? (av added) [:age 1])))))))

(deftest-async tx-range-reproduces-tx-data-on-both-runtimes
  (let [conn (<! (setup))]
    (<! (d/transact! conn [{:name "a" :age 1} {:name "b" :age 2}]))
    (let [before   @conn
          r1       (<! (d/transact! conn [{:name "a" :age 99}]))
          r2       (<! (d/transact! conn [[:db/retractEntity [:name "b"]]]))
          r3       (<! (d/transact! conn [{:name "c" :age 3}
                                          [:db/retract [:name "c"] :age 3]]))
          after    @conn
          expected (into (sorted-map)
                         (for [r [r1 r2 r3]]
                           [(-> r :tx-data first :tx) (set (map tuple (:tx-data r)))]))
          actual   (into (sorted-map)
                         (for [{:keys [t data]} (<! (<tx-range before after))]
                           [t (set (map tuple data))]))]
      (is (= 3 (count actual)) "one entry per transaction in the window")
      (is (= expected actual)
          "reconstructed from the indexes, equal to what the transactor reported"))))

(deftest-async an-empty-window-is-empty-on-both-runtimes
  (let [conn (<! (setup))]
    (<! (d/transact! conn [{:name "a"}]))
    (let [db @conn]
      (is (= {:added [] :removed []} (<! (<diff db db))))
      (is (= [] (<! (<tx-range db db)))))))
