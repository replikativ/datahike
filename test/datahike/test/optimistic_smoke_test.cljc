(ns datahike.test.optimistic-smoke-test
  "Cross-platform smoke test for `datahike.optimistic`.

   Validates the core API on both CLJ and CLJS-Node:
     - register! / unregister!
     - listen! / unlisten!
     - transact! happy path (channel delivers tx-report)
     - transact! eager-validation rejection (synchronous throw)
     - effective-db reduces overlay over @conn
     - external @conn advances re-fire listeners with correct effective-db"
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [datahike.api :as d]
                     [datahike.optimistic :as opt]
                     [datahike.test.async :refer [deftest-async]]
                     [clojure.core.async :as a :refer [<! go]])
     :cljs (:require [cljs.test :refer [deftest is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.optimistic :as opt]
                     [datahike.test.async :refer-macros [deftest-async]]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(defn- mk-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? false})

(defn- setup []
  (go
    (let [cfg (mk-cfg)
          _    (<! #?(:clj  (go (d/create-database cfg))
                      :cljs (d/create-database cfg)))
          conn (<! #?(:clj  (go (d/connect cfg))
                      :cljs (d/connect cfg {:sync? false})))
          _    (<! (d/transact! conn
                                [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one
                                  :db/unique :db.unique/identity}]))
          _    (<! (d/transact! conn [{:name "alice"}]))]
      conn)))

(defn- names [db]
  (->> (d/datoms db :avet :name) (mapv :v) sort vec))

(deftest-async register-listen-transact
  (let [conn (<! (setup))
        events (atom [])]
    (opt/register! conn)
    (opt/listen! conn ::probe (fn [eff-db]
                                (swap! events conj (names eff-db))))
    (testing "happy path: optimistic transact delivers tx-report on result chan"
      (let [{:keys [ov-id result]} (opt/transact! conn [{:name "bob"}])
            reply (<! result)]
        (is (some? ov-id) "ov-id is set")
        (is (not (#?(:clj instance? :cljs instance?)
                  #?(:clj Throwable :cljs js/Error)
                  reply))
            "reply is not an exception")
        (is (some? (:db-after reply)) "reply is a tx-report")
        (is (= 0 (count (opt/pending conn))) "pending drained after success")
        (is (= ["alice" "bob"] (names @conn)) "base advanced")
        (is (every? #(contains? (set %) "bob") @events)
            "all listener events include the optimistic name")
        (is (>= (count @events) 1) "at least one listener event fired")))
    (opt/unlisten! conn ::probe)
    (opt/unregister! conn)))

(deftest-async eager-validation-throws
  (let [conn (<! (setup))]
    (opt/register! conn)
    (testing "schema-violating tx-data throws synchronously and never enters overlay"
      (let [caught (try
                     (opt/transact! conn [{:name 42}])
                     ::no-throw
                     (catch #?(:clj Throwable :cljs :default) e
                       e))]
        (is (not= ::no-throw caught) "transact! threw")
        (is (= 0 (count (opt/pending conn))) "overlay not polluted")))
    (opt/unregister! conn)))

(deftest-async external-advance-keeps-overlay
  (let [conn (<! (setup))
        events (atom [])]
    (opt/register! conn)
    (opt/listen! conn ::probe (fn [eff-db] (swap! events conj (names eff-db))))
    (testing "manually-stacked overlay entries survive an external @conn advance"
      ;; Manually inject an entry that bypasses transact!. Removal is
      ;; only ever by `:ov-id` from transact! itself, so the watcher
      ;; never drops it on its own.
      (let [{:keys [overlay]} (#'opt/conn-state conn)
            ov-id (random-uuid)]
        (swap! overlay conj
               {:ov-id   ov-id
                :tx-data [{:name "carol"}]
                :status  :pending})
        ;; External transact (simulates konserve-sync echo / another peer)
        (<! (d/transact! conn [{:name "dave"}]))
        (is (= 1 (count (opt/pending conn)))
            "manual entry survives external advance")
        (is (= ["alice" "dave"] (names @conn)) "base has dave only")
        (is (= ["alice" "carol" "dave"] (names (opt/effective-db conn)))
            "effective-db includes carol via overlay")))
    (opt/unregister! conn)))
