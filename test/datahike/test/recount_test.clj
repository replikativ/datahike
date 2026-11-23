(ns datahike.test.recount-test
  "A database created before every index branch was stored with its element count
   (#1108): the connect says so once, with the call that fixes it, and
   `datahike.index.recount/recount!` fixes it."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.index.recount :as recount]
            [datahike.migrate.fs :as fs]
            [datahike.tools :as dt]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set :as psset]
            [taoensso.trove :as trove])
  (:import [org.replikativ.persistent_sorted_set Branch PersistentSortedSet]))

(def ^:private index-keys
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn- cfg [extra]
  (merge {:store {:backend :file
                  :path (fs/temp-store-path! "dh-recount")
                  :id (random-uuid)}
          :schema-flexibility :read
          :keep-history? true
          ;; small nodes: a few thousand datoms give three levels, so there are
          ;; branches below the root to lose their counts
          :index-config {:branching-factor 16}}
         extra))

(defn- pss? [conn]
  (= :datahike.index/persistent-set (get-in @conn [:config :index])))

(defn- fill! [conn]
  (dotimes [i 8]
    (d/transact conn (vec (for [j (range 250)] {:k (+ (* i 250) j) :v (str "x" j)}))))
  (d/transact conn (vec (for [j (range 300)] [:db/retract (inc j) :v (str "x" (mod j 250))]))))

(defn- age!
  "Make the stored database look as one written by an earlier version does: every
   branch below an index root is rewritten without its count, and with `roots?` the
   roots too; the database loses its mark. Returns how many branches lost a count.
   The connection must be released afterwards -- its node cache still has them."
  [conn roots?]
  (let [db @conn
        store (:store db)
        branch (get-in db [:config :branch])
        n (atom 0)]
    (doseq [ik index-keys
            :let [^PersistentSortedSet p (get db ik)]
            :when p
            :let [root (.-_address p)
                  addrs (atom [])]]
      (psset/walk-addresses p (fn [a] (swap! addrs conj a) true))
      (doseq [a @addrs
              :when (or roots? (not= a root))
              :let [node (k/get store a nil {:sync? true})]
              :when (instance? Branch node)]
        (set! (.-_subtreeCount ^Branch node) -1)
        (k/assoc store a node {:sync? true})
        (swap! n inc)))
    (k/update-in store [branch]
                 (fn [stored]
                   (when roots?
                     (doseq [sk [:eavt-key :aevt-key :temporal-eavt-key :temporal-aevt-key]
                             :let [^PersistentSortedSet p (get stored sk)]
                             :when p]
                       (set! (.-_count p) (int -1))))
                   (update stored :meta #(-> %
                                             (dissoc :datahike/index-counts)
                                             (assoc :persistent.set/version "0.5.146"))))
                 {:sync? true})
    @n))

(defn- unknown [census]
  (reduce + (map :unknown (vals (:indices census)))))

(defmacro ^:private with-warnings
  "Run `body` with the :datahike/index-counts-incomplete warnings collected in the
   atom bound to `sym`, as [message data] pairs."
  [sym & body]
  `(let [~sym (atom [])
         previous# trove/*log-fn*]
     (trove/set-log-fn! (fn [ns# coords# level# id# lazy#]
                          (if (= :datahike/index-counts-incomplete id#)
                            (let [m# (force lazy#)]
                              (swap! ~sym conj [(:msg m#) (:data m#)]))
                            (when previous# (previous# ns# coords# level# id# lazy#)))))
     (try ~@body
          (finally (trove/set-log-fn! previous#)))))

(deftest a-new-database-is-complete-and-quiet
  (let [cfg (cfg {})]
    (d/create-database cfg)
    (with-warnings warnings
      (let [conn (d/connect cfg)]
        (fill! conn)
        (if (pss? conn)
          (let [c (recount/census conn)]
            (is (:complete? c))
            (is (pos? (reduce + (map :branches (vals (:indices c)))))
                "precondition: the indices have branches")
            (is (zero? (unknown c)) "every write kept every count")
            (testing "a recount of a complete database commits nothing"
              (let [commit-id (get-in @conn [:meta :datahike/commit-id])]
                (is (some? commit-id))
                (is (zero? (unknown (recount/recount! conn))))
                (is (= commit-id (get-in @conn [:meta :datahike/commit-id]))))))
          (testing "an index type without counts carries no mark and has nothing to recount"
            (is (not (:complete? (recount/census conn))))
            (is (thrown-with-msg? Exception #"Only persistent-set" (recount/recount! conn)))))
        (d/release conn))
      (let [conn (d/connect cfg)]
        (when (pss? conn)
          (is (zero? (unknown (recount/census conn))) "and stored them"))
        (d/release conn))
      (is (empty? @warnings)))
    (d/delete-database cfg)))

(deftest an-older-database-is-reported-once-and-recounted
  (let [cfg (cfg {})]
    (d/create-database cfg)
    (let [conn (d/connect cfg)
          pss (pss? conn)]
      (fill! conn)
      (let [erased (age! conn false)]
        (d/release conn)
        (when pss
          (is (pos? erased) "precondition: there were branches below the roots")
          (with-warnings warnings
            (let [conn (d/connect cfg)]
              (d/release conn))
            (let [conn (d/connect cfg)
                  before (recount/census conn)
                  max-tx (:max-tx @conn)
                  datoms (vec (d/datoms @conn :eavt))
                  q '[:find (count ?e) . :where [?e :k ?k] [(< ?k 700)] [?e :v "x7"]]
                  answer (d/q q @conn)]
              (testing "the connect says so, with the call that fixes it -- once, though this is the second connect"
                (is (= 1 (count @warnings)))
                (let [[msg data] (first @warnings)]
                  (is (re-find #"\(datahike\.index\.recount/recount! conn\)" (str msg)))
                  (is (= [] (:roots-without-count data)) "the roots have their counts here")
                  (is (= (get-in cfg [:store :id]) (:store-id data)))))
              (testing "the census finds what was erased"
                (is (not (:complete? before)))
                (is (= erased (unknown before))))
              (testing "recount! reports what it found and commits without a transaction"
                (is (= before (recount/recount! conn)))
                (is (= max-tx (:max-tx @conn)))
                (is (= dt/persistent-set-version (get-in @conn [:meta :persistent.set/version]))
                    "and raises the recorded version, so that an older runtime refuses the database")
                (is (= datoms (vec (d/datoms @conn :eavt))))
                (is (= answer (d/q q @conn))))
              (testing "what it will store has every count"
                (let [after (recount/census conn)]
                  (is (:complete? after))
                  (is (zero? (unknown after)))
                  (is (= (map :branches (vals (:indices before)))
                         (map :branches (vals (:indices after))))
                      "the trees keep their shape")))
              (d/release conn)))
          (testing "the counts and the mark are in the store"
            (with-warnings warnings
              (let [conn (d/connect cfg)
                    c (recount/census conn)]
                (is (:complete? c))
                (is (zero? (unknown c)))
                (testing "and writes keep them"
                  (d/transact conn (vec (for [j (range 400)] {:k (+ 5000 j) :v "y"})))
                  (d/transact conn (vec (for [j (range 200)] [:db/retract (+ 400 j) :v (str "x" (mod (+ 399 j) 250))])))
                  (is (zero? (unknown (recount/census conn)))))
                (d/release conn))
              (is (empty? @warnings) "a marked database is quiet"))))))
    (d/delete-database cfg)))

(deftest roots-without-a-count-are-named
  (let [cfg (cfg {})]
    (d/create-database cfg)
    (let [conn (d/connect cfg)
          pss (pss? conn)]
      (fill! conn)
      (age! conn true)
      (d/release conn)
      (when pss
        (with-warnings warnings
          (let [conn (d/connect cfg)]
            (is (= 1 (count @warnings)))
            (let [[msg data] (first @warnings)]
              (is (= [:eavt-key :aevt-key :temporal-eavt-key :temporal-aevt-key]
                     (:roots-without-count data)))
              (is (re-find #"heuristics" (str msg))))
            (is (pos? (unknown (recount/census conn))))
            (recount/recount! conn)
            (d/release conn)))
        (with-warnings warnings
          (let [conn (d/connect cfg)
                c (recount/census conn)]
            (is (:complete? c))
            (is (zero? (unknown c)) "the roots are counted with the rest")
            (is (= (count (d/datoms @conn :eavt)) (count (:eavt @conn))))
            (d/release conn))
          (is (empty? @warnings)))))
    (d/delete-database cfg)))

(deftest recount-leaves-nothing-for-online-gc-to-recycle
  ;; Rewriting a branch reports the replaced node as freed. Were the recount's
  ;; commit to lose a head race, the winning head would still name those nodes, so
  ;; the recount takes its hints back and leaves the nodes to offline GC.
  (let [cfg (cfg {:online-gc {:enabled? true :grace-period-ms 0}})]
    (d/create-database cfg)
    (let [conn (d/connect cfg)
          pss (pss? conn)]
      (fill! conn)
      (age! conn false)
      (d/release conn)
      (when pss
        (let [conn (d/connect cfg)
              storage (get-in @conn [:store :storage])
              addresses (fn []
                          (let [db @conn acc (atom #{})]
                            (doseq [ik index-keys :let [p (get db ik)] :when p]
                              (psset/walk-addresses p (fn [a] (swap! acc conj a) true)))
                            @acc))
              before (addresses)
              datoms (vec (d/datoms @conn :eavt))
              _ (is (pos? (unknown (recount/recount! conn))))
              replaced (reduce disj before (addresses))]
          (is (seq replaced) "precondition: the recount replaced nodes")
          ;; the recount's own commit ran online GC, with no grace period
          (is (empty? (filter replaced @(:freelist storage)))
              "none of them was handed to the freelist")
          (is (empty? (filter replaced @(:freed-set storage))) "and no hint is left behind")
          (testing "and transactions that run online GC afterwards recycle none of them"
            (dotimes [i 5]
              (d/transact conn (vec (for [j (range 100)] {:k (+ 9000 (* i 100) j) :v "z"}))))
            (d/release conn))
          (let [conn (d/connect cfg)]
            (is (every? (set (d/datoms @conn :eavt)) datoms) "every datom from before is there")
            (is (= (+ 2000 500) (d/q '[:find (count ?e) . :where [?e :k]] @conn)))
            (is (zero? (unknown (recount/census conn))))
            (d/release conn)))))
    (d/delete-database cfg)))

(deftest recount-refuses-what-it-cannot-do-safely
  (let [cfg (cfg {})]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/release conn)
      (is (thrown-with-msg? Exception #"released" (recount/recount! conn)))
      (is (thrown-with-msg? Exception #"released" (recount/census conn))))
    (d/delete-database cfg))
  (let [cfg (cfg {:crypto-hash? true})]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (when (pss? conn)
        (is (thrown-with-msg? Exception #"crypto-hash" (recount/recount! conn))
            "a node's address does not cover its count there"))
      (d/release conn))
    (d/delete-database cfg)))
