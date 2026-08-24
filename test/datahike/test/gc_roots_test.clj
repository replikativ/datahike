(ns datahike.test.gc-roots-test
  "Durable GC roots (`datahike.gc-roots`): a record persisted in the store that
   the mark walks like a branch head, with a lease so a dead holder cannot pin
   forever.

   Every retention assertion has its control — the same sweep with the root
   released, or with it expired — so a passing test cannot be one where nothing
   was collectable to begin with."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.gc :as gc]
            [datahike.gc-guard :refer [with-unreferenced-writes]]
            [datahike.gc-roots :as roots]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- cfg [tag & [extra]]
  (let [id (java.util.UUID/randomUUID)]
    (merge {:store {:backend :file
                    :path (str (System/getProperty "java.io.tmpdir") "/dh-gc-roots-" tag "-" id)
                    :id id}
            :writer {:backend :self :writer-ownership :exclusive}
            :schema-flexibility :write
            :keep-history? false}
           extra)))

(defn- fresh-conn [tag & [extra]]
  (let [c (cfg tag extra)]
    (d/create-database c)
    (let [conn (d/connect c)]
      (d/transact conn schema)
      [conn c])))

(defn- store-keys [conn]
  (set (map :key (k/keys (:store @conn) {:sync? true}))))

(defn- churn!
  "Enough commits that the snapshot taken before them has nodes nothing later
   shares — i.e. real garbage once `remove-before` passes it."
  [conn]
  (dotimes [i 8]
    (d/transact conn (mapv (fn [j] {:name (str "p" i "-" j) :n (+ (* 100 i) j)}) (range 40)))))

(defn- gc! [conn] (set (<?? S (d/gc-storage conn (Date.)))))

(defn- settled-keys
  "The store's keys once everything older than the head is collected: exactly
   what the head commit reaches (plus the registry, if any). Captured BEFORE the
   churn, this is the set a root of that head must keep alive — all of it, since
   nothing else is left to be garbage."
  [conn]
  (gc! conn)
  (store-keys conn))

;; ---------------------------------------------------------------------------

(deftest a-pin-keeps-its-commit-through-a-sweep-that-would-collect-it
  (let [[conn c] (fresh-conn "pin")]
    (try
      (d/transact conn [{:name "before" :n 1}])
      (let [snapshot-keys (settled-keys conn)
            db0 (d/db conn)
            id (<?? S (roots/pin! db0 {:note "test pin"}))]
        (churn! conn)
        (testing "with the root in place a full-range sweep spares every object of the pinned commit"
          (let [swept (gc! conn)]
            (is (empty? (set/intersection swept snapshot-keys))
                (str "swept pinned objects: " (pr-str (set/intersection swept snapshot-keys))))
            (is (contains? (store-keys conn) roots/registry-key)
                "the registry itself is whitelisted")
            (is (contains? (store-keys conn) (roots/record-key id))
                "and so is the root's record")))
        (testing "the registry entry describes the root"
          (let [entry (get (<?? S (roots/roots (:store @conn))) id)]
            (is (= :pin (:kind entry)))
            (is (= (get-in db0 [:meta :datahike/commit-id]) (:commit-id entry)))
            (is (inst? (:expires-at entry)))
            (is (= "test pin" (:note entry)))))
        (testing "releasing the root is the control: the same sweep now collects the commit"
          (<?? S (roots/release! (d/db conn) id))
          (let [swept (gc! conn)]
            (is (seq (set/intersection swept snapshot-keys))
                "the pinned commit's garbage was real — it goes as soon as the root does")
            (is (not (contains? (store-keys conn) (roots/record-key id)))
                "the released root's record is garbage too"))))
      (finally (d/release conn) (d/delete-database c)))))

(deftest an-expired-root-is-reaped-before-the-mark
  (let [[conn c] (fresh-conn "expire")]
    (try
      (d/transact conn [{:name "before" :n 1}])
      (let [snapshot-keys (settled-keys conn)
            ;; A one-millisecond lease: expired, and past its own TTL of grace,
            ;; a few milliseconds later.
            id (<?? S (roots/pin! (d/db conn) {:ttl-ms 1}))]
        (churn! conn)
        (Thread/sleep 20)
        (let [swept (gc! conn)]
          (is (nil? (get (<?? S (roots/roots (:store @conn))) id))
              "the collector reaped the expired entry")
          (is (seq (set/intersection swept snapshot-keys))
              "and did not mark from it: the commit it pinned was collected")
          (is (not (contains? (store-keys conn) (roots/record-key id)))
              "nor its record")))
      (finally (d/release conn) (d/delete-database c)))))

(deftest a-permanent-root-never-expires
  (let [[conn c] (fresh-conn "permanent")]
    (try
      (d/transact conn [{:name "before" :n 1}])
      (let [snapshot-keys (settled-keys conn)
            id (<?? S (roots/pin! (d/db conn) {:ttl-ms nil}))]
        (is (nil? (:expires-at (get (<?? S (roots/roots (:store @conn))) id))))
        (churn! conn)
        (is (empty? (set/intersection (gc! conn) snapshot-keys)))
        (<?? S (roots/release! (d/db conn) id)))
      (finally (d/release conn) (d/delete-database c)))))

(deftest a-lost-root-is-loud-at-renewal-and-at-publish-time
  (let [[conn c] (fresh-conn "lost")]
    (try
      (d/transact conn [{:name "x" :n 1}])
      (let [db (d/db conn)
            id (<?? S (roots/pin! db {}))]
        (testing "renewal extends the lease"
          (let [before (:expires-at (get (<?? S (roots/roots (:store @conn))) id))
                _ (Thread/sleep 5)
                after (:expires-at (<?? S (roots/renew! db id)))]
            (is (pos? (- (.getTime ^Date after) (.getTime ^Date before))))))
        (testing "the publish-time check passes while the root is fresh"
          (is (map? (<?? S (roots/assert-live! db id 60000)))))
        (testing "a stale renewal is refused before publishing"
          (Thread/sleep 10)
          (is (= :gc/root-stale
                 (:type (ex-data (try (<?? S (roots/assert-live! db id 1)) (catch Exception e e)))))))
        (<?? S (roots/release! db id))
        (testing "once the entry is gone — reaped, or eaten by an older collector — both checks raise"
          (is (= :gc/root-lost
                 (:type (ex-data (try (<?? S (roots/renew! db id)) (catch Exception e e))))))
          (is (= :gc/root-lost
                 (:type (ex-data (try (<?? S (roots/assert-live! db id nil)) (catch Exception e e))))))))
      (finally (d/release conn) (d/delete-database c)))))

(deftest the-renewal-loop-stops-and-reports-when-the-root-is-lost
  (let [[conn c] (fresh-conn "loop")]
    (try
      (d/transact conn [{:name "x" :n 1}])
      (let [db (d/db conn)
            id (<?? S (roots/pin! db {}))
            lost (promise)
            stop! (roots/start-renewal! db id {:interval-ms 5 :on-lost #(deliver lost %)})]
        (Thread/sleep 30)
        (is (not (realized? lost)) "renewing a live root reports nothing")
        (<?? S (roots/release! db id))
        (is (= :gc/root-lost (:type (ex-data (deref lost 2000 ::timeout))))
            "the loop notices the entry is gone and hands the holder the exception")
        (stop!))
      (finally (d/release conn) (d/delete-database c)))))

(deftest a-ref-keeps-ancestry-a-pin-keeps-one-commit
  ;; The geschichte case: a deleted branch's head, kept alive as a ref, must
  ;; keep its history; the same commit as a pin keeps only itself.
  (doseq [[kind ancestor-survives?] [[:ref true] [:pin false]]]
    (let [[conn c] (fresh-conn (name kind))]
      (try
        (d/transact conn [{:name "main" :n 0}])
        (d/branch! conn :db :side)
        (let [side (d/connect (assoc c :branch :side))]
          (d/transact side [{:name "side-1" :n 1}])
          (let [x1 (get-in (d/db side) [:meta :datahike/commit-id])]
            (d/transact side [{:name "side-2" :n 2}])
            (let [x2-db (d/db side)
                  x2 (get-in x2-db [:meta :datahike/commit-id])
                  record (<?? S (roots/commit-record x2-db))
                  id (<?? S (roots/root! x2-db {:kind kind :record record :ttl-ms nil}))]
              (d/release side)
              (d/delete-branch! conn :side)
              ;; Full-range: only the deleted branch's lineage is collectable.
              (<?? S (d/gc-storage conn))
              (let [ks (store-keys conn)]
                (is (contains? ks x2) (str kind " keeps the rooted commit"))
                (is (= ancestor-survives? (contains? ks x1))
                    (str kind (if ancestor-survives? " keeps" " drops") " the rooted commit's parent")))
              (<?? S (roots/release! (d/db conn) id)))))
        (finally (d/release conn) (d/delete-database c))))))

(deftest a-checkpoint-is-any-record-shaped-map
  ;; A synthetic record naming partial state is walked like a commit. Built
  ;; here from a real commit record with a different commit-id, which is all
  ;; a bulk build's checkpoint is: the six keys of trees nobody has published.
  (let [[conn c] (fresh-conn "checkpoint")]
    (try
      (d/transact conn [{:name "before" :n 1}])
      (let [original-cid (get-in (d/db conn) [:meta :datahike/commit-id])
            ;; The checkpoint names the commit's TREES under its own id; the
            ;; original commit RECORD is not among them and is fair game.
            snapshot-keys (disj (settled-keys conn) original-cid)
            record (-> (<?? S (roots/commit-record (d/db conn)))
                       (assoc-in [:meta :datahike/commit-id] (java.util.UUID/randomUUID)))
            id (<?? S (roots/root! (d/db conn) {:kind :checkpoint :record record}))]
        (churn! conn)
        (is (empty? (set/intersection (gc! conn) snapshot-keys))
            "the trees the checkpoint names survive")
        (<?? S (roots/release! (d/db conn) id))
        (is (seq (set/intersection (gc! conn) snapshot-keys)) "control"))
      (finally (d/release conn) (d/delete-database c)))))

(deftest a-blob-named-only-by-a-rooted-commit-is-live
  (let [[conn c] (fresh-conn "blob")]
    (try
      (d/transact conn [{:db/ident :doc/body
                         :db/valueType :db.type/store-ref
                         :db/cardinality :db.cardinality/one}])
      (let [bytes (.getBytes "rooted blob" "UTF-8")
            key (blob/blob-id bytes)
            sid (:id (:store c))]
        (with-unreferenced-writes sid
          (k/bassoc (:store @conn) key bytes {:sync? true})
          (d/transact conn [{:name "doc" :doc/body key}]))
        (let [id (<?? S (roots/pin! (d/db conn) {}))
              e (d/q '[:find ?e . :where [?e :doc/body]] (d/db conn))]
          ;; The head no longer names the blob; only the pinned commit does.
          (d/transact conn [[:db/retract e :doc/body key]])
          ;; `(Date.)` as remove-before throughout: with the default epoch-0
          ;; bound the commit that named the blob is retained as history and
          ;; keeps it live on its own, root or not.
          (is (contains? (<?? S (gc/reachable-store-refs (d/db conn) (Date.))) key)
              "the live set the application sweeps against includes it")
          (is (not (contains? (gc! conn) key)) "and the sweep spares it")
          (<?? S (roots/release! (d/db conn) id))
          (is (not (contains? (<?? S (gc/reachable-store-refs (d/db conn) (Date.))) key)) "control")
          (is (contains? (gc! conn) key) "control: collected once unrooted")))
      (finally (d/release conn) (d/delete-database c)))))

(deftest online-gc-pauses-while-a-root-exists
  (let [[conn c] (fresh-conn "online")]
    (try
      (d/transact conn [{:name "x" :n 1}])
      (let [store (:store @conn)
            db (d/db conn)
            id (<?? S (roots/pin! db {}))]
        (churn! conn)
        (is (= 0 (online-gc/online-gc! store {:enabled? true :sync? true :grace-period-ms 0}))
            "a rooted record older than the head makes freed-address hints unsound")
        (<?? S (roots/release! db id))
        (is (pos? (online-gc/online-gc! store {:enabled? true :sync? true :grace-period-ms 0}))
            "control: the freed addresses were there, and go once the root does"))
      (finally (d/release conn) (d/delete-database c)))))

(deftest no-roots-means-no-change
  (let [[conn c] (fresh-conn "none")]
    (try
      (d/transact conn [{:name "x" :n 1}])
      (churn! conn)
      (is (not (contains? (store-keys conn) roots/registry-key))
          "nothing writes the registry until someone declares a root")
      (is (seq (gc! conn)))
      (is (not (contains? (store-keys conn) roots/registry-key))
          "and the collector does not create it either")
      (finally (d/release conn) (d/delete-database c)))))

(deftest root-kinds-are-validated
  (is (= :datahike/gc-root-invalid-kind
         (:type (ex-data (try (roots/root! {} {:kind :bogus}) (catch Exception e e)))))))
