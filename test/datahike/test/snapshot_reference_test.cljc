(ns datahike.test.snapshot-reference-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-reference :as ref]
            [datahike.snapshot-reference :as snapshot]
            [datahike.reachability :as reach]
            [datahike.online-gc :as online]
            [datahike.migrate.fs :as fs]
            [datahike.writing :as writing]
            [datahike.index.audit :as audit]
            [datahike.index.persistent-set :as pss]
            [datahike.kabel.walker :as walker]
            [datahike.index.secondary :as sec]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try-]]
            [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]
            [konserve.core :as k]
            #?(:cljs [konserve.node-filestore])
            [clojure.core.async :refer [<! #?(:clj go)] #?@(:cljs [:refer-macros [go]])]))

(defmethod ref/decode-reference :test/object [value]
  (when-not (and (vector? value) (= 3 (count value)) (= 1 (nth value 1)) (uuid? (nth value 2)))
    (throw (ex-info "Invalid test reference" {})))
  {:type :test/object :key (nth value 2)})

(defmethod ref/expand-reference :test/object [{:keys [key]} {:keys [store config]}]
  (when-not (:crypto-hash? config) (throw (ex-info "Marker context lacks policy" {})))
  (when-not (k/exists? store key {:sync? true})
    (throw (ex-info "Missing required test object" {:key key})))
  {:reachable #{key}})

(sec/register-index-type!
 :test/snapshot-local
 {:create (fn [_ _] nil) :storage-owner :datahike :validate-generation identity
  :mark-generation (fn [{:keys [generation-id]} store]
                     (if-let [generation (k/get store generation-id nil {:sync? true})]
                       (conj (:keys generation) generation-id)
                       (throw (ex-info "Missing test generation" {}))))})

(sec/register-index-type!
 :test/snapshot-external
 {:create (fn [_ _] nil) :storage-owner :external :validate-generation identity
  :mark-generation (fn [_ _] #{})
  :external-root #(select-keys % [:secondary-type :external-store-id :generation-id])})

(def snapshot-schema
  [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :snapshot :db/valueType :db.type/gc-ref :db/cardinality :db.cardinality/one}
   {:db/ident :custom :db/valueType :db.type/gc-ref :db/cardinality :db.cardinality/one}
   {:db/ident :blob :db/valueType :db.type/store-ref :db/cardinality :db.cardinality/one}])

(defn config [extra]
  (merge {:store {:backend :file :path (fs/temp-store-path! "dh-snapshot-ref-") :id (random-uuid)}
          :writer {:backend :self :writer-ownership :exclusive}
          :crypto-hash? true :keep-history? false :schema-flexibility :write
          :index-config {:branching-factor 8 :diff-buf-size 4}}
         extra))

(defn cutoff [] (#?(:clj java.util.Date. :cljs js/Date.) 4102444800000))
(defn error? [value] #?(:clj (instance? Throwable value) :cljs (instance? js/Error value)))

(deftest reference-marker-contract
  (let [sid (random-uuid) cid (random-uuid) value (ref/snapshot-reference sid cid)]
    (is (ref/valid-reference? value))
    (is (not (ref/valid-reference? [:datahike/snapshot 2 sid cid])))
    (is (not (ref/valid-reference? [:unknown 1 sid cid])))
    (is (= [{:key cid :parents? false :required? true}]
           (:records (ref/expand-reference (ref/decode-reference value) {:store-id sid}))))
    (is (try (ref/expand-reference (ref/decode-reference value) {:store-id (random-uuid)}) false
             (catch #?(:clj Exception :cljs :default) _ true)))
    (doseq [bad [nil {} {:reachable [cid]} {:unexpected #{}} {:records [{:key cid}]}]]
      (is (try (ref/merge-contributions (ref/merge-contributions) bad) false
               (catch #?(:clj Exception :cljs :default) _ true))))))

(defn- create-chain! [conn]
  (go-try-
   (<! (d/transact! conn snapshot-schema))
   (<! (d/transact! conn (mapv #(hash-map :n %) (range 120))))
   (let [store (:store @conn) blob (random-uuid) custom (random-uuid)
         eid (ffirst (d/q '[:find ?e :where [?e :n 0]] @conn))]
     (<! (k/assoc store custom :custom-payload {:sync? false}))
     (<! (k/assoc store blob :snapshot-payload {:sync? false}))
     (<! (d/transact! conn [[:db/add eid :blob blob] [:db/add eid :custom [:test/object 1 custom]]]))
     (let [first-ref (<! (snapshot/snapshot-ref @conn {:sync? false}))
           ids (mapv :e (d/datoms @conn :aevt :n))]
       (<! (d/transact! conn (into [[:db/retract eid :blob blob] [:db/retract eid :custom [:test/object 1 custom]]]
                                   (map-indexed (fn [i id] [:db/add id :n (+ i 1000)]) ids))))
       (<! (d/transact! conn [[:db/add eid :snapshot first-ref]]))
       (let [second-ref (<! (snapshot/snapshot-ref @conn {:sync? false}))
             parent (first (get-in (<! (k/get store (nth second-ref 3) nil {:sync? false})) [:meta :datahike/parents]))]
         (<! (d/transact! conn [[:db/add eid :snapshot second-ref]]))
         {:blob blob :custom custom :eid eid :first-ref first-ref :second-ref second-ref :parent parent})))))

(defn- check-retained! [conn {:keys [blob custom first-ref second-ref parent]}]
  (go-try-
   (let [store (:store @conn) cid (nth first-ref 3) second-cid (nth second-ref 3)
         shipped (<! (walker/datahike-walk-fn store {}))]
     (doseq [key [cid second-cid blob custom]]
       (is (true? (<! (k/exists? store key {:sync? false})))))
     (is (not (<! (k/exists? store parent {:sync? false}))) "exact references do not keep ancestry")
     (is (every? (set shipped) [cid second-cid blob custom :datahike/gc-reference-values?]))
     (is (< (get (zipmap shipped (range)) cid) (get (zipmap shipped (range)) :db)))
     (is (= #{blob} (<! (gc/reachable-store-refs @conn (cutoff) {:sync? false})))))
   true))

(defn- check-cold-and-release! [conn {:keys [eid blob custom first-ref second-ref]}]
  (go-try-
   (let [store (:store @conn)
         old-db (writing/stored->db (<! (k/get store (nth first-ref 3) nil {:sync? false})) store)]
     (is (= 120 (d/q '[:find (count ?e) . :where [?e :n _]] old-db)))
     (is (= 0 (d/q '[:find ?n . :where [?e :n ?n] [(= ?n 0)]] old-db)))
     (is (= 0 (<! (online/online-gc! store {:enabled? true :sync? false :grace-period-ms 0}))))
     (<! (d/transact! conn [[:db/retract eid :snapshot second-ref]]))
     (is (not (error? (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0})))))
     (doseq [key [(nth first-ref 3) (nth second-ref 3) blob custom]]
       (is (not (<! (k/exists? store key {:sync? false}))))))
   true))

(deftest-async transitive-snapshot-closure-and-release
  (doseq [extra [{:fuse-index-roots? false} {:fuse-index-roots? true :attribute-refs? true}]]
    (let [cfg (config extra)]
      #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
      (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))]
        (try
          (let [fixture (<! (create-chain! conn))
                swept (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0}))]
            (is (map? fixture))
            (is (not (error? swept)))
            (is (true? (<! (check-retained! conn fixture))))
            (d/release conn)
            (let [cold #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))]
              (try (is (true? (<! (check-cold-and-release! cold fixture))))
                   (finally (d/release cold)))))
          (finally (d/release conn)
                   #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))))))

(deftest-async missing-target-aborts-before-publication-and-sweep
  (let [cfg (config {})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn [{:n 1}]))
        (let [value (<! (snapshot/snapshot-ref @conn {:sync? false}))
              cid (nth value 3)
              eid (ffirst (d/q '[:find ?e :where [?e :n 1]] @conn))
              invalid (ref/snapshot-reference (:id (:store cfg)) (random-uuid))
              before (get-in @conn [:meta :datahike/commit-id])
              validation (<! (reach/validate-db-references
                              (:db-after (d/with @conn [[:db/add eid :snapshot invalid]])) {:sync? false}))]
          (is (error? validation))
          (is (= before (get-in @conn [:meta :datahike/commit-id])))
          (<! (d/transact! conn [[:db/add eid :snapshot value]]))
          (<! (k/dissoc store cid {:sync? false}))
          (let [garbage (random-uuid)]
            (<! (k/assoc store garbage :must-survive-aborted-mark {:sync? false}))
            (is (error? (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0}))))
            (is (true? (<! (k/exists? store garbage {:sync? false})))))
          (is (error? (<! (snapshot/snapshot-ref @conn {:sync? false})))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest reference-policy
  (doseq [cfg [{:crypto-hash? false} {:crypto-hash? true :online-gc {:enabled? true}}]]
    (is (try (reach/require-reference-policy! cfg) false
             (catch #?(:clj Exception :cljs :default) _ true))))
  (is (nil? (reach/require-reference-policy! {:crypto-hash? true}))))

(deftest-async invalid-reference-transaction-does-not-publish
  (doseq [cross-store? [false true]]
    (let [cfg (config {})]
      #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
      (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
            store (:store @conn)]
        (try
          (<! (d/transact! conn snapshot-schema))
          (<! (d/transact! conn [{:n 1}]))
          (let [cid (get-in @conn [:meta :datahike/commit-id])
                eid (ffirst (d/q '[:find ?e :where [?e :n 1]] @conn))
                invalid (if cross-store?
                          (ref/snapshot-reference (random-uuid) cid)
                          (ref/snapshot-reference (:id (:store cfg)) (random-uuid)))
                result (<! (d/transact! conn [[:db/add eid :snapshot invalid]]))]
            (is (error? result))
            (is (= cid (get-in (<! (k/get store :db nil {:sync? false})) [:meta :datahike/commit-id]))))
          (finally (d/release conn)
                   #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))))))

(deftest-async retained-temporal-reference-keeps-snapshot
  (let [cfg (config {:keep-history? true :fuse-index-roots? true})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn [{:n 0}]))
        (let [value (<! (snapshot/snapshot-ref @conn {:sync? false}))
              eid (ffirst (d/q '[:find ?e :where [?e :n 0]] @conn))]
          (<! (d/transact! conn [[:db/add eid :snapshot value]]))
          (<! (d/transact! conn [[:db/retract eid :snapshot value] [:db/add eid :n 1]]))
          (is (not (error? (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0})))))
          (is (true? (<! (k/exists? store (nth value 3) {:sync? false}))))
          (is (true? (<! (k/get store :datahike/gc-reference-values? nil {:sync? false}))))
          (is (= 0 (<! (online/online-gc! store {:enabled? true :sync? false :grace-period-ms 0}))))
          (is (= 0 (d/q '[:find ?n . :where [?e :n ?n]]
                        (writing/stored->db (<! (k/get store (nth value 3) nil {:sync? false})) store)))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest-async incomplete-exact-record-is-rejected
  (let [cfg (config {:keep-history? true})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn [{:n 1}]))
        (let [db @conn cid (get-in db [:meta :datahike/commit-id])
              record (<! (k/get store cid nil {:sync? false}))]
          (doseq [bad [(dissoc record :config)
                       (assoc record :config {:index :datahike.index/persistent-set})
                       (dissoc record :schema-meta-key)
                       (dissoc record :temporal-aevt-key)
                       (assoc-in record [:config :crypto-hash?] false)
                       (assoc-in record [:config :online-gc :enabled?] true)]]
            (<! (k/assoc store cid bad {:sync? false}))
            (is (error? (<! (snapshot/snapshot-ref db {:sync? false})))))
          (<! (k/assoc store cid record {:sync? false})))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest-async warmed-index-does-not-hide-missing-stored-child
  (let [cfg (config {:fuse-index-roots? true})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn (mapv #(hash-map :n %) (range 120))))
        (let [db @conn cid (get-in db [:meta :datahike/commit-id])
              record (<! (k/get store cid nil {:sync? false}))
              root (audit/-merkle-root (:eavt-key record))
              child (first (:children (pss/node-edges (<! (k/get store root nil {:sync? false})))))]
          (is (some? child))
          (is (= 120 (d/q '[:find (count ?e) . :where [?e :n _]] db)))
          (<! (k/dissoc store child {:sync? false}))
          (is (error? (<! (snapshot/snapshot-ref db {:sync? false})))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest-async snapshot-edges-include-secondary-generations
  (let [cfg (config {})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn [{:n 1}]))
        (let [db @conn cid (get-in db [:meta :datahike/commit-id])
              record (<! (k/get store cid nil {:sync? false}))
              local (random-uuid) payload (random-uuid)
              foreign (random-uuid) foreign-store (random-uuid)
              local-map {:type :test/snapshot-local :format-version 1 :storage-owner :datahike :generation-id local}
              foreign-map {:type :test/snapshot-external :format-version 1 :storage-owner :external
                           :secondary-type :test/vector :external-store-id foreign-store :generation-id foreign}
              envelope (select-keys foreign-map [:secondary-type :external-store-id :generation-id])
              eid (ffirst (d/q '[:find ?e :where [?e :n 1]] db))]
          (<! (k/assoc store local {:keys #{payload}} {:sync? false}))
          (<! (k/assoc store payload :secondary-payload {:sync? false}))
          ;; Exercise the persisted-generation boundary independently of a live
          ;; native adapter; the synthetic key-maps have registered markers.
          (<! (k/assoc store cid (assoc record :secondary-index-keys {:local local-map :foreign foreign-map}) {:sync? false}))
          (let [value (<! (snapshot/snapshot-ref db {:sync? false}))]
            (is (not (error? value)))
            (<! (d/transact! conn [[:db/add eid :snapshot value] [:db/add eid :n 2]]))
            (is (= #{envelope} (<! (gc/reachable-external-secondary-roots @conn (cutoff) {:sync? false}))))
            (is (not (error? (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0})))))
            (is (true? (<! (k/exists? store local {:sync? false}))))
            (is (true? (<! (k/exists? store payload {:sync? false}))))
            (<! (k/dissoc store payload {:sync? false}))
            (is (error? (<! (snapshot/snapshot-ref db {:sync? false}))))
            (<! (k/assoc store payload :secondary-payload {:sync? false}))
            (<! (d/transact! conn [[:db/retract eid :snapshot value]]))
            (is (= #{} (<! (gc/reachable-external-secondary-roots @conn (cutoff) {:sync? false}))))
            (<! (gc/gc-storage! @conn (cutoff) {:min-age-ms 0}))
            (is (not (<! (k/exists? store local {:sync? false}))))
            (is (not (<! (k/exists? store payload {:sync? false}))))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(defn replace-value-marker! [f]
  #?(:clj (alter-var-root #'reach/value-references (constantly f))
     :cljs (set! reach/value-references f)))

(defn replace-secondary-marker! [f]
  #?(:clj (.addMethod ^clojure.lang.MultiFn ref/expand-reference :secondary f)
     :cljs (-add-method ref/expand-reference :secondary f)))

(defn fatal-marker [] (throw #?(:clj (AssertionError. "Fatal test marker") :cljs 42)))
(defmethod ref/decode-reference :test/fatal [_] {:type :test/fatal})
(defmethod ref/expand-reference :test/fatal [_ _] (fatal-marker))

(deftest-async fatal-markers-cannot-skip-validation
  (let [cfg (config {})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (<! (d/transact! conn [{:n 1}]))
        (let [cid (get-in @conn [:meta :datahike/commit-id])
              eid (ffirst (d/q '[:find ?e :where [?e :n 1]] @conn))
              result (<! (d/transact! conn [[:db/add eid :custom [:test/fatal 1]]]))
              record (<! (k/get store cid nil {:sync? false}))]
          (is (error? result))
          (is (= cid (get-in (<! (k/get store :db nil {:sync? false})) [:meta :datahike/commit-id])))
          (let [original reach/value-references]
            (replace-value-marker! (fn [& _] (fatal-marker)))
            (try (is (error? (<! (gc/record-reference-keys store record))))
                 (finally (replace-value-marker! original)))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest-async cyclic-record-edges-and-secondary-payloads
  (let [cfg (config {})]
    #?(:clj (d/create-database cfg) :cljs (<! (d/create-database cfg)))
    (let [conn #?(:clj (d/connect cfg) :cljs (<! (d/connect cfg {:sync? false})))
          store (:store @conn)]
      (try
        (<! (d/transact! conn snapshot-schema))
        (let [record (<! (k/get store :db nil {:sync? false}))
              a (random-uuid) b (random-uuid) payload (random-uuid)
              request (fn [key] {:key key :parents? false :required? true})
              original (get-method ref/expand-reference :secondary)
              original-values reach/value-references
              expansions (atom 0)]
          ;; Synthetic immutable envelopes exercise cycles/shared targets at
          ;; the extension boundary, independent of content-hash construction.
          (doseq [[key next-key] [[a b] [b a]]]
            (<! (k/assoc store key (-> record
                                       (assoc-in [:meta :datahike/commit-id] key)
                                       (assoc :secondary-index-keys {:edge {:next next-key}})) {:sync? false})))
          (<! (k/assoc store payload :payload {:sync? false}))
          (replace-secondary-marker!
           (fn [descriptor _context]
             (swap! expansions inc)
             {:records [(request (get-in descriptor [:key-map :next]))]
              :store-refs #{payload}}))
          (replace-value-marker! (fn [& _] {:records [(request a)]}))
          (try
            (let [walked (<! (reach/reachable-in-branch store a (cutoff) (:config @conn) (atom {})
                                                        {:sync? false :required-root? true :include-parents? false}))
                  shipped (<! (gc/record-reference-keys store record))]
              (is (not (error? walked)) (str walked " " (pr-str (ex-data walked))))
              (is (every? (:reachable walked) [a b]))
              (is (= #{payload} (:store-refs walked)))
              (is (every? shipped [a b payload]))
              (is (= 4 @expansions) "two records expanded once per independent walk"))
            (finally (replace-secondary-marker! original)
                     (replace-value-marker! original-values))))
        (finally (d/release conn)
                 #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg))))))))

(deftest malformed-reference-identity-map-is-rejected
  (is (try (reach/value-references {:attribute-refs? true}
                                   {:snapshot {:db/valueType :db.type/gc-ref}}
                                   {} nil nil {})
           false
           (catch #?(:clj Exception :cljs :default) _ true))))
