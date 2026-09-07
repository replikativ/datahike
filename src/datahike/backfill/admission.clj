(ns ^:no-doc datahike.backfill.admission
  "Process-local admission of owned AVET candidates. Not a transaction option
   or a serialized capability. The coordinator owns journal authenticity,
   storage lifetime, and final conditional publication."
  (:require [datahike.backfill.effects :as effects]
            [datahike.backfill.unique :as unique]
            [datahike.backfill.storage :as storage]
            [datahike.db.utils :as dbu]
            [datahike.schema :as schema]
            [datahike.index.interface :as di])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]
           [datahike.backfill.storage PrivateStorage]))

(defn- fail! [message]
  (throw (ex-info message {:type ::invalid-admission})))

(defn plan
  "Resolve an enable-only map of canonical attribute idents to index/unique
   flags. Produces the exact known-entity transaction; no user data or other
   schema updates can be appended to an admission. Ordinary schema validation
   still runs when this transaction is applied."
  [source patch]
  (when-not (and (map? patch) (seq patch) (<= (count patch) 256))
    (fail! "An AVET patch must contain between 1 and 256 attributes."))
  (let [entries
        (into (sorted-map)
              (map (fn [[ident flags]]
                     (let [old (get-in source [:schema ident])
                           eid (when (keyword? ident) (dbu/entid source ident))]
                       (when-not (and (keyword? ident) (not (schema/is-system-keyword? ident))
                                      (map? old) (integer? eid)
                                      (map? flags) (seq flags)
                                      (every? #{:db/index :db/unique} (keys flags))
                                      (or (not (contains? flags :db/index))
                                          (true? (:db/index flags)))
                                      (or (not (contains? flags :db/unique))
                                          (contains? #{:db.unique/value :db.unique/identity}
                                                     (:db/unique flags)))
                                      (or (not (:db/unique old))
                                          (not (contains? flags :db/unique))
                                          (= (:db/unique old) (:db/unique flags)))
                                      (not= old (merge old flags)))
                         (fail! "AVET patches may only enable flags on existing schema entities."))
                       (let [new (merge old flags)
                             assessment (schema/assess-schema-transition
                                         old new (= :write (get-in source [:config :schema-flexibility])))]
                         (when (seq (:invalid assessment))
                           (fail! "Requested AVET flags produce invalid schema."))
                         [ident {:old old :new new :eid eid}])))
                   patch))]
    {:patch patch :schema (:schema source) :entries entries
     :attrs (set (keys entries))
     :unique-attrs (into #{} (keep (fn [[ident {:keys [new]}]]
                                     (when (:db/unique new) ident))) entries)
     :tx-data (into [] (mapcat (fn [[ident {:keys [old new eid]}]]
                                 (keep (fn [flag]
                                         (when (not= (get old flag) (get new flag))
                                           [:db/add eid flag (get new flag)]))
                                       [:db/index :db/unique]))) entries)}))

(def ^:private issuer (Object.))
(deftype ^:private Certificate [issuer state])

(defn- state [certificate]
  (when-not (and (instance? Certificate certificate)
                 (identical? issuer (.-issuer ^Certificate certificate)))
    (fail! "Expected a local checked AVET certificate."))
  (.-state ^Certificate certificate))

(defn- source-cursor! [source cursor]
  (when-not (and (uuid? (:generation cursor)) (uuid? (:journal-id cursor))
                 (integer? (:sequence cursor)) (pos? (:sequence cursor))
                 (integer? (:offset cursor)) (pos? (:offset cursor))
                 (= (:generation cursor) (get-in source [:avet-build :id]))
                 (= cursor (get-in source [:avet-build-journal :cursor]))
                 (not (:avet-build-invalidated? source))
                 (empty? (:avet-build-effects source)))
    (fail! "Candidate source is not at the exact complete build cursor.")))

(defn- observed-attrs [source request]
  (into (:attrs request)
        (keep (fn [[ident entry]]
                (when (and (keyword? ident) (map? entry)
                           (or (:db/index entry) (:db/unique entry)))
                  ident)))
        (:schema source)))

(defn mint!
  "Worker-only full uniqueness validation. Candidate must be the complete
   AVET family built from source and owned until publication or cancellation.
   This constructor scans data: never call it in writer admission. A typed
   certificate authenticates this validation, not arbitrary caller-built trees."
  [source candidate patch cursor]
  (source-cursor! source cursor)
  (let [request (plan source patch)]
    (when-not (and (instance? PersistentSortedSet (:current candidate))
                   (or (nil? (:temporal candidate)) (instance? PersistentSortedSet (:temporal candidate)))
                   (if (:store source)
                     (instance? PrivateStorage (:storage candidate))
                     (nil? (:storage candidate)))
                   (or (not (get-in source [:config :keep-history?])) (:temporal candidate))
                   (set? (get-in source [:avet-build :attrs]))
                   (every? (get-in source [:avet-build :attrs]) (observed-attrs source request)))
      (fail! "Candidate lacks the complete requested AVET family."))
    (unique/validate! source (:current candidate) (:unique-attrs request))
    (Certificate. issuer {:source source :candidate candidate :request request :cursor cursor})))

(defn advance
  "Coordinator-only advancement over ONE authenticated complete transaction.
   Replays effects itself, then checks touched values. The coordinator supplies
   the exact resulting source and journal boundary, never speculative frames."
  [certificate next-source transaction cursor]
  (let [{:keys [source candidate request] previous :cursor} (state certificate)]
    (source-cursor! next-source cursor)
    (when-not (and (= (:schema source) (:schema next-source))
                   (= (:avet-build source) (:avet-build next-source))
                   (= (:generation previous) (:generation cursor) (:generation transaction))
                   (= (:journal-id previous) (:journal-id cursor))
                   (= (inc (:sequence previous)) (:sequence cursor) (:sequence transaction))
                   (< (:offset previous) (:offset cursor))
                   (= :transaction (:kind transaction))
                   (= (:max-tx next-source) (:max-tx transaction))
                   (vector? (:effects transaction)))
      (fail! "AVET certificate advancement requires the next complete transaction."))
    (let [candidate' (reduce effects/replay candidate (:effects transaction))]
      (unique/validate-effects! next-source (:current candidate') (:unique-attrs request)
                                (:effects transaction))
      (Certificate. issuer {:source next-source :candidate candidate'
                            :request request :cursor cursor}))))

(defn transaction-data [certificate] (:tx-data (:request (state certificate))))
(defn candidate [certificate] (:candidate (state certificate)))
(defn cursor [certificate] (:cursor (state certificate)))

(defn flush!
  "Persist both owned candidate trees and their private pending nodes before
   handoff. Returns a certificate for the flushed root objects. Must run on the
   worker/coordinator, never inside pure core/with. Advancement clears this seal."
  [certificate]
  (let [{:keys [candidate] :as checked} (state certificate)]
    (if-let [private (:storage candidate)]
      (let [backend (storage/build-store private)
            candidate' (cond-> (update candidate :current di/-flush backend)
                         (:temporal candidate) (update :temporal di/-flush backend))]
        (storage/flush! private)
        (Certificate. issuer (assoc checked :candidate candidate' :flushed? true)))
      certificate)))

(defn- live-candidate [checked]
  (let [{:keys [source candidate]} checked]
    (if (:storage candidate)
      (let [live-storage (get-in source [:store :storage])]
        (when-not live-storage (fail! "Prepared AVET activation requires live publication storage."))
        (cond-> (update candidate :current #(di/with-storage :datahike.index/persistent-set % live-storage))
          (:temporal candidate) (update :temporal #(di/with-storage :datahike.index/persistent-set % live-storage))))
      candidate)))

(defn rebind-source
  "Bind an authoritative writer wrapper to the same checked state. The same
   commit content with a DIFFERENT storage revision is not the same fence.
   Never substitutes database equality or a transaction number for root identity."
  [certificate actual-source]
  (let [{:keys [source cursor] :as checked} (state certificate)
        commit-id (get-in source [:meta :datahike/commit-id])
        revision (:datahike.writing/head-revision source)]
    (source-cursor! actual-source cursor)
    (when-not (and (some? commit-id) (some? revision)
                   (= commit-id (get-in actual-source [:meta :datahike/commit-id]))
                   (= revision (:datahike.writing/head-revision actual-source))
                   (every? #(identical? (get source %) (get actual-source %))
                           [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet :store])
                   (= (select-keys source [:schema :rschema :ident-ref-map :ref-ident-map
                                           :config :meta :avet-build :avet-build-journal
                                           :secondary-indices :secondary-index-keys :system-entities :hash :max-eid :max-tx :op-count
                                           :datahike.writing/stored-head-identity])
                      (select-keys actual-source [:schema :rschema :ident-ref-map :ref-ident-map
                                                  :config :meta :avet-build :avet-build-journal
                                                  :secondary-indices :secondary-index-keys :system-entities :hash :max-eid :max-tx :op-count
                                                  :datahike.writing/stored-head-identity])))
      (fail! "Writer source differs from the checked roots, schema, commit or storage fence."))
    (Certificate. issuer (assoc checked :source actual-source))))

(defn advance-range
  "Advance through a scoped authenticated journal reduction, retaining one
   candidate and cursor. reduce-frames takes [f init]; f takes [acc frame cursor].
   Check each complete transaction before proceeding, then bind the result to
   the exact leased final source. Intermediate source DBs are unnecessary while
   the generation's schema and ident mapping remain unchanged."
  [certificate final-source final-cursor reduce-frames]
  (let [{:keys [source candidate request] previous :cursor} (state certificate)]
    (source-cursor! final-source final-cursor)
    (when-not (and (= (:schema source) (:schema final-source))
                   (= (:ident-ref-map source) (:ident-ref-map final-source))
                   (= (:avet-build source) (:avet-build final-source))
                   (= (select-keys previous [:generation :journal-id])
                      (select-keys final-cursor [:generation :journal-id])))
      (fail! "AVET range no longer belongs to the checked schema and generation."))
    (let [result
          (reduce-frames
           (fn [{:keys [candidate cursor max-tx]} frame next-cursor]
             (when-not (and (= :transaction (:kind frame))
                            (= (:generation cursor) (:generation next-cursor) (:generation frame))
                            (= (:journal-id cursor) (:journal-id next-cursor))
                            (= (inc (:sequence cursor)) (:sequence next-cursor) (:sequence frame))
                            (integer? (:offset next-cursor))
                            (< (:offset cursor) (:offset next-cursor))
                            (<= (:offset next-cursor) (:offset final-cursor))
                            (<= (:sequence next-cursor) (:sequence final-cursor))
                            (integer? (:max-tx frame)) (<= max-tx (:max-tx frame))
                            (vector? (:effects frame)))
               (fail! "AVET range contains a noncontiguous or incomplete transaction."))
             (let [next-candidate (reduce effects/replay candidate (:effects frame))]
               (unique/validate-effects! final-source (:current next-candidate)
                                         (:unique-attrs request) (:effects frame))
               {:candidate next-candidate :cursor next-cursor :max-tx (:max-tx frame)}))
           {:candidate candidate :cursor previous :max-tx (:max-tx source)})]
      (when-not (and (= final-cursor (:cursor result))
                     (= (:max-tx final-source) (:max-tx result))
                     (or (not= previous final-cursor) (identical? source final-source)))
        (fail! "AVET range did not reach the exact leased source boundary."))
      (Certificate. issuer {:source final-source :candidate (:candidate result)
                            :request request :cursor final-cursor}))))

(defn check!
  "Verify exact source identity and generated transaction before any mutation."
  [certificate source tx-data]
  (let [{checked :source :keys [request cursor]} (state certificate)]
    (source-cursor! source cursor)
    (when-not (and (identical? source checked)
                   (= (:schema source) (:schema request))
                   (= tx-data (:tx-data request)))
      (fail! "Prepared AVET admission does not match this source and schema patch.")))
  (when-let [private (:storage (:candidate (state certificate)))]
    (let [{:keys [closed? failed? pending-nodes]} (storage/resources private)]
      (when (or (not (:flushed? (state certificate))) closed? failed? (pos? pending-nodes))
        (fail! "Prepared private AVET roots must be flushed and owned before activation."))))
  certificate)

(defn prepare-db [certificate source]
  (check! certificate source (transaction-data certificate))
  (let [{:keys [request] :as checked} (state certificate)
        candidate (live-candidate checked)]
    (-> source
        (assoc :avet (:current candidate) :temporal-avet (:temporal candidate))
        (dissoc :avet-build :avet-build-journal :avet-build-effects :avet-build-invalidated?)
        (effects/observe (observed-attrs source request)))))

(defn covers?
  "Only the exact certified old/new schema entry may skip a synchronous sweep."
  [certificate ident old-entry new-entry]
  (when certificate
    (let [entry (get-in (state certificate) [:request :entries ident])]
      (and entry (= old-entry (:old entry)) (= new-entry (:new entry))))))

(defn finish
  "Check activation's exact primary effects after all tuples and metadata have
   settled. Keep ordinary per-datom uniqueness checks as well."
  [certificate report]
  (if-not certificate
    report
    (let [{:keys [request] :as checked} (state certificate)
          candidate (live-candidate checked)
          after (:db-after report)
          observations (effects/observations after)
          ;; A derived tuple may change before its requested index flag becomes
          ;; schema-visible. Replay the complete activation into the original
          ;; certified roots, not into roots already changed by native hooks.
          trees (reduce effects/replay candidate observations)
          after (assoc after :avet (:current trees) :temporal-avet (:temporal trees))]
      (doseq [[ident {:keys [old new]}] (:entries request)]
        (when-not (and (= old (get-in report [:db-before :schema ident]))
                       (= new (get-in after [:schema ident])))
          (fail! "Activation did not produce its certified schema patch.")))
      (unique/validate-effects! after (:avet after) (:unique-attrs request)
                                observations)
      (assoc report :db-after (effects/unobserve after)))))
