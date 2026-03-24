(ns datahike.experimental.versioning
  "Git-like versioning tools for Datahike.
   All operations support both synchronous (CLJ default) and asynchronous modes."
  (:require [konserve.core :as k]
            [datahike.connections :refer [delete-connection!]]
            [datahike.store :refer [store-identity]]
            [datahike.writing :refer [stored->db db->stored stored-db?
                                      commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs!]]
            [datahike.writer]
            [datahike.index.secondary :as sec]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [superv.async :refer [#?(:clj <?) S go-loop-try]]
            [datahike.db.utils :refer [db?]]
            [datahike.tools :as dt]
            [replikativ.logging :as log]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            #?(:cljs [clojure.core.async :refer [<!]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]))
  #?(:clj (:import [datahike.connector Connection])))

(defn- branch-check [branch]
  (when-not (keyword? branch)
    (log/raise "Branch must be a keyword." {:type :branch-must-be-keyword :branch branch})))

(defn- db-check [db]
  (when-not (db? db)
    (log/raise "You must provide a DB value." {:type :db-value-required :db db})))

(defn- parent-check [parents]
  (when-not (pos? (count parents))
    (log/raise "You must provide at least one parent."
               {:type :must-provide-at-least-one-parent :parents parents})))

(defn- commit-id-check [commit-id]
  (when-not (uuid? commit-id)
    (log/raise "Commit-id must be a uuid."
               {:type :commit-id-must-be-uuid :commit-id commit-id})))

(defn- extract-store
  "Extract konserve store from a connection or db value."
  [conn-or-db]
  (cond
    #?(:clj (instance? Connection conn-or-db) :cljs (satisfies? IDeref conn-or-db))
    (:store @conn-or-db)

    (db? conn-or-db)
    (:store conn-or-db)

    :else
    ;; Assume it's a raw store
    conn-or-db))

;; ========================= public API =========================

(defn branches
  "List all known branch names. Returns set of keywords."
  ([conn] (branches conn {:sync? true}))
  ([conn opts]
   (let [store (extract-store conn)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try- (<?- (k/get store :branches nil opts)))))))

(defn branch-history
  "Returns the commit history of the branch of the connection in
  form of all stored db values. Performs backtracking and returns dbs in order.
  Always returns a channel."
  [conn]
  (let [{:keys [store] {:keys [branch]} :config} @conn]
    (go-loop-try S [[to-check & r] [branch]
                    visited #{}
                    reachable []]
                 (if to-check
                   (if (visited to-check) ;; skip
                     (recur r visited reachable)
                     (if-let [raw-db (<? S (k/get store to-check))]
                       (let [{{:keys [datahike/parents]} :meta
                              :as db} (stored->db raw-db store)]
                         (recur (concat r parents)
                                (conj visited to-check)
                                (conj reachable db)))
                       reachable))
                   reachable))))

(defn branch!
  "Create a new branch from commit-id or existing branch as new-branch.
   Secondary indices are CoW-branched via their native branching support."
  ([conn from new-branch] (branch! conn from new-branch {:sync? true}))
  ([conn from new-branch opts]
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [store (:store @conn)
                      existing-branches (<?- (k/get store :branches nil opts))
                      _ (when (and existing-branches (existing-branches new-branch))
                          (log/raise "Branch already exists." {:type :branch-already-exists
                                                               :new-branch new-branch}))
                      stored-db (<?- (k/get store from nil opts))]
                  (when-not (stored-db? stored-db)
                    (throw (ex-info "From does not point to an existing branch or commit."
                                    {:type :from-branch-does-not-point-to-existing-branch-or-commit
                                     :from from})))
                  ;; Branch secondary indices via their native CoW support.
                  ;; Prefer live indices from the connection (they hold the write lock).
                  (let [sec-keys (:secondary-index-keys stored-db)
                        live-indices (:secondary-indices @conn)
                        from-branch (or (when (keyword? from) from) :db)
                        branched-sec-keys
                        #?(:clj
                           (when (or (seq sec-keys) (seq live-indices))
                             (reduce-kv
                              (fn [acc idx-ident idx]
                                (if (satisfies? sec/IVersionedSecondaryIndex idx)
                                  (let [branched (sec/-sec-branch idx store from-branch new-branch)
                                        key-map (sec/-sec-flush branched store new-branch)]
                                    (when (instance? java.io.Closeable branched)
                                      (.close ^java.io.Closeable branched))
                                    (assoc acc idx-ident key-map))
                                  (if-let [key-map (get sec-keys idx-ident)]
                                    (assoc acc idx-ident
                                           (sec/branch-from-key-map key-map store from-branch new-branch))
                                    acc)))
                              {} (or live-indices {})))
                           :cljs nil)
                        updated-db (cond-> (assoc-in stored-db [:config :branch] new-branch)
                                     (seq branched-sec-keys) (assoc :secondary-index-keys branched-sec-keys))]
                    (<?- (k/assoc store new-branch updated-db opts))
                    (<?- (k/update store :branches #(conj (set %) new-branch) opts)))))))))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Remote readers need to release their connections."
  ([conn branch] (delete-branch! conn branch {:sync? true}))
  ([conn branch opts]
   (when (= branch :db)
     (log/raise "Cannot delete main :db branch. Delete database instead."
                {:type :cannot-delete-main-db-branch}))
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [store (:store @conn)
                      existing-branches (<?- (k/get store :branches nil opts))]
                  (when-not (and existing-branches (existing-branches branch))
                    (log/raise "Branch does not exist." {:type :branch-does-not-exist
                                                         :branch branch}))
                  (delete-connection! [(store-identity (get-in @conn [:config :store])) branch])
                  (<?- (k/update store :branches #(disj (set %) branch) opts))))))))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents must point to a set of branches or commits.

  WARNING: This overwrites the branch head unconditionally, like git reset --hard.
  Existing connections to this branch will see stale state and must be released
  and reconnected. Use with care — you can render data inaccessible."
  ([db branch parents] (force-branch! db branch parents {:sync? true}))
  ([db branch parents opts]
   (db-check db)
   (branch-check branch)
   (parent-check parents)
   (let [opts (select-keys opts [:sync?])
         sync? (:sync? opts)]
     (async+sync sync? *default-sync-translation*
               (go-try-
                (let [store (:store db)
                      cid (create-commit-id db)
                      db-with-meta (-> db
                                       (assoc-in [:config :branch] branch)
                                       (assoc-in [:meta :datahike/parents] parents)
                                       (assoc-in [:meta :datahike/commit-id] cid))
                      [schema-meta-kv-to-write db-to-store] (db->stored db-with-meta true)
                      pending-kvs (get-and-clear-pending-kvs! store)]

                  ;; Update the set of known branches
                  (<?- (k/update store :branches #(conj (set %) branch) opts))

                  ;; Write all data
                  (if (multi-key-capable? store)
                    (let [writes-map (cond-> (into {} pending-kvs)
                                      schema-meta-kv-to-write (assoc (first schema-meta-kv-to-write) (second schema-meta-kv-to-write))
                                      true                    (assoc cid db-to-store)
                                      true                    (assoc branch db-to-store))]
                      (<?- (k/multi-assoc store writes-map opts)))
                    (do
                      (<?- (write-pending-kvs! store pending-kvs sync?))
                      (when schema-meta-kv-to-write
                        (<?- (k/assoc store (first schema-meta-kv-to-write) (second schema-meta-kv-to-write) opts)))
                      (<?- (k/assoc store cid db-to-store opts))
                      (<?- (k/assoc store branch db-to-store opts))))
                  nil))))))

(defn commit-id
  "Retrieve the commit-id for this db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/commit-id]))

(defn parent-commit-ids
  "Retrieve parent commit ids from db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/parents]))

(defn commit-as-db
  "Loads the database stored at this commit id.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store cid] (commit-as-db conn-or-store cid {:sync? true}))
  ([conn-or-store cid opts]
   (commit-id-check cid)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store cid nil opts))]
                    (stored->db raw-db store)))))))

(defn branch-as-db
  "Loads the database stored at this branch.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store branch] (branch-as-db conn-or-store branch {:sync? true}))
  ([conn-or-store branch opts]
   (branch-check branch)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store branch nil opts))]
                    (stored->db raw-db store)))))))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids or branch keywords. It is the responsibility of the caller to
  make sure that tx-data contains the data to be merged into the branch from
  the parents. This function ensures that the parent commits are properly tracked.

  Routed through the writer for proper serialization with concurrent transactions.
  Returns a tx-report (sync) or promise/channel (async)."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   @(datahike.writer/merge-db! conn {:parents parents
                                     :tx-data tx-data
                                     :tx-meta tx-meta})))

(defn merge-async!
  "Async version of merge!. Returns a promise (CLJ) or channel (CLJS)."
  ([conn parents tx-data]
   (merge-async! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   (datahike.writer/merge-db! conn {:parents parents
                                    :tx-data tx-data
                                    :tx-meta tx-meta})))
