(ns datahike.experimental.versioning
  "Git-like versioning tools for Datahike."
  (:require [konserve.core :as k]
            [datahike.connections :refer [delete-connection!]]
            [datahike.core :refer [with]]
            [datahike.store :refer [store-identity]]
            [datahike.writing :refer [stored->db db->stored stored-db?
                                      complete-db-update commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs!]]
            [superv.async :refer [<? S go-loop-try]]
            [datahike.db.utils :refer [db?]]
            [datahike.tools :as dt]
            [konserve.utils :refer [multi-key-capable?]]))

(defn- branch-check [branch]
  (when-not (keyword? branch)
    (dt/raise "Branch must be a keyword." {:type :branch-must-be-uuid :branch branch})))

(defn- db-check [db]
  (when-not (db? db)
    (dt/raise "You must provide a DB value." {:type :db-value-required :db db})))

(defn- parent-check [parents]
  (when-not (pos? (count parents))
    (dt/raise "You must provide at least one parent."
              {:type :must-provide-at-least-one-parent :parents parents})))

(defn- commit-id-check [commit-id]
  (when-not (uuid? commit-id)
    (dt/raise "Commit-id must be a uuid."
              {:type :commit-id-must-be-uuid :commit-id commit-id})))

;; ========================= public API =========================

(defn branch-history
  "Returns a go-channel with the commit history of the branch of the connection in
  form of all stored db values. Performs backtracking and returns dbs in order."
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
  "Create a new branch from commit-id or existing branch as new-branch."
  [conn from new-branch]
  (let [store (:store @conn)
        branches (k/get store :branches nil {:sync? true})
        _ (when (branches new-branch)
            (dt/raise "Branch already exists." {:type :branch-already-exists
                                                :new-branch new-branch}))
        db (k/get store from nil {:sync? true})]
    (when-not (stored-db? db)
      (throw (ex-info "From does not point to an existing branch or commit."
                      {:type :from-branch-does-not-point-to-existing-branch-or-commit
                       :from from})))
    (k/assoc store new-branch (assoc-in db [:config :branch] new-branch) {:sync? true})
    (k/update store :branches #(conj % new-branch) {:sync? true})))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Remote readers need to release their connections."
  [conn branch]
  (when (= branch :db)
    (dt/raise "Cannot delete main :db branch. Delete database instead."
              {:type :cannot-delete-main-db-branch}))
  (let [store (:store @conn)
        branches (k/get store :branches nil {:sync? true})]
    (when-not (branches branch)
      (dt/raise "Branch does not exist." {:type :branch-does-not-exist
                                          :branch branch}))
    (delete-connection! [(store-identity (get-in @conn [:config :store])) branch])
    (k/update store :branches #(disj % branch) {:sync? true})))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents need to point to a set of branches or commits.

  Be careful with this command as you can render a db inaccessible by corrupting
  a branch. You will also conflict with existing connections to the branch so
  make sure to close them before forcing the branch."
  [db branch parents]
  (db-check db)
  (branch-check branch)
  (parent-check parents)
  (let [store (:store db)
        cid (create-commit-id db)
        db-with-meta (-> db
                         (assoc-in [:config :branch] branch)
                         (assoc-in [:meta :datahike/parents] parents)
                         (assoc-in [:meta :datahike/commit-id] cid))
        ;; db->stored now returns [schema-meta-kv-to-write db-to-store]
        ;; and index flushes will have populated pending-writes
        [schema-meta-kv-to-write db-to-store] (db->stored db-with-meta true)
        ;; Get all pending [k v] pairs (e.g., from index flushes)
        pending-kvs (get-and-clear-pending-kvs! store)]

    ;; Update the set of known branches
    (k/update store :branches #(conj % branch) {:sync? true})

    ;; Write all data synchronously
    (if (multi-key-capable? store)
      (let [writes-map (cond-> (into {} pending-kvs) ; Initialize with pending KVs
                         schema-meta-kv-to-write (assoc (first schema-meta-kv-to-write) (second schema-meta-kv-to-write))
                         true                    (assoc cid db-to-store)
                         true                    (assoc branch db-to-store))]
        (k/multi-assoc store writes-map {:sync? true}))
      (do
        ;; Use the helper function to write pending KVs (synchronously)
        (write-pending-kvs! store pending-kvs true)
        ;; Then write schema-meta
        (when schema-meta-kv-to-write
          (k/assoc store (first schema-meta-kv-to-write) (second schema-meta-kv-to-write) {:sync? true}))
        ;; Then write commit-log and branch head
        (k/assoc store cid db-to-store {:sync? true})
        (k/assoc store branch db-to-store {:sync? true})))
    nil))

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
  "Loads the database stored at this commit id."
  [store commit-id]
  (commit-id-check commit-id)
  (when-let [raw-db (k/get store commit-id nil {:sync? true})]
    (stored->db raw-db store)))

(defn branch-as-db
  "Loads the database stored at this branch."
  [store branch]
  (branch-check branch)
  (when-let [raw-db (k/get store branch nil {:sync? true})]
    (stored->db raw-db store)))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids. It is the responsibility of the caller to make sure that tx-data
  contains the data to be merged into the branch from the parents. This function
  ensures that the parent commits are properly tracked.
   
   NOTE: Currently merge! requires that you release all connections to conn and reconnect afterwards to reset the writer state. This will be fixed in the future by handling merge! through the writer."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   (let [old @conn
         db (:db-after (complete-db-update old (with old tx-data tx-meta)))
         parents (conj parents (get-in old [:config :branch]))
         commit-db (commit! db parents)]
     (reset! conn commit-db)
     true)))
