(ns datahike.experimental.versioning
  "Git-like versioning tools for Datahike."
 (:require [konserve.core :as k]
           [datahike.core :refer [transact]]
           [datahike.connector :refer [update-and-flush-db stored-db? stored->db]]
           [superv.async :refer [<? S go-loop-try]]
           [datahike.tools :as dt]))

(defn branch-history
  "Returns a go-channel with the commit history of the branch of the connection in
  form of all stored db values. Performs backtracking and returns dbs in order."
  [conn]
  (let [{:keys [store]
         {:keys [branch]} :config} @conn]
    (go-loop-try S [[to-check & r] [branch]
                    visited #{}
                    reachable []]
                 (if to-check
                   (if (visited to-check) ;; skip
                     (recur r visited reachable)
                     (if-let [raw-db (<? S (k/get store to-check))]
                       (let [{{:keys [datahike/parents]} :meta
                              :as                        db} (stored->db raw-db store)]
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
    (k/assoc store new-branch db {:sync? true})
    (k/update store :branches #(conj % new-branch) {:sync? true})))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc."
  [conn branch]
  (let [store (:store @conn)
        branches (k/get store :branches nil {:sync? true})]
    (when-not (branches branch)
      (dt/raise "Branch does not exist." {:type :branch-does-not-exist
                                          :branch branch}))
    (k/update store :branches #(disj % branch) {:sync? true})))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids. It is the responsibility of the caller to make sure that tx-data
  contains the data to be merged into the branch from the parents. This function
  ensures that the parent commits are properly tracked."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (assert (pos? (count parents)) "You must provide at least one parent.")
   (update-and-flush-db conn tx-data tx-meta transact
                        (conj parents (get-in @conn [:config :branch])))))
