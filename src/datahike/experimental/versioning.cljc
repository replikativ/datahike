(ns datahike.experimental.versioning
  "Git-like versioning tools for Datahike."
 (:require [konserve.core :as k]
           [datahike.core :refer [transact]]
           [datahike.connector :refer [update-and-flush-db stored-db?]]))

(defn branch!
  "Create a new branch from commit-id or existing branch as new-branch."
  [conn from new-branch]
  (let [store (:store @conn)
        db (k/get store from nil {:sync? true})]
    (when-not (stored-db? db)
      (throw (ex-info "From does not point to an existing branch or commit."
                      {:type :from-branch-does-not-point-to-existing-branch-or-commit
                       :from from})))
    (k/assoc store new-branch db {:sync? true})
    (k/update store :branches #(conj % new-branch) {:sync? true})))

(defn delete-branch! [conn branch]
  (let [store (:store @conn)]
    (k/update store :branches #(disj % branch) {:sync? true})))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids. It is the responsibility of the caller to make sure that tx-data
  contains the data to be merged into the branch from the parents. This function
  ensures that the parent commits are properly tracked."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (let [{:keys [store config]} @conn
         _ (doseq [p parents]
             (when-not (k/get store p nil {:sync? true})
               (throw (ex-info "Parent does not exist in store."
                               {:type   :parent-does-not-exist-in-store
                                :parent p}))))
         commit-parents (doall (for [p (conj parents (:branch config))]
                                (if (keyword? p)
                                  (if-let [id (k/get-in store [p :meta :datahike/commit-id] nil {:sync? true})]
                                    id
                                    (throw (ex-info "Parent not pointing to a valid branch."
                                                    {:type   :parent-not-pointing-to-a-valid-branch
                                                     :parent p})))
                                  p)))]
     (update-and-flush-db conn tx-data tx-meta transact commit-parents))))
