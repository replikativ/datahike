(ns datahike.norm.norm
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [datahike.api :as d]))

(defn attribute-installed? [conn attr]
  (some? (d/entity @conn [:db/ident attr])))

(defn ensure-norm-attribute! [conn]
  (if-not (attribute-installed? conn :tx/norm)
    (:db-after (d/transact conn {:tx-data [{:db/ident       :tx/norm
                                            :db/valueType   :db.type/keyword
                                            :db/cardinality :db.cardinality/one}]}))
    @conn))

(defn norm-installed? [db norm]
  (->> {:query '[:find (count ?t)
                 :in $ ?tn
                 :where
                 [_ :tx/norm ?tn ?t]]
        :args  [db norm]}
       d/q
       first
       some?))

(defn read-norm-files! [norms-folder]
  (let [folder (io/file norms-folder)]
    (if (.exists folder)
      (let [migration-files (file-seq folder)
            xf (comp
                (filter #(re-find #".edn" (.getPath %)))
                (map (fn [migration-file]
                       (-> (.getPath migration-file)
                           slurp
                           read-string
                           (update :norm (fn [norm] (or norm
                                                        (-> (.getName migration-file)
                                                            (string/replace #" " "_")
                                                            (string/replace #"\.edn" "")
                                                            keyword))))))))]
        (sort-by :norm (into [] xf migration-files)))
      (throw
       (ex-info
        (format "Norms folder %s does not exist." norms-folder)
        {:folder norms-folder})))))

(defn neutral-fn [_] [])

(defn ensure-norms!
  ([conn]
   (ensure-norms! conn (io/resource "migrations")))
  ([conn migrations]
   (let [db (ensure-norm-attribute! conn)
         norm-list (cond
                     (string? migrations) (read-norm-files! migrations)
                     (vector? migrations) migrations)]
     (log/info "Checking migrations ...")
     (doseq [{:keys [norm tx-data tx-fn]
              :or   {tx-data []
                     tx-fn   #'neutral-fn}}
             norm-list]
       (log/info "Checking migration" norm)
       (when-not (norm-installed? db norm)
         (log/info "Run migration" norm)
         (->> (d/transact conn {:tx-data (vec (concat [{:tx/norm norm}]
                                                      tx-data
                                                      ((eval tx-fn) conn)))})
              (log/info "Done")))))))

(comment
  (d/delete-database {:store {:backend :file
                              :path "/tmp/file-example"}})
  (d/create-database {:store {:backend :file
                              :path "/tmp/file-example"}})
  (def conn (d/connect {:store {:backend :file
                                :path "/tmp/file-example"}}))
  (ensure-norms! conn "test/resources")
  (def norm-list (read-norm-files! "test/datahike/norm/resources"))
  (norm-installed? (d/db conn) (:norm (first norm-list)))
  (d/transact conn {:tx-data [{:foo "foo"}]}))


