(ns datahike.norm.norm
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.pprint :as pp]
   [clojure.data :as data]
   [clojure.edn :as edn]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :as log]
   [datahike.api :as d])
  (:import
   [java.security MessageDigest DigestInputStream]
   [java.io File]
   [java.nio.file Files]))

(defn- attribute-installed? [conn attr]
  (some? (d/entity @conn [:db/ident attr])))

(defn- ensure-norm-attribute! [conn]
  (if-not (attribute-installed? conn :tx/norm)
    (:db-after (d/transact conn {:tx-data [{:db/ident       :tx/norm
                                            :db/valueType   :db.type/keyword
                                            :db/cardinality :db.cardinality/one}]}))
    @conn))

(defn- norm-installed? [db norm]
  (->> {:query '[:find (count ?t) .
                 :in $ ?tn
                 :where
                 [_ :tx/norm ?tn ?t]]
        :args  [db norm]}
       d/q
       some?))

(defn- read-norm-files! [norms-folder]
  (let [folder (io/file norms-folder)]
    (if (.exists folder)
      (let [migration-files (file-seq folder)
            xf (comp
                (filter #(not (.isDirectory %)))
                (filter #(re-find #".edn" (.getPath %)))
                (filter #(not= "checksums.edn" (.getName %)))
                (map (fn [migration-file]
                       (-> (.getPath migration-file)
                           slurp
                           read-string
                           (assoc :norm (-> (.getName migration-file)
                                            (string/replace #" " "-")
                                            (string/replace #"\.edn" "")
                                            keyword))))))]
        (sort-by :norm (into [] xf migration-files)))
      (throw
       (ex-info
        (format "Norms folder %s does not exist." norms-folder)
        {:folder norms-folder})))))

(defn- compute-checksums [norms-folder]
  (let [folder (io/file norms-folder)]
    (if (.exists folder)
      (let [md (MessageDigest/getInstance "SHA-256")
            migration-files (file-seq folder)
            xf (-> (comp
                    (filter #(not (.isDirectory %)))
                    (filter #(re-find #".edn" (.getPath %)))
                    (filter #(not= "checksums.edn" (.getName %)))))
            filenames (sort (into [] xf migration-files))]
        (->> (doseq [f filenames]
               {(-> (.getName f)
                    (string/replace #" " "-")
                    (keyword))
                (->> (Files/readAllBytes (.toPath f))
                     (.digest md)
                     (BigInteger. 1)
                     (format "%064x"))})
             (into {})))
      (throw
       (ex-info
        (format "Norms folder %s does not exist." norms-folder)
        {:folder norms-folder})))))

(defn- verify-checksums [norms-folder]
  (let [diff (data/diff (compute-checksums norms-folder)
                        (edn/read-string (slurp (str norms-folder "/checksums.edn"))))]
    (when-not (every? nil? (butlast diff))
      diff)))

(s/def ::tx-data vector?)
(s/def ::tx-fn symbol?)
(s/def ::norm-map (s/keys :opt-un [::tx-data ::tx-fn]))
(defn- validate-norm [norm]
  (if (s/valid? ::norm-map norm)
    (log/debug "Norm validated" {:norm-map norm})
    (let [res (s/explain-data ::norm-map norm)]
      (throw
        (ex-info "Invalid norm" {:validation-error res})))))

(defn neutral-fn [_] [])

(defn- ensure-norms [conn norms-folder]
  (let [db (ensure-norm-attribute! conn)
        norm-list (read-norm-files! norms-folder)]
    (log/info "Checking migrations ...")
    (doseq [{:keys [norm tx-data tx-fn]
             :as   norm-map
             :or   {tx-data []
                    tx-fn   #'neutral-fn}}
            norm-list]
      (log/info "Checking migration" norm)
      (validate-norm norm-map)
      (when-not (norm-installed? db norm)
        (log/info "Run migration" norm)
        (->> (d/transact conn {:tx-data (vec (concat [{:tx/norm norm}]
                                                     tx-data
                                                     ((eval tx-fn) conn)))})
             (log/info "Done"))))))

(defn ensure-norms!
  ([conn]
   (ensure-norms! conn (io/resource "migrations")))
  ([conn norms-folder]
   (if-let [diff (verify-checksums norms-folder)]
     (throw
      (ex-info "Deviation of the checksums found. Migration aborted."
               {:diff diff}))
     (ensure-norms conn norms-folder))))

(defn update-checksums!
  ([]
   (update-checksums! (io/resource "migrations")))
  ([^String norms-folder]
   (-> (compute-checksums norms-folder)
       (#(spit (io/file (str norms-folder "/" "checksums.edn"))
               (with-out-str (pp/pprint %)))))))
