(ns datahike.norm.norm
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [datahike.api :as d])
  (:import
    [java.security MessageDigest DigestInputStream]))

(defn attribute-installed? [conn attr]
  (some? (d/entity @conn [:db/ident attr])))

(defn ensure-norm-attribute! [conn]
  (if-not (attribute-installed? conn :tx/norm)
    (:db-after (d/transact conn {:tx-data [{:db/ident       :tx/norm
                                            :db/valueType   :db.type/keyword
                                            :db/cardinality :db.cardinality/one}]}))
    @conn))

(defn norm-installed? [db norm]
  (->> {:query '[:find (count ?t) .
                 :in $ ?tn
                 :where
                 [_ :tx/norm ?tn ?t]]
        :args  [db norm]}
       d/q
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
                           (assoc :norm (-> (.getName migration-file)
                                            (string/replace #" " "_")
                                            (string/replace #"\.edn" "")
                                            keyword))))))]
        (sort-by :norm (into [] xf migration-files)))
      (throw
       (ex-info
        (format "Norms folder %s does not exist." norms-folder)
        {:folder norms-folder})))))

(defn neutral-fn [_] [])

(defn ensure-norms!
  ([conn]
   (ensure-norms! conn (io/resource "migrations")))
  ([conn norms-folder]
   (let [db (ensure-norm-attribute! conn)
         norm-list (read-norm-files! norms-folder)]
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

(defn update-checksums!
  ([^String filename]
   (update-checksums! filename (io/resource "migrations")))
  ([^String filename ^String norms-folder]
   (let [folder (io/file norms-folder)])))

(comment
  (def norms-folder "test/datahike/norm/resources")
  (let [folder (io/file norms-folder)]
    (if (.exists folder)
      (->> (file-seq folder)
           (filter #(re-find #".edn" (.getPath %)))
           (map #(.getName %))
           sort)
      (throw
       (ex-info
        (format "Norms folder %s does not exist." "foo")
        {:folder "foo"}))))


  (not (.exists (io/file (str norms-folder "checksums.edn"))))

  (spit (str norms-folder "/" "checksums.edn") {:foo "foo"})

  (def md (MessageDigest/getInstance "SHA-256"))
  (.checksum "test/datahike/norm/resources/checksums.edn" md)
  (import '[java.util Base64])
  (-> (io/input-stream (str norms-folder "/" "checksums.edn"))
      (DigestInputStream. md)
      .getMessageDigest
      .digest
      (#(map (partial format "%02x") %))
      (#(string/join "" %))))
