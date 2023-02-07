(ns datahike.norm.norm
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.pprint :as pp]
   [clojure.data :as data]
   [clojure.edn :as edn]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :as log]
   [datahike.api :as d]
   [hasch.core :as h]
   [hasch.platform :as hp])
  (:import
   [java.security MessageDigest DigestInputStream]
   [java.io File]
   [java.nio.file Files]
   [java.util.jar JarFile]
   [java.net URL]))

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

(defmulti ^:private retrieve-file-list
  (fn [file-or-resource]
    (println "FILEORRESOURCE: " file-or-resource)
    (cond
      (instance? File file-or-resource) :file
      (instance? URL file-or-resource) :resource)))

(defmethod ^:private retrieve-file-list :file [norms-folder]
  (let [folder (io/file norms-folder)]
    (if (.exists folder)
      (let [migration-files (file-seq folder)
            xf (comp
                (filter #(.isFile %))
                (filter #(re-find #".edn" (.getPath %)))
                (filter #(not= "checksums.edn" (.getName %))))]
        (into [] xf migration-files))
      (throw
       (ex-info
        (format "Norms folder %s does not exist." norms-folder)
        {:folder norms-folder})))))

(defmethod ^:private retrieve-file-list :resource [resource]
  (if resource
    (let [abs-path (.getPath resource)
          migration-files (if (string/starts-with? abs-path "file:")
                            (let [jar-path (-> abs-path (string/split #"!" 2) first (subs 5))
                                  jar-file (JarFile. jar-path)]
                              (->> (enumeration-seq (.entries jar-file))
                                   (filter #(and (string/starts-with? % abs-path)
                                                 (not (string/ends-with? % "/"))))))
                            (file-seq (io/file abs-path)))
          xf (comp
               (filter #(.isFile (io/file %)))
               (filter #(re-find #".edn" (.getPath %)))
               (filter #(not= "checksums.edn" (.getName %))))]
      (into [] xf migration-files))
    (throw
     (ex-info
      "Resource does not exist."
      {:resource (str resource)}))))

(comment
  (def norms-folder "test/datahike/norm/resources")
  (retrieve-file-list (io/file norms-folder))
  (retrieve-file-list (io/resource "migrations"))
  *e
  (-> (.getPath (io/resource "migrations")) (string/split #"!" 2) first (subs 5))
  (require '[datahike.norm.norm-test :as t])
  (def conn (t/create-test-db))
  (str (io/resource "migrations")))

(defn- read-norm-files! [norms-folder]
  (let [file-list (retrieve-file-list norms-folder)]
    (->> (map (fn [migration-file]
                (-> (.getPath migration-file)
                    slurp
                    read-string
                    (assoc :norm (-> (.getName migration-file)
                                     (string/replace #" " "-")
                                     (string/replace #"\.edn" "")
                                     keyword))))
             file-list)
         (sort-by :norm))))

(defn- compute-checksums [norms-folder]
  (->> (retrieve-file-list norms-folder)
       sort
       (reduce (fn [m f]
                (assoc m
                       (-> (.getName f)
                           (string/replace #" " "-")
                           (keyword))
                       (-> (h/edn-hash (slurp f))
                           (hp/hash->str))))
               {})))

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
  "Takes Datahike-connection and optional a folder as string.
   Returns nil when successful and throws exception when not.

   Ensure your norms are present on your Datahike database.
   All the edn-files in the folder and its subfolders are
   considered as migration-files aka norms and will be sorted
   and transacted into your database.
   All norms that are successfully transacted will have an
   attribute that marks them as migrated and they will not
   be transacted twice."
  ([conn]
   (ensure-norms! conn (io/resource "migrations")))
  ([conn norms-folder]
   (if-let [diff (verify-checksums norms-folder)]
     (throw
      (ex-info "Deviation of the checksums found. Migration aborted."
               {:diff diff}))
     (ensure-norms conn norms-folder))))

(defn update-checksums!
  "Optionally takes a folder as string. Defaults to the
   folder `resources/migrations`.

   All the edn-files in the folder and its subfolders are
   considered as migrations-files aka norms. For each of
   these norms a checksum will be computed and written to
   the file `checksums.edn`. Each time this fn is run,
   the `checksums.edn` will be overwritten with the current
   values.
   This prevents inadvertent migrations of your database
   when used in conjunction with a VCS. A merge-conflict
   should be raised when trying to merge a checksums.edn
   with stale checksums."
  ([]
   (update-checksums! "resources/migrations"))
  ([^String norms-folder]
   (-> (compute-checksums norms-folder)
       (#(spit (io/file (str norms-folder "/" "checksums.edn"))
               (with-out-str (pp/pprint %)))))))
