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
   [java.util.jar JarFile JarEntry]
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
  (fn [file-or-resource] (type file-or-resource)))

(defmethod ^:private retrieve-file-list File [file]
  (if (.exists file)
    (let [migration-files (file-seq file)
          xf (comp
              (filter #(.isFile %))
              (filter #(string/ends-with? (.getPath %) ".edn"))
              (filter #(not= "checksums.edn" (.getName %))))]
      (into [] xf migration-files))
    (throw
     (ex-info
      (format "Norms folder %s does not exist." file)
      {:folder file}))))

(defmethod ^:private retrieve-file-list URL [resource]
  (if resource
    (let [abs-path (.getPath resource)]
      (if (string/starts-with? abs-path "file:")
        (let [jar-path (-> abs-path (string/split #"!" 2) first (subs 5))
              jar-file (JarFile. jar-path)]
          (->> (enumeration-seq (.entries jar-file))
               (filter #(and #_(string/starts-with? % abs-path)
                             #_(not (string/ends-with? % "/"))
                             (not (.isDirectory %))
                             (string/ends-with? % ".edn")
                             (not= "checksums.edn" (.getName %))))))
        (->> (file-seq (io/file abs-path))
             (filter #(and (.isFile (io/file %))
                           (not (.isDirectory %))
                           (string/ends-with? % ".edn")
                           (not= "checksums.edn" (.getName %)))))))
    (throw
     (ex-info
      "Resource does not exist."
      {:resource (str resource)}))))

(defmethod ^:private retrieve-file-list :default [arg]
  (throw (ex-info "Can only read a File or a URL (resource)" {:arg arg
                                                              :type (type arg)})))

(comment
  (retrieve-file-list (io/file norms-folder))
  (def jar-path (-> (io/resource "migrations") (string/split #"!" 2) first (subs 5)))
  (def jar-file (JarFile. jar-path)))

(defn filename->keyword [filename]
  (-> filename
      (string/replace #" " "-")
      (keyword)))

(defmulti ^:private read-edn-file
  (fn [f & _args] (type f)))

(defmethod read-edn-file File [f]
  [(-> (slurp f)
       edn/read-string)
   {:name (.getName f)
    :norm (filename->keyword (.getName f))}])

(defmethod read-edn-file JarEntry [entry jar-file]
  [(-> (.getInputStream jar-file entry)
       slurp
       edn/read-string)
   {:name (.getName entry)
    :norm (filename->keyword (.getName entry))}])

(defn- read-norm-files-with-meta [norm-files]
  (->> norm-files
       (map (fn [f]
              (let [[content metadata] (read-edn-file f)]
                (merge content metadata))))
       (sort-by :norm)))

(defn- read-norm-files [norm-files]
  (->> norm-files
       (map #(first (read-edn-file %)))
       (sort-by :norm)))

(defn- compute-checksums [norm-files]
  (->> norm-files
       (reduce (fn [m {:keys [norm] :as content}]
                (assoc m
                       norm
                       (-> (h/edn-hash content)
                           (hp/hash->str))))
               {})))

(defn- verify-checksums [checksums checksums-edn]
  (let [diff (data/diff checksums
                        (read-edn-file checksums-edn))]
    (when-not (every? nil? (butlast diff))
      (throw
       (ex-info "Deviation of the checksums found. Migration aborted."
                {:diff diff})))))

(comment
  (def norms-folder "test/datahike/norm/resources")
  (read-norm-files! (retrieve-file-list (io/file norms-folder)))
  (verify-checksums (-> (io/file norms-folder)
                        retrieve-file-list
                        read-norm-files-with-meta
                        compute-checksums)
                    (-> (io/file (io/file norms-folder) "checksums.edn")
                        read-edn-file
                        first)))

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

(defmulti ensure-norms
  (fn [_conn file-or-resource] (type file-or-resource)))

(defmethod ensure-norms File [conn file]
  (verify-checksums file
                    (io/file (io/file file) "checksums.edn"))
  (let [db (ensure-norm-attribute! conn)
        norm-list (-> (retrieve-file-list (io/file file))
                      read-norm-files!)]
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
  ([conn file-or-resource]
   (ensure-norms conn file-or-resource)))

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
   (-> (io/file norms-folder)
       read-norm-files!
       compute-checksums
       (#(spit (io/file (str norms-folder "/" "checksums.edn"))
               (with-out-str (pp/pprint %)))))))

(comment
  (def norms-folder "test/datahike/norm/resources")
  (read-norm-files! (io/file norms-folder))
  (read-norm-files! (io/resource "migrations"))
  (read-norm-files! (io/resource "foo"))
  (-> (io/resource "migrations")
      (read-norm-files!)
      (compute-checksums))
  (-> (io/file norms-folder)
      (read-norm-files!)
      (compute-checksums))
  (verify-checksums norms-folder)
  (update-checksums! norms-folder))
