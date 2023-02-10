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
   [java.io File]
   [java.util.jar JarFile JarEntry]
   [java.net URL]))

(def checksums-file "checksums.edn")

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

(defn- get-jar [resource]
  (-> (.getPath resource)
      (string/split #"!" 2)
      first
      (subs 5)
      JarFile.))

(defmulti ^:private retrieve-file-list
  (fn [file-or-resource] (type file-or-resource)))

(defmethod ^:private retrieve-file-list File [file]
  (if (.exists file)
    (let [migration-files (file-seq file)
          xf (comp
              (filter #(.isFile %))
              (filter #(string/ends-with? (.getPath %) ".edn")))]
      (into [] xf migration-files))
    (throw
     (ex-info
      (format "Norms folder %s does not exist." file)
      {:folder file}))))

(defmethod ^:private retrieve-file-list URL [resource]
  (if resource
    (let [abs-path (.getPath resource)
          last-path-segment (-> abs-path (string/split #"/") peek)]
      (if (string/starts-with? abs-path "file:")
        (->> (get-jar resource)
             .entries
             enumeration-seq
             (filter #(and (string/starts-with? (.getName %) last-path-segment)
                           (not (.isDirectory %))
                           (string/ends-with? % ".edn"))))
        (->> (file-seq (io/file abs-path))
             (filter #(not (.isDirectory %))))))
    (throw
     (ex-info
      "Resource does not exist."
      {:resource (str resource)}))))

(defmethod ^:private retrieve-file-list :default [arg]
  (throw (ex-info "Can only read a File or a URL (resource)" {:arg arg
                                                              :type (type arg)})))

(defn- filter-file-list [file-list]
  (filter #(and (string/ends-with? % ".edn")
                (not (string/ends-with? (.getName %) checksums-file)))
          file-list))

(defn filename->keyword [filename]
  (-> filename
      (string/replace #" " "-")
      (keyword)))

(defmulti ^:private read-edn-file
  (fn [file-or-entry _file-or-resource] (type file-or-entry)))

(defmethod ^:private read-edn-file File [f _file]
  (when (not (.exists f))
    (throw (ex-info "Failed reading file because it does not exist" {:filename (str f)})))
  [(-> (slurp f)
       edn/read-string)
   {:name (.getName f)
    :norm (filename->keyword (.getName f))}])

(defmethod ^:private read-edn-file JarEntry [entry resource]
  (when (nil? resource)
    (throw (ex-info "Failed reading resource because it does not exist" {:resource (str resource)})))
  (let [file-name (-> (.getName entry)
                      (string/split #"/")
                      peek)]
    [(-> (get-jar resource)
         (.getInputStream entry)
         slurp
         edn/read-string)
     {:name file-name
      :norm (filename->keyword file-name)}]))

(defmethod ^:private read-edn-file :default [t _]
  (throw (ex-info "Can not handle argument" {:type (type t)
                                             :arg t})))

(defn- read-norm-files [norm-list file-or-resource]
  (->> norm-list
       (map (fn [f]
              (let [[content metadata] (read-edn-file f file-or-resource)]
                (merge content metadata))))
       (sort-by :norm)))

(defn- compute-checksums [norm-files]
  (->> norm-files
       (reduce (fn [m {:keys [norm] :as content}]
                 (assoc m
                        norm
                        (-> (select-keys content [:tx-data :tx-fn])
                            h/edn-hash
                            hp/hash->str)))
               {})))

(defn- verify-checksums [checksums checksums-edn file-or-resource]
  (let [edn-content (-> (read-edn-file checksums-edn file-or-resource) first)
        diff (data/diff checksums edn-content)]
    (when-not (every? nil? (butlast diff))
      (throw
       (ex-info "Deviation of the checksums found. Migration aborted."
                {:diff diff})))))

(s/def ::tx-data vector?)
(s/def ::tx-fn symbol?)
(s/def ::norm-map (s/keys :opt-un [::tx-data ::tx-fn]))
(defn- validate-norm [norm]
  (if (s/valid? ::norm-map norm)
    (log/debug "Norm validated" {:norm-map norm})
    (let [res (s/explain-data ::norm-map norm)]
      (throw
        (ex-info "Invalid norm" {:validation-error res})))))

(defn- neutral-fn [_] [])

(defn- transact-norms [conn norm-list]
  (let [db (ensure-norm-attribute! conn)]
    (log/info "Checking migrations ...")
    (doseq [{:keys [norm tx-data tx-fn]
             :as   norm-map
             :or   {tx-data []
                    tx-fn   'datahike.norm.norm/neutral-fn}}
            norm-list]
      (log/info "Checking migration" norm)
      (validate-norm norm-map)
      (when-not (norm-installed? db norm)
        (log/info "Run migration" {:tx-data (vec (concat [{:tx/norm norm}]
                                                         tx-data
                                                         ((var-get (requiring-resolve (symbol tx-fn))) conn)))})
        (->> (d/transact conn {:tx-data (vec (concat [{:tx/norm norm}]
                                                     tx-data
                                                     ((var-get (requiring-resolve tx-fn)) conn)))})
             (log/info "Done"))))))

(defmulti ^:private ensure-norms
  (fn [_conn file-or-resource] (type file-or-resource)))

(defmethod ^:private ensure-norms File [conn file]
  (let [norm-list (-> (retrieve-file-list file)
                      filter-file-list
                      (read-norm-files file))]
    (verify-checksums (compute-checksums norm-list)
                      (io/file (io/file file) checksums-file)
                      file)
    (transact-norms conn norm-list)))

(defmethod ^:private ensure-norms URL [conn resource]
  (let [file-list (retrieve-file-list resource)
        norm-list (-> (filter-file-list file-list)
                      (read-norm-files resource))]
    (verify-checksums (compute-checksums norm-list)
                      (->> file-list
                           (filter #(-> (.getName %) (string/ends-with? checksums-file)))
                           first)
                      resource)
    (transact-norms conn norm-list)))

(defn ensure-norms!
  "Takes Datahike-connection and optional a java.io.File object
   or java.net.URL to specify the location of your norms.
   Defaults to the resource `migrations`.
   Returns nil when successful and throws exception when not.

   Ensures your norms are present on your Datahike database.
   All the edn-files in this folder and its subfolders are
   considered migration-files aka norms and will be transacted
   ordered by their names into your database. All norms that
   are successfully transacted will have an attribute that
   marks them as migrated and they will not be applied twice."
  ([conn]
   (ensure-norms! conn (io/resource "migrations")))
  ([conn file-or-resource]
   (ensure-norms conn file-or-resource)))

(defn update-checksums!
  "Optionally takes a folder as string. Defaults to the
   folder `resources/migrations`.
   Returns nil when successful and throws exception when not.

   All the edn-files in the folder and its subfolders are
   considered migration-files aka norms. For each of
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
   (let [file (io/file norms-folder)]
    (-> (retrieve-file-list file)
        filter-file-list
        (read-norm-files file)
        compute-checksums
        (#(spit (io/file (io/file norms-folder checksums-file))
                (with-out-str (pp/pprint %))))))))

(comment
  (def norms-folder "test/datahike/norm/resources")
  (def config {:store {:backend :mem
                       :id "bar"}})
  (def conn (do
             (d/delete-database config)
             (d/create-database config)
             (d/connect config)))
  (ensure-norms! conn (io/file norms-folder))
  (ensure-norms! conn (io/resource "migrations"))
  (update-checksums! norms-folder)
  (update-checksums! "resources/migrations")
  (def my-fn 'datahike.norm.norm-test/tx-fn-test-fn)
  (#'datahike.norm.norm-test/tx-fn-test-fn conn)
  (def myfn (var-get (requiring-resolve my-fn)))
  (qualified-symbol? (symbol conn)))
