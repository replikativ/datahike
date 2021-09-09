(ns ^:no-doc datahike.migrate
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [taoensso.timbre :as timbre]
            [clj-cbor.core :as cbor]))

(defn coerce-tx [tx]
  (update tx :data (fn [data] (mapv (comp vec seq) data))))

(defmulti export-tx-log
  "Exports tx log"
  {:arglists '([tx-log meta-data path {:keys [format]}])}
  (fn [_ _ _ {:keys [format]}] format))

(defmethod export-tx-log :default [_ _ _ {:keys [format]}]
  (throw (IllegalArgumentException. (str "Can not write unknown export format: " format))))

(defmethod export-tx-log :edn [tx-log meta-data path _]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (prn meta-data)
      (doseq [tx tx-log]
        (prn (coerce-tx tx))))))

(defmethod export-tx-log :cbor [tx-log meta-data path _]
  (->> tx-log
       (map coerce-tx)
       (cons meta-data)
       (cbor/spit-all path)))

(defn compare-configs [{attribute-refs?-a    :attribute-refs?
                        keep-history?-a      :keep-history?
                        schema-flexibility-a :schema-flexibility
                        :as                  config-a}
                       {attribute-refs?-b    :attribute-refs?
                        keep-history?-b      :keep-history?
                        schema-flexibility-b :schema-flexibility
                        :as                  config-b}]
  (when (and (= :read schema-flexibility-a)
             (= :write schema-flexibility-b))
    (throw (ex-info "Import configuration mismatch. Databases with :read schema flexibility can not be imported into databases with :write schema flexibility." {:config-a config-a :config-b config-b})))
  (when-not (= attribute-refs?-a attribute-refs?-b)
    (timbre/warn "Attribute reference configuration mismatch. Adjusting mapping for import."))
  (when-not (= keep-history?-a keep-history?-b)
    (timbre/warn "History index configuration mismatch. Adjusting check for import.")))

(defn check-meta-data [{imported-version :version}
                       {running-version :version}]
  (when-not (= imported-version running-version)
    (let [[major-a minor-a fix-a :as version-a] (string/split imported-version #"\.")
          [major-b minor-b fix-b :as version-b] (string/split running-version #"\.")]
      (when-not (= major-a major-b)
        (throw (ex-info "Version mismatch." {:imported-version version-a
                                             :running-version  version-b}))))))

(defn read-import-data [s]
  (try (read-string s)
       (catch Exception e
         (throw (ex-info "Import meta data could not be read." (.getMessage e))))))

(defn read-tx-log [db-config db-meta path]
  (let [[raw-meta-data & raw-tx-reports] (line-seq (io/reader path))
        {:keys [config meta]}            (read-import-data raw-meta-data)
        {:keys [attribute-refs?]} config]
    (compare-configs config db-config)
    (check-meta-data meta db-meta)
    (for [tx-report (if attribute-refs?
                      (rest raw-tx-reports)
                      raw-tx-reports)]
      (read-import-data tx-report))))
