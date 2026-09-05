(ns datahike.index.compatibility
  "Checks persisted comparator formats before an index is exposed to readers."
  (:require [datahike.index :as di]))

(def pss-comparator-version 1)

(defn stored-marker [config]
  (when (= :datahike.index/persistent-set (:index config))
    {:pss-comparator-version pss-comparator-version}))

(defn- contains-scalar-nan? [v]
  (or #?(:clj (or (and (instance? Double v) (Double/isNaN ^double v))
                  (and (instance? Float v) (Float/isNaN ^float v)))
         :cljs (and (number? v) (js/isNaN v)))
      ;; Primitive arrays have their own, unchanged comparator. Sequential
      ;; values are compared recursively by datom/compare-value.
      (and (sequential? v) (some contains-scalar-nan? v))))

(defn ensure-compatible!
  "Reject unknown PSS formats and legacy roots containing scalar NaNs.

   Walk physical index sequences, never comparator-guided slices: a legacy
   tree's ordering may already disagree with the running comparator. Check all
   six roots, since NaN may remain only in history or in an inconsistent old
   index. This deliberately does not mutate or stamp a legacy record. A normal
   subsequent commit records the current format. HHT uses a separate comparator
   and is not assigned a PSS format."
  [stored store]
  (when (= :datahike.index/persistent-set (get-in stored [:config :index]))
    (if (contains? stored :pss-comparator-version)
      (when-not (= pss-comparator-version (:pss-comparator-version stored))
        (throw (ex-info "Unsupported persistent-set comparator format"
                        {:error :index/unsupported-comparator-version
                         :expected pss-comparator-version
                         :actual (:pss-comparator-version stored)})))
      (doseq [[key root-key] [[:eavt-key :eavt-root]
                              [:aevt-key :aevt-root]
                              [:avet-key :avet-root]
                              [:temporal-eavt-key :temporal-eavt-root]
                              [:temporal-aevt-key :temporal-aevt-root]
                              [:temporal-avet-key :temporal-avet-root]]
              :let [idx (get stored key)]
              :when idx]
        (let [attached (cond-> (di/with-storage :datahike.index/persistent-set
                                 idx (:storage store))
                         (get stored root-key) (di/-seed-root! (get stored root-key)))]
          (when (some #(contains-scalar-nan? (:v %)) (di/-seq attached))
            (throw (ex-info
                    "Legacy persistent-set index contains scalar NaN; export with the old runtime and rebuild into a new database before upgrading"
                    {:error :index/comparator-migration-required
                     :index key
                     :target-version pss-comparator-version}))))))))
