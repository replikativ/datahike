(ns ^:no-doc datahike.pull-api
  (:require
   [datahike.db.utils :as dbu]
   [datahike.db.interface :as dbi]
   [datalog.parser.pull :as dpp #?@(:cljs [:refer [PullSpec]])])
  #?(:clj
     (:import
      [datahike.datom Datom]
      [datalog.parser.pull PullSpec])))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(def ^:private ^:const +default-limit+ 1000)

(defn- initial-frame
  "Creates an empty pattern frame according to pattern information."
  [pattern eids multi?]
  {:state     :pattern
   :pattern   pattern
   :wildcard? (:wildcard? pattern)
   :specs     (-> pattern :attrs seq)
   :results   (transient [])
   :kvps      (transient {})
   :eids      eids
   :multi?    multi?
   :recursion {:depth {} :seen #{}}})

(defn subpattern-frame
  "Returns frame specific for given attribute"
  [pattern eids multi? attr]
  (assoc (initial-frame pattern eids multi?) :attr attr))

(defn reset-frame
  "Recalculate frame attributes from frame pattern and transfer end results to frame-specific result section"
  [frame eids kvps]
  (let [pattern (:pattern frame)]
    (assoc frame
           :eids      eids
           :specs     (seq (:attrs pattern))
           :wildcard? (:wildcard? pattern)
           :kvps      (transient {})
           :results   (cond-> (:results frame)
                        (seq kvps) (conj! kvps)))))

(defn push-recursion
  "Push newly processed entity and increase recursion depth."
  [rec attr eid]
  (let [{:keys [depth seen]} rec]
    (assoc rec
           :depth (update depth attr (fnil inc 0))
           :seen (conj seen eid))))

(defn seen-eid?
  [frame eid]
  (-> frame
      (get-in [:recursion :seen] #{})
      (contains? eid)))

(defn pull-seen-eid
  "Add eid to result set if entity already seen. Else return nil."
  [frame frames eid]
  (when (seen-eid? frame eid)
    (conj frames (update frame :results conj! {:db/id eid}))))

(defn single-frame-result
  [key frame]
  (some-> (:kvps frame) persistent! (get key)))

(defn recursion-result [frame]
  (single-frame-result ::recursion frame))

(defn recursion-frame
  [parent eid]
  (let [attr (:attr parent)
        rec  (push-recursion (:recursion parent) attr eid)]
    (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
           :recursion rec)))

(defn pull-recursion-frame
  "Processes recursion for one entity ID or collects results.
  Replaces current frame with
  - one frame with remaining entiy IDs and
  - one subpattern frame"
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (let [frame  (reset-frame frame (rest eids) (recursion-result frame))
          eid    (first eids)]
      (or (pull-seen-eid frame frames eid)
          (conj frames frame (recursion-frame frame eid))))
    (let [kvps    (recursion-result frame)
          results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))]
      (conj frames (assoc frame :state :done :results results)))))

(defn recurse-attr
  "Adds recursion frame to frame set if maximum recursion depth not reached"
  [db attr multi? eids eid parent frames]
  (let [{:keys [recursion pattern]} parent
        depth  (-> recursion (get :depth) (get attr 0))]
    (if (-> pattern :attrs (get attr) :recursion (= depth))
      (conj frames parent)
      (pull-recursion-frame
       db
       (conj frames parent
             {:state :recursion :pattern pattern
              :attr attr :multi? multi? :eids eids
              :recursion recursion
              :results (transient [])})))))

(let [pattern (PullSpec. true {})]                          ;; For performance purposes?
  (defn- expand-frame
    [parent eid attr-key multi? eids]
    (let [rec (push-recursion (:recursion parent) attr-key eid)]
      (-> pattern
          (subpattern-frame eids multi? attr-key)
          (assoc :recursion rec)))))

(defn db-ident-and-id [db x]
  (let [{:keys [ident ref]} (dbu/attr-info db x)]
    (if (dbu/ident-name? ident)
      {:db/id ref :db/ident ident}
      {:db/id ref})))

(defn pull-attr-datoms
  "Processes datoms found to requested pattern for given attribute, i.e.
   - limits the result set to specified or default limit,
   - renames attribute key if requested,
   - adds default value on missing attributes if requested.
   Adds frame if
   - subpattern requested on attribute,
   - recursion requested,
   - attribute is reference.
   Returns frame set."
  [db attr-key attr eid forward? datoms opts [parent & frames]]
  (let [limit (get opts :limit +default-limit+)
        attr-key (or (:as opts) attr-key)
        found (not-empty
               (cond->> datoms
                 limit (into [] (take limit))))]
    (if found
      (let [ref?       (dbu/ref? db attr)
            system-attrib-ref? (dbu/system-attrib-ref? db attr)
            component? (and ref? (dbu/component? db attr))
            multi?     (if forward? (dbu/multival? db attr)
                           (not component?))
            datom-val  (if forward? (fn [d] (.-v ^Datom d))
                           (fn [d] (.-e ^Datom d)))]

        (cond
          (contains? opts :subpattern)
          (->> (subpattern-frame (:subpattern opts)
                                 (mapv datom-val found)
                                 multi? attr-key)
               (conj frames parent))

          (contains? opts :recursion)
          (recurse-attr db attr-key multi?
                        (mapv datom-val found)
                        eid parent frames)

          (and component? forward?)
          (->> found
               (mapv datom-val)
               (expand-frame parent eid attr-key multi?)
               (conj frames parent))

          :else
          (let [as-value  (if (or ref? system-attrib-ref?)
                            #(db-ident-and-id db (datom-val %))
                            datom-val)
                single?   (not multi?)]
            (->> (cond-> (into [] (map as-value) found)
                   single? first)
                 (update parent :kvps assoc! attr-key)
                 (conj frames)))))
      (->> (cond-> parent
             (contains? opts :default)
             (update :kvps assoc! attr-key (:default opts)))
           (conj frames)))))

(defn pull-attr
  "Retrieve datoms for given entity id and specification from database"
  [db spec eid frames]
  (let [[attr-key opts] spec]
    (if (= :db/id attr-key)
      (if (not-empty (dbi/datoms db :eavt [eid]))
        (conj (rest frames)
              (update (first frames) :kvps assoc! :db/id eid))
        frames)
      (let [attr     (:attr opts)
            forward? (= attr-key attr)
            a (if (and (:attribute-refs? (dbi/-config db))
                       (not (number? attr)))
                (dbi/-ref-for db attr)
                attr)
            results  (if (nil? a)
                       []
                       (if forward?
                         (dbi/datoms db :eavt [eid a])
                         (dbi/datoms db :avet [a eid])))]
        (pull-attr-datoms db attr-key attr eid forward?
                          results opts frames)))))

(def ^:private filter-reverse-attrs
  (filter (fn [[k v]] (not= k (:attr v)))))

(defn expand-reverse-subpattern-frame
  [parent eid rattrs]
  (-> (:pattern parent)
      (assoc :attrs rattrs :wildcard? false)
      (subpattern-frame [eid] false ::expand-rev)))

(defn expand-result
  "Add intermediate result to frame next in line. Return frame set."
  [frames kvps]
  (->> kvps
       (persistent!)
       (update (first frames) :kvps into!)
       (conj (rest frames))))

(defn pull-expand-reverse-frame
  "Adds expand results of current frame to next frame in frame set."
  [db [frame & frames]]
  (->> (or (single-frame-result ::expand-rev frame) {})
       (into! (:expand-kvps frame))
       (expand-result frames)))

(defn pull-expand-frame
  "Processes datoms for one attribute or changes frame state to process reverse attributes
  and spawns new frame for subpattern."
  [db [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)
          opts          (-> frame
                            (get-in [:pattern :attrs])
                            (get attr {}))]
      (pull-attr-datoms db attr attr (:eid frame) true datoms opts
                        (conj frames (update frame :datoms rest))))
    (if-let [rattrs (->> (get-in frame [:pattern :attrs])
                         (into {} filter-reverse-attrs)
                         not-empty)]
      (let [frame  (assoc frame
                          :state       :expand-rev
                          :expand-kvps (:kvps frame)
                          :kvps        (transient {}))]
        (->> rattrs
             (expand-reverse-subpattern-frame frame (:eid frame))
             (conj frames frame)))
      (expand-result frames (:kvps frame)))))

(defn pull-wildcard-expand
  [db frame frames eid pattern]
  (let [datoms (group-by (fn [d] (if (:attribute-refs? (dbi/-config db))
                                   (dbi/-ident-for db (.-a ^Datom d))
                                   (.-a ^Datom d)))
                         (dbi/datoms db :eavt [eid]))
        {:keys [attr recursion]} frame
        rec (cond-> recursion
              (some? attr) (push-recursion attr eid))]
    (->> {:state :expand :kvps (transient {:db/id eid})
          :eid eid :pattern pattern :datoms (seq datoms)
          :recursion rec}
         (conj frames frame)
         (pull-expand-frame db))))

(defn pull-wildcard
  [db frame frames]
  (let [{:keys [eid pattern]} frame]
    (or (pull-seen-eid frame frames eid)
        (pull-wildcard-expand db frame frames eid pattern))))

(defn pull-pattern-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (if (:wildcard? frame)
      (pull-wildcard db
                     (assoc frame
                            :specs []
                            :eid (first eids)
                            :wildcard? false)
                     frames)
      (if-let [specs (seq (:specs frame))]
        (let [spec       (first specs)
              new-frames (conj frames (assoc frame :specs (rest specs)))]
          (pull-attr db spec (first eids) new-frames))
        (->> frame :kvps persistent! not-empty
             (reset-frame frame (rest eids))
             (conj frames)
             (recur db))))
    (conj frames (assoc frame :state :done))))

(defn pull-pattern
  [db frames]
  (case (:state (first frames))
    :expand     (recur db (pull-expand-frame db frames))
    :expand-rev (recur db (pull-expand-reverse-frame db frames))
    :pattern    (recur db (pull-pattern-frame db frames))
    :recursion  (recur db (pull-recursion-frame db frames))
    :done       (let [[f & remaining] frames
                      result (cond-> (persistent! (:results f))
                               (not (:multi? f)) first)]
                  (if (seq remaining)
                    (->> (cond-> (first remaining)
                           result (update :kvps assoc! (:attr f) result))
                         (conj (rest remaining))
                         (recur db))
                    result))))

(defn pull-spec
  [db pattern eids multi?]
  (let [eids (into [] (map #(dbu/entid-strict db %)) eids)]
    (pull-pattern db (list (initial-frame pattern eids multi?)))))

(defn pull
  ([db {:keys [selector eid]}]
   (pull db selector eid))
  ([db selector eid]
   {:pre [(dbu/db? db)]}
   (pull-spec db (dpp/parse-pull selector) [eid] false)))

(defn pull-many [db selector eids]
  {:pre [(dbu/db? db)]}
  (pull-spec db (dpp/parse-pull selector) eids true))
