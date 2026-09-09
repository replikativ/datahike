(ns ^:no-doc datahike.sort
  "Synchronous external merge sorting shared by local-file adapters.
   Adapters own codecs, paths, quotas and private scratch directories. This
   engine owns windows, bounded merge scheduling and open-resource lifetimes.
   Run identifiers are [pass index]; records and byte buffers stay on entries.")

(defn- suppress! [primary cleanup]
  #?(:clj (when-not (identical? primary cleanup)
            (.addSuppressed ^Throwable primary ^Throwable cleanup))
     :cljs nil))

(defn- close-after! [close! primary]
  (try (close!)
       (catch #?(:clj Throwable :cljs :default) error
         (if primary (suppress! primary error) (throw error)))))

(defn- decorate [entry {:keys [key-fn]}]
  (if key-fn (assoc entry ::key (key-fn (:record entry))) entry))

(defn- entry-comparator [{:keys [key-fn cmp]}]
  (if key-fn
    (fn [a b] (compare (::key a) (::key b)))
    (fn [a b] (cmp (:record a) (:record b)))))

(defn sort-window
  "Sort records in memory. Keyed orders evaluate a key once per record and
   eagerly discard decorations before returning the result sequence."
  [records {:keys [key-fn cmp]}]
  (if key-fn
    (->> records
         (mapv (fn [record] [(key-fn record) record]))
         (sort-by first)
         (mapv second)
         seq)
    (sort cmp records)))

(defn- with-writer! [backend run-id consume]
  (let [writer ((:open-writer backend) run-id)
        primary (volatile! nil)]
    (try
      (consume (:write! writer))
      (catch #?(:clj Throwable :cljs :default) error
        (vreset! primary error)
        (throw error))
      (finally (close-after! (:close! writer) @primary)))))

(defn- spill! [backend run-id entries order]
  (with-writer! backend run-id
    (fn [write!]
      (doseq [entry (sort (entry-comparator order) entries)]
        (write! entry)))))

(defn initial-runs!
  "Prepare each input once and spill sorted windows. Returns the number of
   initial runs, named [0 index], without retaining a collection of paths.
   Entry :charge is supplied by the adapter; nil window-bytes disables that
   window limit. Adapter preparation must enforce individual record bounds."
  [records order backend {:keys [window-records window-bytes]}]
  (let [{:keys [run window]}
        (reduce (fn [{:keys [run window charge]} record]
                  (let [entry (decorate ((:prepare backend) record) order)
                        entry-charge (or (:charge entry) 0)]
                    (when (and window-bytes (> entry-charge window-bytes))
                      (throw (ex-info "Record exceeds the sort window."
                                      {:type ::record-too-large})))
                    (if (and (seq window)
                             (or (>= (count window) window-records)
                                 (and window-bytes (> entry-charge (- window-bytes charge)))))
                      (do (spill! backend [0 run] window order)
                          {:run (inc run) :window [entry] :charge entry-charge})
                      {:run run :window (conj window entry) :charge (+ charge entry-charge)})))
                {:run 0 :window [] :charge 0} records)]
    (when (seq window) (spill! backend [0 run] window order))
    (+ run (if (seq window) 1 0))))

(defn- merge-entry-resource! [run-ids order backend consume?]
  (let [readers (volatile! [])
        queue (volatile! nil)
        closed? (volatile! false)
        cmp (entry-comparator order)
        cursor-cmp (fn [a b]
                     (let [c (cmp a b)]
                       (if (zero? c) (compare (::reader a) (::reader b)) c)))
        close-reader! (fn [{:keys [reader closed?]}]
                        (when-not @closed?
                          (vreset! closed? true)
                          ((:close! reader))))
        close! (fn []
                 (when-not @closed?
                   (vreset! closed? true)
                   (vreset! queue nil)
                   (let [failure (volatile! nil)]
                     (doseq [reader @readers]
                       (try (close-reader! reader)
                            (catch #?(:clj Throwable :cljs :default) error
                              (if-let [first-error @failure]
                                (suppress! first-error error)
                                (vreset! failure error)))))
                     (vreset! readers [])
                     (when-let [error @failure] (throw error)))))
        next-entry! (fn [i]
                      (let [{:keys [reader run-id] :as handle} (nth @readers i)]
                        (if-let [entry ((:next! reader))]
                          (assoc (decorate entry order) ::reader i)
                          (do (close-reader! handle)
                              (when consume? ((:delete! backend) run-id))
                              nil))))]
    (try
      (vreset! queue (sorted-set-by cursor-cmp))
      (doseq [run-id run-ids]
        (let [reader ((:open-reader backend) run-id)
              i (count @readers)]
          (vswap! readers conj {:reader reader :run-id run-id :closed? (volatile! false)})
          (when-let [entry (next-entry! i)]
            (vswap! queue conj entry))))
      (letfn [(next! []
                (when-not @closed?
                  (try
                    (if-let [entry (first @queue)]
                      (let [i (::reader entry)]
                        (vswap! queue disj entry)
                        (when-let [next-entry (next-entry! i)]
                          (vswap! queue conj next-entry))
                        (dissoc entry ::reader ::key))
                      (do (close!) nil))
                    (catch #?(:clj Throwable :cljs :default) error
                      (close-after! close! error)
                      (throw error)))))]
        {:next! next! :close! close!})
      (catch #?(:clj Throwable :cljs :default) error
        (close-after! close! error)
        (throw error)))))

(defn merge-runs-resource!
  "Open a sorted run merge. Close explicitly on early abandonment; exhaustion
   and read/comparison failures close automatically. consume? deletes drained
   inputs. run-ids must be bounded by the caller's fan-in. The close closure
   retains only bounded reader/queue state, never the returned lazy sequence."
  [run-ids order backend consume?]
  (let [{:keys [next! close!]} (merge-entry-resource! run-ids order backend consume?)]
    (letfn [(step []
              (lazy-seq
               (when-let [entry (next!)]
                 (cons (:record entry) (step)))))]
      {:records (step) :close! close!})))

(defn- merge-into! [run-ids target order backend]
  (if (= 1 (count run-ids))
    ((:move! backend) (first run-ids) target)
    (let [{:keys [next! close!]} (merge-entry-resource! run-ids order backend true)
          primary (volatile! nil)]
      (try
        (with-writer! backend target
          (fn [write!]
            ;; Do not capture a lazy sequence head in this callback: on CLJS
            ;; that would retain the entire merged run until the sink closes.
            (loop []
              (when-let [entry (next!)]
                (write! entry)
                (recur)))))
        (catch #?(:clj Throwable :cljs :default) error
          (vreset! primary error)
          (throw error))
        (finally (close-after! close! @primary))))))

(defn reduce-runs!
  "Reduce initial runs to at most fan-in, preserving contiguous stable groups.
   Run metadata is pass/count plus one bounded group, never all run paths.
   Near the final fan-in, merge only enough groups to reach that cap: fan-in+1
   runs merge their first two and carry the rest without reencoding. Larger
   inputs use balanced full passes instead of repeatedly rewriting a growing
   accumulator run. Singleton carries use the adapter's move operation."
  [run-count order backend {:keys [fan-in]}]
  (loop [pass 0 runs run-count]
    (if (<= runs fan-in)
      (mapv #(vector pass %) (range runs))
      (let [partial? (<= runs (* fan-in fan-in))
            next-runs
            (loop [start 0 target 0 removals (if partial? (- runs fan-in) nil)]
              (if (= start runs)
                target
                (let [size (if partial?
                             (if (pos? removals) (min fan-in (inc removals)) 1)
                             (min fan-in (- runs start)))
                      end (+ start size)]
                  (merge-into! (mapv #(vector pass %) (range start end))
                               [(inc pass) target] order backend)
                  (recur end (inc target)
                         (when partial? (- removals (dec size)))))))]
        (recur (inc pass) next-runs)))))

(defn sorted-file!
  "Materialize one sorted run, or nil for empty input. A sole initial run is
   returned untouched. The adapter owns returned scratch and failure cleanup."
  [records order backend opts]
  (let [runs (reduce-runs! (initial-runs! records order backend opts) order backend opts)]
    (case (count runs)
      0 nil
      1 (first runs)
      (let [target [(inc (ffirst runs)) 0]]
        (merge-into! runs target order backend)
        target))))
