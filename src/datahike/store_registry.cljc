(ns ^:no-doc datahike.store-registry
  "Leased PSS reconstruction registrations. Native store decoders are lexical;
   wire decoders need this process-local registry while their subscription lives."
  (:require [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]))

(defonce ^:private owners (atom {}))
(defonce ^:private monitor #?(:clj (Object.) :cljs nil))

(defn- locked [f]
  #?(:clj (locking monitor (f)) :cljs (f)))

(defn- without-token [slots token]
  (filterv #(not= token (first %)) slots))

(defn- reconcile-external [id entry]
  ;; Direct PSS clients are outside our lease protocol. Preserve a different
  ;; registration rather than letting an old lease remove or restore over it.
  (let [actual (nodes/registered-storage id)]
    (if (identical? actual (:installed entry))
      entry
      (assoc entry :slots (cond-> (without-token (:slots entry) ::explicit)
                            actual (conj [::explicit actual]))
             :installed actual))))

(defn- install! [id entry slots]
  (let [store (second (peek slots))]
    (swap! nodes/storage-registry
           (fn [registry]
             (if (identical? (get registry id) (:installed entry))
               (if store (assoc registry id store) (dissoc registry id))
               registry)))
    (if (some #(not= ::explicit (first %)) slots)
      (swap! owners assoc id {:slots slots :installed store})
      (swap! owners dissoc id))))

(defn register!
  "Explicit, unmanaged registration. Unlike a lease, it requires unregister!.
   Tracked even for same-object replacement, so stale leases cannot erase it."
  [id store]
  (locked
   #(do
      (when-let [entry (get @owners id)]
        (swap! owners assoc id
               {:slots (conj (without-token (:slots entry) ::explicit) [::explicit store])
                :installed store}))
      (nodes/register-storage! id store))))

(defn unregister! [id]
  (locked
   #(if-let [entry (get @owners id)]
      (let [entry (reconcile-external id entry)]
        (install! id entry (without-token (:slots entry) ::explicit)))
      (nodes/unregister-storage! id)))
  nil)

(defn acquire!
  "Return an opaque lease. The latest live registration resolves wire roots;
   releasing it restores its live predecessor, never a released wrapper."
  [id store]
  (locked
   #(let [token (random-uuid)
          actual (nodes/registered-storage id)
          entry (if-let [entry (get @owners id)]
                  (reconcile-external id entry)
                  {:slots (if actual [[::explicit actual]] []) :installed actual})]
      (nodes/register-storage! id store)
      (swap! owners assoc id {:slots (conj (:slots entry) [token store]) :installed store})
      {:id id :token token})))

(defn replace!
  "Update this lease after rewrapping its store, without taking another lease."
  [{:keys [id token]} store]
  (locked
   #(let [entry (some->> (get @owners id) (reconcile-external id))]
      (when-not (some (fn [[t _]] (= token t)) (:slots entry))
        (throw (ex-info "Reconstruction registration lease is no longer live" {:id id})))
      (install! id entry (mapv (fn [[t value]] [t (if (= token t) store value)]) (:slots entry)))))
  store)

(defn release!
  "Idempotently release only this token. Explicit registrations survive."
  [{:keys [id token]}]
  (locked
   #(when-let [entry (get @owners id)]
      (let [entry (reconcile-external id entry)]
        (when (some (fn [[t _]] (= token t)) (:slots entry))
          (install! id entry (without-token (:slots entry) token))))))
  nil)
