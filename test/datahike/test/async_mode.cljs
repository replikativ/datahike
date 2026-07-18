(ns datahike.test.async-mode
  "Shared harness for the three-mode query axis (sync / async-warm /
   async-cold): the AsyncOnlyStore wrapper, node-granular index flushing,
   per-case cold restores, and a uniform run-query-in-mode entry.

   Cold restores work directly on store-LESS in-memory dbs: psset/store
   walks the live tree writing every node into an ad-hoc konserve memory
   store (memory-backed connections never node-flush on their own — that
   is exactly why they are sync-capable), and psset/restore hands back a
   lazy root over a fresh CachedStorage whose sync reads the AsyncOnlyStore
   rejects. The restored tree is bit-identical to the source — no
   transact replay, no config/schema divergence."
  (:require [clojure.core.async :as a]
            [konserve.core :as k]
            [konserve.memory :as mem]
            [konserve.protocols :as kp]
            [datahike.index.persistent-set :as dip]
            [datahike.query :as q]
            [org.replikativ.persistent-sorted-set :as psset]))

(defrecord AsyncOnlyStore [inner]
  kp/PEDNKeyValueStore
  (-exists? [_ key opts] (kp/-exists? inner key opts))
  (-get-meta [_ key opts] (kp/-get-meta inner key opts))
  (-get-in [_ key-vec not-found opts]
    (if (:sync? opts)
      (throw (ex-info "async-only store: no synchronous reads" {:store :async-only}))
      (kp/-get-in inner key-vec not-found opts)))
  (-update-in [_ key-vec meta-up-fn up-fn opts] (kp/-update-in inner key-vec meta-up-fn up-fn opts))
  (-assoc-in [_ key-vec meta-up-fn val opts] (kp/-assoc-in inner key-vec meta-up-fn val opts))
  (-dissoc [_ key opts] (kp/-dissoc inner key opts))
  kp/PLockFreeStore
  (-lock-free? [_] true))

(defn fresh-sink-store
  "Ad-hoc synchronous konserve memory store to hold flushed index nodes."
  []
  (mem/new-mem-store (atom {}) {:sync? true :id (str (random-uuid))}))

(defn flush-index!
  "Write every node of a live PSS index into `store`; returns the root
   address for restore-cold. Works on never-flushed in-memory trees."
  [store index]
  (let [write-storage (dip/create-storage store {:store-cache-size 10000})
        addr (psset/store index write-storage {:sync? true})]
    (doseq [[a node] @(:pending-writes write-storage)]
      (k/assoc store a node {:sync? true}))
    {:address addr :comparator (.-comparator index)}))

(defn restore-cold
  "A LAZY copy of a flushed index: fresh empty CachedStorage over an
   async-only wrapper — every node load must go through the async seam."
  [store {:keys [address comparator]}]
  (psset/restore {:root-address address :comparator comparator}
                 (dip/create-storage (->AsyncOnlyStore store) {:store-cache-size 10000})))

(def index-keys [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn flush-db!
  "Flush all (present) indices of db into a fresh sink store; returns the
   handle map for cold-db."
  [db]
  (let [store (fresh-sink-store)]
    {:store store
     :roots (into {}
                  (keep (fn [k]
                          (when-some [idx (get db k)]
                            [k (flush-index! store idx)])))
                  index-keys)}))

(defn cold-db
  "db with every flushed index replaced by a fresh cold restore. Call per
   case — restores are O(1) and an earlier case's reads must not warm a
   later case. Temporal wrappers (as-of/history) must be applied AFTER."
  [db {:keys [store roots]}]
  (reduce-kv (fn [d k handle] (assoc d k (restore-cold store handle)))
             db roots))

(defn- classify [e]
  (if (= :storage/sync-read-unavailable (:error (ex-data e)))
    {:fault e}
    {:error e}))

(defn run-query-in-mode
  "Uniform three-mode query runner. Always returns a promise-chan yielding
   {:result r} | {:fault e} | {:error e}, with :sync-completed? set on the
   async modes (true iff the expression resolved on the calling stack —
   the trampoline property; always true on warm stores).
   :sync — q with the result cache bypassed. :async-warm/:async-cold —
   q-async (the caller supplies the warm or cold db)."
  [mode query & args]
  (case mode
    :sync
    (let [ch (a/promise-chan)]
      (a/put! ch (try {:result (binding [q/*query-result-cache?* false]
                                 (apply q/q query args))}
                      (catch :default e (classify e))))
      ch)
    (:async-warm :async-cold)
    (let [ch (a/promise-chan)
          sync-flag (atom true)]
      ((apply q/q-async query args)
       (fn [v] (a/put! ch {:result v :sync-completed? @sync-flag}))
       (fn [e] (a/put! ch (assoc (classify e) :sync-completed? @sync-flag))))
      ;; single-threaded: anything resolving after this line had parked
      (reset! sync-flag false)
      ch)))
