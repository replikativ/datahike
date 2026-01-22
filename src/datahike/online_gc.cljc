(ns datahike.online-gc
  "Online garbage collection for freed index nodes.

   Runs incrementally during transaction commits with configurable grace period.
   Addresses are marked as freed during transient operations (PSS mutations),
   then deleted after a grace period to ensure concurrent readers don't crash.

   Usage:
     ;; In datahike config
     {:online-gc {:enabled? true
                  :grace-period-ms 60000    ;; 1 minute default
                  :max-batch 1000}}         ;; Max addresses per GC run

     ;; For bulk imports (no concurrent readers)
     {:online-gc {:enabled? true
                  :grace-period-ms 0        ;; No grace period
                  :max-batch 10000}}

     ;; Background GC (optional)
     (def stop-ch (start-background-gc! store {...opts...}))
     ;; Later: (async/close! stop-ch)"
  (:require [konserve.core :as k]
            [konserve.utils :refer [multi-key-capable?]]
            [clojure.core.cache.wrapped :as wrapped]
            [taoensso.timbre :refer [debug trace warn]]
            #?(:clj [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer [go-try- <?-]]))
  #?(:clj (:import [java.util Date])))

(defn get-time [d]
  #?(:clj (.getTime ^Date d)
     :cljs (.getTime d)))

(defn get-and-clear-eligible-freed!
  "Get freed addresses eligible for deletion based on grace period.

   Arguments:
     store - The datahike store
     grace-period-ms - Minimum age before deletion (milliseconds)
     max-batch - Maximum addresses to delete in one batch

   Returns: [addresses-to-delete remaining-freed]
     - addresses-to-delete: vector of addresses ready to delete
     - remaining-freed: vector of [addr ts] pairs still in grace period"
  [store grace-period-ms max-batch]
  (let [freed-atom (-> store :storage :freed-addresses)
        now #?(:clj (Date.) :cljs (js/Date.))
        cutoff-time (- (get-time now) grace-period-ms)]
    (if freed-atom
      (let [result (atom nil)]
        (swap! freed-atom
               (fn [freed-pairs]
                 (let [;; Filter into eligible and remaining
                       eligible (filterv (fn [[_addr ts]]
                                          (<= (get-time ts) cutoff-time))
                                        freed-pairs)
                       remaining (filterv (fn [[_addr ts]]
                                           (> (get-time ts) cutoff-time))
                                         freed-pairs)
                       ;; Limit batch size
                       to-delete (take max-batch eligible)
                       still-pending (vec (concat remaining (drop max-batch eligible)))]
                   (reset! result [(mapv first to-delete) still-pending])
                   still-pending)))
        @result)
      ;; No freed-addresses atom (shouldn't happen with CachedStorage)
      [[] []])))

(defn delete-freed-addresses!
  "Delete freed addresses from store and cache.

   Arguments:
     store - The datahike store
     addresses - Vector of addresses to delete
     sync? - Whether to perform synchronous deletion

   Returns: Number of addresses deleted (async channel or sync value)"
  [store addresses sync?]
  (if (empty? addresses)
    (if sync? 0 (go-try- 0))
    (let [cache (-> store :storage :cache)]
      (trace "Deleting" (count addresses) "freed addresses")
      ;; Evict from cache first
      (doseq [addr addresses]
        (wrapped/evict cache addr))
      ;; Batch delete from store
      (if (multi-key-capable? store)
        ;; Use batch delete if supported
        (if sync?
          (do
            (k/multi-dissoc store (vec addresses) {:sync? true})
            (count addresses))
          (go-try-
           (<?- (k/multi-dissoc store (vec addresses) {:sync? false}))
           (count addresses)))
        ;; Fallback to individual deletes for stores without multi-key support
        (if sync?
          (do
            (doseq [addr addresses]
              (k/dissoc store addr {:sync? true}))
            (count addresses))
          (go-try-
           (doseq [addr addresses]
             (<?- (k/dissoc store addr {:sync? false})))
           (count addresses)))))))

(defn online-gc!
  "Perform online GC during commit.

   Options:
     :grace-period-ms - Minimum age before deletion (default 60000 = 1 minute)
     :max-batch - Maximum addresses to delete per GC (default 1000)
     :enabled? - Enable online GC (default false for safety)
     :sync? - Synchronous deletion (default true)

   Returns: Number of addresses deleted (or async channel with count)"
  [store {:keys [grace-period-ms max-batch enabled? sync?]
          :or {grace-period-ms 60000
               max-batch 1000
               enabled? false
               sync? true}}]
  (if-not enabled?
    (if sync? 0 (go-try- 0))
    (let [[to-delete _remaining] (get-and-clear-eligible-freed! store grace-period-ms max-batch)]
      (if (seq to-delete)
        (do
          (debug "Online GC: deleting" (count to-delete) "addresses")
          (delete-freed-addresses! store to-delete sync?))
        (if sync? 0 (go-try- 0))))))

(defn start-background-gc!
  "Start a background process that periodically runs online GC.

   Arguments:
     store - The datahike store
     options - GC configuration map
       :grace-period-ms - Minimum age before deletion (default 60000 = 1 minute)
       :interval-ms - How often to run GC (default 10000 = 10 seconds)
       :max-batch - Maximum addresses to delete per run (default 1000)

   Returns: A channel that can be closed to stop the background GC

   Example:
     (def stop-ch (start-background-gc! store {:grace-period-ms 60000
                                               :interval-ms 10000
                                               :max-batch 1000}))
     ;; Later, to stop:
     (async/close! stop-ch)"
  [store {:keys [grace-period-ms interval-ms max-batch]
          :or {grace-period-ms 60000
               interval-ms 10000
               max-batch 1000}}]
  (let [stop-ch (async/chan)]
    (async/go-loop []
      (let [[_ ch] (async/alts! [stop-ch (async/timeout interval-ms)])]
        (when-not (= ch stop-ch)
          ;; Run GC asynchronously
          (try
            (let [deleted (async/<! (online-gc! store {:grace-period-ms grace-period-ms
                                                       :max-batch max-batch
                                                       :enabled? true
                                                       :sync? false}))]
              (when (and deleted (pos? deleted))
                (debug "Background GC deleted" deleted "addresses")))
            (catch #?(:clj Exception :cljs js/Error) e
              (warn "Background GC error:" e)))
          (recur))))
    stop-ch))
