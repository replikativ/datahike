(ns datahike.kabel.walker-test
  "The datahike konserve-sync walker (`datahike.kabel.walker`) follows
   `:db.type/store-ref` values, so a blob referenced by a datom replicates with the
   datoms that name it — and lands in the NODE portion of the walk (ahead of the
   mutable pointer cells), so a subscriber never applies a head that points at an
   object it has not yet received.

   Portable (`deftest-async`): the walker runs on JVM and Node cljs — a browser
   subscriber walks the same way — so the test does too. Platform seams
   (create/connect/delete, byte construction) mirror `datahike.test.store-ref-test`."
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [datahike.api :as d]
                     [datahike.blob :as blob]
                     [datahike.kabel.walker :as w]
                     [datahike.test.async :refer [deftest-async]]
                     [konserve.core :as k]
                     [clojure.core.async :as a :refer [<! go]])
     :cljs (:require [cljs.test :refer [deftest is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.blob :as blob]
                     [datahike.kabel.walker :as w]
                     [datahike.test.async :refer-macros [deftest-async]]
                     [konserve.node-filestore]        ;; register :file backend on Node
                     [konserve.core :as k]
                     [cljs.nodejs :as nodejs]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(def ^:private schema
  [{:db/ident :issue/title :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :issue/attachment :db/valueType :db.type/store-ref
    :db/cardinality :db.cardinality/one}])

;; --- platform seams (see datahike.test.store-ref-test) ---

(defn- rand-uuid [] #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(defn- tmp-path [id]
  #?(:clj  (str (System/getProperty "java.io.tmpdir") "/dh-walker-" id)
     :cljs (let [os (nodejs/require "os") p (nodejs/require "path")]
             (.join p (.tmpdir os) (str "dh-walker-" id)))))

(defn- str->bytes [s]
  #?(:clj (.getBytes ^String s "UTF-8") :cljs (js/Buffer.from s "utf-8")))

(defn- index-of
  "Portable position of `x` in vector `v`, or -1 (cljs vectors have no `.indexOf`)."
  [v x]
  (or (first (keep-indexed (fn [i e] (when (= e x) i)) v)) -1))

(defn- fresh-conn [cfg]
  (go #?(:clj  (do (when (d/database-exists? cfg) (d/delete-database cfg))
                   (d/create-database cfg)
                   (d/connect cfg))
         :cljs (do (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
                   (<! (d/create-database cfg))
                   (<! (d/connect cfg {:sync? false}))))))

(deftest-async walk-follows-store-refs
  (let [cfg   {:store {:backend :file :path (tmp-path (rand-uuid)) :id (rand-uuid)}
               :schema-flexibility :write :keep-history? false}
        conn  (<! (fresh-conn cfg))
        store (:store @conn)]
    (<! (d/transact! conn schema))
    (let [bytes (str->bytes "sync payload")
          blob  (blob/blob-id bytes)]
      (<! (k/bassoc store blob bytes {:sync? false}))
      (<! (d/transact! conn [{:issue/title "sync me" :issue/attachment blob}]))
      (let [walked (<! (w/datahike-walk-fn store {}))
            v      (vec walked)]
        (testing "a store-ref'd blob is in the walked set — an index-only walk misses it"
          (is (contains? (set walked) blob)
              "the blob a datom names must be shipped, or the reference dangles on the subscriber"))
        (testing "and it precedes the mutable pointer cells, so it arrives before the head"
          (is (< (index-of v blob) (index-of v :branches))
              "content-addressed blobs belong with the nodes, ahead of :branches/:db")
          (is (contains? (set walked) :db) "the branch head is walked")))
      (testing "retract the datom and the blob drops out of the walk"
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "sync me"]] @conn)]
          (<! (d/transact! conn [[:db/retractEntity eid]])))
        (is (not (contains? (set (<! (w/datahike-walk-fn store {}))) blob))
            "nothing names it now — no reason to replicate it")))
    (d/release conn)
    #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))
