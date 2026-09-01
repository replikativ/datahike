(ns backward-secondary-test
  (:require [datahike.api :as d]
            [datahike.index.entity-set :as entity-set]
            [datahike.index.secondary :as secondary]
            [datahike.index.secondary.proximum]
            [datahike.index.secondary.scriptum]
            [datahike.index.secondary.stratum]
            [konserve.core :as konserve]
            [taoensso.trove :as trove])
  (:import [java.io File FileInputStream]
           [java.security MessageDigest]))

(def ^:private primary-ids
  {:stratum #uuid "e31a77d5-ae9b-4743-a35a-cd2b7c05a54d"
   :scriptum #uuid "7b06d098-1d39-491f-808e-e801b60e6ca2"
   :proximum #uuid "2c3a63c5-bc5a-4835-89a4-f36ef12b27fb"
   :current-scriptum #uuid "889263b6-78ba-4590-a397-385e764d38f9"
   :current-proximum #uuid "2d60fefe-9cec-48e3-9123-52c4cb4d2e99"})

(def ^:private proximum-store-id
  #uuid "b36c027c-c1a4-4f24-a71d-42686c97954f")

(def ^:private current-proximum-store-id
  #uuid "15ff6416-5c8c-454e-bd6c-6ef6bfb231b0")

(defn- root []
  (or (System/getenv "BACK_COMPAT_ROOT")
      (throw (ex-info "BACK_COMPAT_ROOT is required." {}))))

(defn- path [suffix]
  (str (root) "/" suffix))

(defn- config [kind]
  {:store {:backend :file
           :path (path (str "primary-" (name kind)))
           :id (primary-ids kind)}
   :writer {:backend :self :writer-ownership :exclusive}
   :schema-flexibility :write
   :keep-history? true
   :max-string-length 0})

(defn- await-ready [conn ident]
  (let [deadline (+ (System/currentTimeMillis) 30000)]
    (loop []
      (let [status (get-in (d/db conn) [:schema ident :db.secondary/status])]
        (cond
          (= :ready status) true
          (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Timed out waiting for released secondary index."
                          {:index ident :status status}))
          :else (do (Thread/sleep 25) (recur)))))))

(defn- create-stratum! []
  (let [cfg (config :stratum)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :item/price
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :idx/price
                           :db.secondary/type :stratum
                           :db.secondary/attrs [:item/price]}])
        (await-ready conn :idx/price)
        (d/transact conn [{:item/price 10} {:item/price 20}])
        (finally
          (d/release conn))))))

(defn- create-scriptum-db! [kind cache-path text]
  (let [cfg (config kind)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :doc/body
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:doc/body]
                           :db.secondary/config {:path (path cache-path)}}])
        (await-ready conn :idx/fulltext)
        (d/transact conn [{:doc/body text}])
        (finally
          (d/release conn))))))

(defn- create-scriptum! []
  (create-scriptum-db! :scriptum "scriptum-index" "released brown fox"))

(defn- create-proximum-db! [kind external-store-id store-path mmap-path]
  (let [cfg (config kind)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :doc/embedding
                           :db/valueType :db.type/float-array
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :idx/vector
                           :db.secondary/type :proximum
                           :db.secondary/attrs [:doc/embedding]
                           :db.secondary/config
                           {:dim 4
                            :distance :cosine
                            :capacity 64
                            :mmap-dir (path mmap-path)
                            :store-config {:backend :file
                                           :path (path store-path)
                                           :id external-store-id}}}])
        (await-ready conn :idx/vector)
        (d/transact conn [{:doc/embedding
                           (float-array [1.0 0.0 0.0 0.0])}])
        (finally
          (d/release conn))))))

(defn- create-proximum! []
  (create-proximum-db! :proximum proximum-store-id
                       "proximum-store" "proximum-release-mmap"))

(defn write [_]
  (create-stratum!)
  (create-scriptum!)
  (create-proximum!))

(defn- secondary-index [conn ident]
  (or (get-in (d/db conn) [:secondary-indices ident])
      (throw (ex-info "Secondary index was not restored." {:index ident}))))

(defn- verify-primary-count [conn attr expected]
  (assert (= expected
             (d/q '[:find (count ?v) . :in $ ?a :where [_ ?a ?v]]
                  (d/db conn) attr))))

(defn- verify-old-scriptum! []
  (let [conn (d/connect (config :scriptum))]
    (try
      (verify-primary-count conn :doc/body 1)
      (let [matches (secondary/-search
                     (secondary-index conn :idx/fulltext)
                     {:query "fox" :field :value} nil)]
        (assert (= 1 (entity-set/entity-bitset-cardinality matches))))
      (finally
        (d/release conn)))))

(defn verify-old [_]
  ;; Run after current code deliberately refuses these roots. Successful old
  ;; Scriptum open proves that refusal did not rewrite its Datahike head or
  ;; native index. The released Proximum adapter itself cannot reopen a durable
  ;; file store (its restore path calls create-store on the existing path), so
  ;; current code checks its exact files before and after the refusal instead.
  (trove/set-log-fn! (fn [& _]))
  (verify-old-scriptum!))

(defn write-current [_]
  (trove/set-log-fn! (fn [& _]))
  (create-scriptum-db! :current-scriptum "scriptum-current-cache"
                       "current generation fox")
  (create-proximum-db! :current-proximum current-proximum-store-id
                       "proximum-current-store" "proximum-current-mmap"))

(defn verify-current-formats [_]
  (trove/set-log-fn! (fn [& _]))
  (let [scriptum-conn (d/connect (config :current-scriptum))
        proximum-conn (d/connect (config :current-proximum))]
    (try
      (verify-primary-count scriptum-conn :doc/body 1)
      (let [matches (secondary/-search
                     (secondary-index scriptum-conn :idx/fulltext)
                     {:query "fox" :field :value} nil)]
        (assert (= 1 (entity-set/entity-bitset-cardinality matches))))
      (verify-primary-count proximum-conn :doc/embedding 1)
      (let [matches (secondary/-search
                     (secondary-index proximum-conn :idx/vector)
                     {:vector (float-array [1.0 0.0 0.0 0.0]) :k 1} nil)]
        (assert (= 1 (entity-set/entity-bitset-cardinality matches))))
      (finally
        (d/release proximum-conn)
        (d/release scriptum-conn)))))

(defn- throwable-data [failure]
  (loop [pending [failure]
         result []]
    (if-let [current (peek pending)]
      (let [data (ex-data current)
            nested-error (when (map? data) (:error data))]
        (recur (cond-> (pop pending)
                 (.getCause ^Throwable current)
                 (conj (.getCause ^Throwable current))
                 (instance? Throwable nested-error)
                 (conj nested-error))
               (cond-> result data (conj data))))
      result)))

(defn- digest-file [^File file]
  (let [digest (MessageDigest/getInstance "SHA-256")
        buffer (byte-array 8192)]
    (with-open [input (FileInputStream. file)]
      (loop []
        (let [n (.read input buffer)]
          (when (pos? n)
            (.update digest buffer 0 n)
            (recur)))))
    (vec (.digest digest))))

(defn- file-fingerprint [root-path]
  (let [root-file (File. root-path)
        root-uri (.toURI root-file)]
    (into (sorted-map)
          (for [^File file (file-seq root-file)
                :when (.isFile file)]
            [(str (.relativize root-uri (.toURI file)))
             [(.length file) (digest-file file)]]))))

(defn- legacy-paths [kind]
  (case kind
    :scriptum [(path "primary-scriptum") (path "scriptum-index")]
    :proximum [(path "primary-proximum")
               (path "proximum-store")
               (path "proximum-release-mmap")]))

(defn- expect-legacy-refusal! [kind expected-type expected-reason]
  (let [paths (legacy-paths kind)
        before (mapv file-fingerprint paths)
        failure (try
                  (let [conn (d/connect (config kind))]
                    (d/release conn)
                    nil)
                  (catch Throwable failure failure))
        after (mapv file-fingerprint paths)]
    (assert failure (str "Current Datahike unexpectedly opened legacy " kind " root."))
    (assert (= before after)
            (str "Refusing legacy " kind " changed persisted files."))
    (let [data-chain (throwable-data failure)]
      (assert (some #(and (= expected-type (:type %))
                          (= expected-reason (:reason %)))
                    data-chain)
              (str "Unexpected failure chain: " (pr-str data-chain))))))

(defn- current-stratum! []
  (let [cfg (config :stratum)
        conn (d/connect cfg)]
    (try
      (verify-primary-count conn :item/price 2)
      (assert (= 2 (secondary/-estimate
                    (secondary-index conn :idx/price) {})))
      ;; A successful current transaction republishes the normalized explicit
      ;; envelope and proves that an old generation is a writable base.
      (d/transact conn [{:item/price 30}])
      (let [db (d/db conn)
            stored (konserve/get (:store db)
                                 (get-in db [:config :branch]) nil {:sync? true})
            root-map (get-in stored [:secondary-index-keys :idx/price])]
        (assert (= 1 (:format-version root-map)))
        (assert (= :datahike (:storage-owner root-map))))
      (finally
        (d/release conn))))
  (let [conn (d/connect (config :stratum))]
    (try
      (verify-primary-count conn :item/price 3)
      (assert (= 3 (secondary/-estimate
                    (secondary-index conn :idx/price) {})))
      (finally
        (d/release conn)))))

(defn verify-current [_]
  (trove/set-log-fn! (fn [& _]))
  (current-stratum!)
  (expect-legacy-refusal! :scriptum
                          :secondary/invalid-scriptum-generation
                          :legacy-scriptum-v1-generation)
  (expect-legacy-refusal! :proximum
                          :secondary/invalid-proximum-generation
                          :legacy-proximum-commit-root))
