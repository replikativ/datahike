;; Write amplification of a one-row commit, default layout vs
;; :index-config {:temporal-superseded-only? true}.
;;
;; Modelled on the measurement that prompted the change: a file store with
;; :diff-buf-size 128 and :keep-history? true, loaded to ~40k datoms in batches,
;; then five single-row commits with the store's files diffed around each one.
;;
;;   clojure -M:test -m temporal-superseded-only-measure <out-dir>
(ns temporal-superseded-only-measure
  (:require [datahike.api :as d]
            [clojure.java.io :as io]))

(defn files [dir]
  (into {} (map (fn [f] [(.getName ^java.io.File f) (.length ^java.io.File f)]))
        (filter #(.isFile ^java.io.File %) (file-seq (io/file dir)))))

(def schema
  [{:db/ident :m/text :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :m/n :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])

(defn text [i] (apply str (take 260 (cycle (str "message " i " lorem ipsum dolor ")))))

(defn one-commit! [conn dir i]
  (let [before (files dir)
        _ (d/transact conn [{:m/n (+ 100000 i) :m/text (text i)}])
        after (files dir)
        new (into (sorted-map) (remove (fn [[n sz]] (= sz (before n)))) after)]
    {:objects (count new)
     :bytes (reduce + (vals new))
     :sizes (vec (sort > (vals new)))
     :rewritten (count (filter before (keys new)))}))

(defn run [root label extra-index-config]
  (let [dir (str root "/" label)
        cfg {:store {:backend :file :path dir :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :index-config (merge {:diff-buf-size 128} extra-index-config)}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema)
      (doseq [chunk (partition-all 500 (range 20000))]
        (d/transact conn (mapv (fn [i] {:m/n i :m/text (text i)}) chunk)))
      (let [db @conn
            loaded {:label label
                    :live-eavt (count (:eavt db))
                    :temporal-eavt (count (:temporal-eavt db))
                    :store-files (count (files dir))
                    :store-bytes (reduce + (vals (files dir)))}
            commits (mapv #(one-commit! conn dir %) (range 5))]
        (assoc loaded :commits commits))
      (finally (d/release conn)))))

(defn -main [& [root]]
  (let [root (or root "/tmp/dh-wa")
        a (run root "legacy" {})
        b (run root "superseded-only" {:temporal-superseded-only? true})]
    (doseq [r [a b]]
      (println "====" (:label r))
      (println "  live eavt datoms     " (:live-eavt r))
      (println "  temporal eavt datoms " (:temporal-eavt r))
      (println "  store files          " (:store-files r))
      (println "  store bytes          " (:store-bytes r))
      (doseq [c (:commits r)]
        (println "  one-row commit:" (select-keys c [:objects :bytes :rewritten])
                 "sizes" (:sizes c)))
      (let [cs (:commits r)]
        (println "  MEAN objects/commit" (double (/ (reduce + (map :objects cs)) (count cs)))
                 " MEAN bytes/commit" (double (/ (reduce + (map :bytes cs)) (count cs))))))
    (shutdown-agents)))
