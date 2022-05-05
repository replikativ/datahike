(ns datahike.test.config
  (:require [clojure.test :refer :all]
            [datahike.index :refer [init-index]]
            [datahike.store :refer [default-config]]))

;; Can be set via kaocha config in tests.edn
;; e.g.
;; - '[{:backend :mem :index :datahike.index/persistent-set}]' to only run on in-memory database with persistent-set index
;; - '[{:backend :mem} {:index :datahike.index/persistent-set}]' to run all configs with either in-memory backend, or persistent-set index
(def ^:dynamic user-filter-maps [])

;; For now disables configurations the tests are not adjusted for
;; Should eventually not filter out anything
;; Configs can also be filtered on a test-to-test basis
(def system-config-filter
  (fn [config] (and (not (and (= :datahike.index/persistent-set (:index config))
                              (= :file (get-in config [:store :backend]))))
                    (= :attribute-refs? false)
                    (= :keep-history? false)
                    (= :schema-flexibility :write))))

(def configs
  (->> (for [index (keys (methods init-index))
             backend (keys (methods default-config))
             history? [true false]
             refs? [true false]
             schema [:read :write]]
         {:keep-history? history?
          :attribute-refs? refs?
          :schema-flexibility schema
          :index index
          :store (default-config {:backend backend})})
       (filter system-config-filter)
       (filter (fn [config] (some (fn [m] (every? #(fn [[k v]] (= (k config) v))
                                                  m))
                                  user-filter-maps)))))

(defn config-str [{:keys [keep-history? attribute-refs? schema-flexibility index store] :as _config}]
  (str "schema-on-" (name schema-flexibility)
       (when attribute-refs? " reference")
       " database with "
       (when keep-history? "history, ")
       (subs (name index) 15) " index, and "
       (:backend store) " backend"))
