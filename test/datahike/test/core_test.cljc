(ns datahike.test.core-test
  (:require
   [clojure.core.async :as async :refer [go <!]]
   [hitchhiker.tree.utils.cljs.async :as ha]
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing async]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [clojure.string :as str]
   #?(:clj [kaocha.stacktrace])
   [datahike.core :as d]
   [datahike.impl.entity :as de]
   [datahike.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                           :clj  [:refer [defrecord-updatable]])]
   #?(:cljs [datahike.test.cljs])))

#?(:cljs
   (enable-console-print!))

;; Added special case for printing ex-data of ExceptionInfo
#?(:cljs
   (defmethod t/report [::t/default :error] [m]
     (t/inc-report-counter! :error)
     (println "\nERROR in" (t/testing-vars-str m))
     (when (seq (:testing-contexts (t/get-current-env)))
       (println (t/testing-contexts-str)))
     (when-let [message (:message m)] (println message))
     (println "expected:" (pr-str (:expected m)))
     (print "  actual: ")
     (let [actual (:actual m)]
       (cond
         (instance? ExceptionInfo actual)
         (println (.-stack actual) "\n" (pr-str (ex-data actual)))
         (instance? js/Error actual)
         (println (.-stack actual))
         :else
         (prn actual)))))

#?(:cljs (def test-summary (atom nil)))
#?(:cljs (defmethod t/report [::t/default :end-run-tests] [m]
           (reset! test-summary (dissoc m :type))))

(defn wrap-res [f]
  #?(:cljs (do (f) (clj->js @test-summary))
     :clj  (let [res (f)]
             (when (pos? (+ (:fail res) (:error res)))
               (System/exit 1)))))

;; utils
#?(:clj
   (defmethod t/assert-expr 'thrown-msg? [msg form]
     (let [[_ match & body] form]
       `(try ~@body
             (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
             (catch Throwable e#
               (let [m# (.getMessage e#)]
                 (if (= ~match m#)
                   (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                   (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
               e#)))))

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eavt)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

;; Filter Kaocha frames from exceptions

#?(:clj
   (alter-var-root #'kaocha.stacktrace/*stacktrace-filters* (constantly ["java." "clojure." "kaocha." "orchestra."])))

;; Core tests

#_#?(:cljs
     (deftest test-protocols
       (async done
              (async/go
                (let [schema {:aka {:db/cardinality :db.cardinality/many}}
                      db (async/<!
                          (d/db-with (async/<! (d/empty-db schema))
                                     [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                                      {:db/id 2 :name "Petr" :age 37 :huh? false}]))]
                  (is (= 2 2))
                  #_(is (async/<! (= (async/<! (d/empty-db schema))
                                     (async/<! (empty db)))))
                ;(is (= 6 (count db)))
                  #_(is (= (set (async/<! (seq db)))
                           #{(d/datom 1 :aka "IV")
                             (d/datom 1 :aka "Terrible")
                             (d/datom 1 :name "Ivan")
                             (d/datom 2 :age 37)
                             (d/datom 2 :name "Petr")
                             (d/datom 2 :huh? false)}))
                  (done))))))

(defn empty-db-helper []
  (+ 1 1)
  (db/empty-db)
  )

#?(:cljs (deftest compliance-test-cljs
           (db/empty-db)
           #_(async done
                  (go
                    (let [c (async/chan 1)
                          v #_(ha/<?) (empty-db-helper)]
                      (is (= 1 1))
                      (async/>! c 1)
                      (is (= 1 (ha/<? c)))
                      (done))))))



#_(defn with
  "Same as [[transact!]], but applies to an immutable database value. Returns transaction report (see [[transact!]])."
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
   {:pre [(db/db? db)]}
   (db/transact-tx-data (db/map->TxReport
                         {:db-before db
                          :db-after  db
                          :tx-data   []
                          :tempids   {}
                          :tx-meta   tx-meta}) tx-data)))




#_#?(:cljs (deftest sandbox
           (ha/go-try (let [bob-db (:db-after (ha/<? (with (ha/<? (db/empty-db)) [{:name "bob" :age 5}])))]))))


(comment
  ;; REPL code

  (def schema {:aka {:db/cardinality :db.cardinality/many}})


  (async/go
    (def db (async/<! (d/db-with (async/<! (d/empty-db schema))
                                 [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                                  {:db/id 2 :name "Petr" :age 37 :huh? false}]))))

  (async/go (println (async/<! (= (async/<! (d/empty-db schema))
                                  (async/<! (empty db))))))

  (async/go (println (async/<! (d/empty-db schema))))

  (async/go (println (async/<! (empty db))))

  (async/go (println (= (set (async/<! (seq db)))
                        #{(d/datom 1 :aka "IV")
                          (d/datom 1 :aka "Terrible")
                          (d/datom 1 :name "Ivan")
                          (d/datom 2 :age 37)
                          (d/datom 2 :name "Petr")
                          (d/datom 2 :huh? false)})))

  (async/go (println (async/<! (count db))))




  ;; format blocker
  )

