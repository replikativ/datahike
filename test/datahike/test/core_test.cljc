(ns datahike.test.core-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj [clojure.test :as t :refer [is deftest]])
   [clojure.walk]
   [clojure.data]
   #?(:clj [kaocha.stacktrace])
   [datahike.core :as dc]
   [datahike.constants :as const]
   [datahike.impl.entity :as de]
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
     :clj (let [res (f)]
            (when (pos? (+ (:fail res) (:error res)))
              (System/exit 1)))))

(defn entity-map [db e]
  (when-let [entity (dc/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (dc/datoms db :eavt)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

;; Filter Kaocha frames from exceptions

#?(:clj
   (alter-var-root #'kaocha.stacktrace/*stacktrace-filters* (constantly ["java." "clojure." "kaocha." "orchestra."])))

;; Core tests

(deftest test-protocols
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db (dc/db-with (dc/empty-db schema)
                       [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                        {:db/id 2 :name "Petr" :age 37 :huh? false}])]
    (is (= (dc/empty-db schema)
           (empty db)))
    (is (= 6 (count db)))
    (is (= (set (seq db))
           #{(dc/datom 1 :aka "IV")
             (dc/datom 1 :aka "Terrible")
             (dc/datom 1 :name "Ivan")
             (dc/datom 2 :age 37)
             (dc/datom 2 :name "Petr")
             (dc/datom 2 :huh? false)}))))

(defn- now []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ now-ms 1000))]
    (is (= (* 1000 now) (dc/squuid-time-millis (dc/squuid))))
    (is (not= (dc/squuid) (dc/squuid)))
    (is (= (subs (str (dc/squuid)) 0 8)
           (subs (str (dc/squuid)) 0 8)))))

(deftest test-diff
  (is (= [[(dc/datom 1 :b 2) (dc/datom 1 :c 4) (dc/datom 2 :a 1)]
          [(dc/datom 1 :b 3) (dc/datom 1 :d 5)]
          [(dc/datom 1 :a 1)]]
         (clojure.data/diff
          (dc/db-with (dc/empty-db) [{:a 1 :b 2 :c 4} {:a 1}])
          (dc/db-with (dc/empty-db) [{:a 1 :b 3 :d 5}])))))

