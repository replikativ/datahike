(ns datahike.test.core
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   #?(:clj [kaocha.stacktrace])
   [datahike.core :as d]
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

;; Valid entity ids for adding datoms

(def ^:const e1 (inc d/e0))
(def ^:const e2 (inc e1))
(def ^:const e3 (inc e2))
(def ^:const e4 (inc e3))
(def ^:const e5 (inc e4))
(def ^:const e6 (inc e5))
(def ^:const e7 (inc e6))
(def ^:const e8 (inc e7))
(def ^:const e9 (inc e8))
(def ^:const e10 (inc e9))
(def ^:const e11 (inc e10))
(def ^:const e12 (inc e11))
(def ^:const e13 (inc e12))
(def ^:const e14 (inc e13))
(def ^:const e15 (inc e14))
(def ^:const e16 (inc e15))
(def ^:const e17 (inc e16))
(def ^:const e18 (inc e17))

;; Core tests

#_(deftest test-protocols                                   ;; TODO: fix
    (let [schema {:aka {:db/cardinality :db.cardinality/many}}
          db (d/db-with (d/empty-db schema)
                        [{:db/id e1 :name "Ivan" :aka ["IV" "Terrible"]}
                         {:db/id e2 :name "Petr" :age 37 :huh? false}])]
      (println "schema" (.-schema db))
      (is (= (d/empty-db schema)
             (empty db)))
      (is (= 6 (count db)))
      (is (= (set (seq db))
             #{(d/datom e1 :aka "IV")
               (d/datom e1 :aka "Terrible")
               (d/datom e1 :name "Ivan")
               (d/datom e2 :age 37)
               (d/datom e2 :name "Petr")
               (d/datom e2 :huh? false)}))))
