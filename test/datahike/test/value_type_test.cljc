(ns datahike.test.value-type-test
  (:require
   #?(:cljs [cljs.test :refer-macros [deftest is]]
      :clj [clojure.test :refer [deftest is]])
   #?(:clj [datahike.api :as d])
   [datahike.datom :as datom]
   [datahike.value-type :as vt]))

(defrecord CustomBox [n])
(defrecord OtherCustomBox [n])

(defn- box-descriptor []
  {:id :example.type/box
   :type (type (->CustomBox 0))
   ;; Deliberately broad: runtime validation must still enforce exact :type.
   :valid? (constantly true)
   :compare (fn [left right] (compare (:n left) (:n right)))
   :wire {:name "example/box"
          :version 1
          :encode :n
          :decode (fn [version payload]
                    (when-not (= version 1)
                      (throw (ex-info "Unsupported Box wire version"
                                      {:version version})))
                    (->CustomBox payload))}})

(deftest registry-is-conflict-checked-and-dispatches-exactly
  (vt/reset-registry!)
  (try
    (let [descriptor (box-descriptor)]
      (is (= descriptor (vt/register! descriptor)))
      (is (= descriptor (vt/register! descriptor)) "equal registration is idempotent")
      (is (= descriptor (vt/descriptor :example.type/box)))
      (is (= descriptor (vt/descriptor-for-value (->CustomBox 1))))
      (is (nil? (vt/descriptor-for-value (->OtherCustomBox 1))) "no inheritance/predicate dispatch")
      (is (= descriptor (vt/descriptor-for-wire "example/box")))
      (is (= {:example.type/box descriptor} (vt/descriptors)))
      (is (= -1 (datom/compare-value (->CustomBox 1) (->CustomBox 2))))
      (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            #"wire-name already registered"
                            (vt/register! (-> (box-descriptor)
                                              (assoc :id :example.type/other-box
                                                     :type (type (->OtherCustomBox 0)))))))
      (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            #":compare must be callable"
                            (vt/register! (assoc descriptor :id :example.type/no-order
                                                 :type (type (->OtherCustomBox 0))
                                                 :compare nil))))
      (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            #"must not be a built-in Datahike runtime type"
                            (vt/register! (assoc descriptor
                                                 :id :example.type/string
                                                 :type (type "")
                                                 :wire (assoc (:wire descriptor)
                                                              :name "example/string")))))
      #?(:clj
         (is (thrown-with-msg? clojure.lang.ExceptionInfo
                               #":type must be an exact runtime type"
                               (vt/register! (assoc descriptor
                                                    :id :example.type/not-a-class
                                                    :type :not-a-class)))))
      (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            #"decoder returned an invalid value"
                            (vt/decode-value (assoc-in descriptor [:wire :decode]
                                                       (fn [_ _] (->OtherCustomBox 1)))
                                             1 1))))
    (finally
      (vt/reset-registry!))))

#?(:clj
   (deftest custom-type-schema-and-runtime-validation
     (vt/reset-registry!)
     (let [cfg {:store {:backend :memory
                        :id #uuid "5c100000-0000-0000-0000-00000000c001"}
                :schema-flexibility :write
                :attribute-refs? false}]
       (try
         (vt/register! (box-descriptor))
         (d/delete-database cfg)
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (try
             (d/transact conn [{:db/ident :box
                                :db/valueType :example.type/box
                                :db/cardinality :db.cardinality/one}])
             (d/transact conn [{:db/id 1 :box (->CustomBox 7)}])
             (is (= #{[(->CustomBox 7)]}
                    (d/q '[:find ?v :where [_ :box ?v]] (d/db conn))))
             (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                   #"registered custom value type :example.type/box"
                                   (d/transact conn [{:db/id 2 :box (->OtherCustomBox 7)}])))
             (finally
               (d/release conn))))
         (finally
           (d/delete-database cfg)
           (vt/reset-registry!))))))

#?(:clj
   (deftest schema-rejects-missing-registration-and-attribute-refs
     (doseq [[suffix register? attribute-refs? message]
             [["c002" false false #"Unregistered custom value type :example.type/box"]
              ["c003" true true #"not supported when :attribute-refs\? is true"]]]
       (vt/reset-registry!)
       (when register? (vt/register! (box-descriptor)))
       (let [cfg {:store {:backend :memory
                          :id (java.util.UUID/fromString
                               (str "5c100000-0000-0000-0000-00000000" suffix))}
                  :schema-flexibility :write
                  :attribute-refs? attribute-refs?}]
         (try
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (try
               (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                     message
                                     (d/transact conn [{:db/ident :box
                                                        :db/valueType :example.type/box
                                                        :db/cardinality :db.cardinality/one}])))
               (finally
                 (d/release conn))))
           (finally
             (d/delete-database cfg)
             (vt/reset-registry!)))))))

#?(:clj
   (deftest reconnect-requires-the-owning-extension
     (vt/reset-registry!)
     (let [cfg {:store {:backend :file
                        :path (str (System/getProperty "java.io.tmpdir")
                                   "/datahike-custom-type-" (System/nanoTime))
                        :id (java.util.UUID/randomUUID)}
                :schema-flexibility :write
                :attribute-refs? false
                :index-config {:branching-factor 4}}]
       (try
         (vt/register! (box-descriptor))
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (d/transact conn [{:db/ident :box
                              :db/valueType :example.type/box
                              :db/cardinality :db.cardinality/one
                              :db/index true}])
           (d/transact conn (mapv (fn [n] {:db/id n :box (->CustomBox n)})
                                  (range 1 65)))
           (d/release conn))
         (let [conn (d/connect cfg)]
           (is (= (vec (range 1 65))
                  (mapv (comp :n :v) (d/datoms (d/db conn) :avet :box))))
           (is (= #{[37]}
                  (d/q '[:find ?e :in $ ?v :where [?e :box ?v]]
                       (d/db conn) (->CustomBox 37))))
           (d/release conn))
         (vt/reset-registry!)
         (is (thrown-with-msg? clojure.lang.ExceptionInfo
                               #"Unregistered custom value type :example.type/box"
                               (d/connect cfg)))
         (finally
           (d/delete-database cfg)
           (vt/reset-registry!))))))
