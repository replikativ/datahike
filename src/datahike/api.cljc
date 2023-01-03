(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.connector :as dc]
            [clojure.spec.alpha :as s]
            [datahike.storing :as storing]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.spec :as spec]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.db :as db #?@(:cljs [:refer [HistoricalDB AsOfDB SinceDB FilteredDB]])]
            [datahike.db.interface :as dbi]
            [datahike.db.transaction :as dbt]
            [datahike.impl.entity :as de])
  #?(:clj
     (:import [clojure.lang Keyword PersistentArrayMap]
              [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity])))

(s/fdef
  connect
  :args (s/alt :config (s/cat :config spec/SConfig)
               :nil (s/cat))
  :ret spec/SConnectionAtom)
(def
  ^{:arglists '([] [config])
    :doc "Connects to a datahike database via configuration map. For more information on the configuration refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md).

          The configuration for a connection is a subset of the Datahike configuration with only the store necessary: `:store`.

          `:store` defines the backend configuration as hash-map with mandatory key: `:backend` and store dependent keys.

          Per default Datahike ships with `:mem` and `:file` backend.

          The default configuration:
           `{:store {:backend :mem :id \"default\"}}`

          Usage:

          Connect to default in-memory configuration:
           `(connect)`

          Connect to a database with persistent store:
           `(connect {:store {:backend :file :path \"/tmp/example\"}})`"}

  connect dc/connect)

(s/fdef
  database-exists?
  :args (s/alt :config (s/cat :config spec/SConfig)
               :nil (s/cat))
  :ret boolean?)
(def
  ^{:arglists '([] [config])
    :doc "Checks if a database exists via configuration map.
          Usage:

              (database-exists? {:store {:backend :mem :id \"example\"}})"}
  database-exists? storing/database-exists?)

(s/fdef
  create-database
  :args (s/alt :config (s/cat :config spec/SConfig
                              :initial-tx (s/? (s/cat :k (s/? (s/and #(= % :initial-tx))) :v spec/STransactions))
                              :temporal-index (s/? (s/cat :k (s/? (s/and #(= % :temporal-index))) :v boolean?))
                              :schema-on-read (s/? (s/cat :k (s/? (s/and #(= % :schema-on-read))) :v boolean?)))
               :nil (s/cat))
  :ret nil?)
(def
  ^{:arglists '([] [config & deprecated-opts])
    :doc "Creates a database via configuration map. For more information on the configuration refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md).

          The configuration is a hash-map with keys: `:store`, `:initial-tx`, `:keep-history?`, `:schema-flexibility`, `:index`

          - `:store` defines the backend configuration as hash-map with mandatory key: `:backend` and store dependent keys.
            Per default Datahike ships with `:mem` and `:file` backend.
          - `:initial-tx` defines the first transaction into the database, often setting default data like the schema.
          - `:keep-history?` is a boolean that toggles whether Datahike keeps historical data.
          - `:schema-flexibility` can be set to either `:read` or `:write` setting the validation method for the data.
            - `:read` validates the data when your read data from the database, `:write` validates the data when you transact new data.
          - `:index` defines the data type of the index. Available are `:datahike.index/hitchhiker-tree`, `:datahike.index/persistent-set` (only available with in-memory storage)
          - `:name` defines your database name optionally, if not set, a random name is created
          - `:transactor` optionally configures a transactor as a hash map. If not set, the default local transactor is used.

          Default configuration has in-memory store, keeps history with write schema flexibility, and has no initial transaction:
          {:store {:backend :mem :id \"default\"} :keep-history? true :schema-flexibility :write}

          Usage:

              ;; create an empty database:
              (create-database {:store {:backend :mem :id \"example\"} :name \"my-favourite-database\"})

              ;; Datahike has a strict schema validation (schema-flexibility `:write`) policy by default, that only allows transaction of data that has been pre-defined by a schema.
              ;; You may influence this behaviour using the `:schema-flexibility` attribute:
              (create-database {:store {:backend :mem :id \"example\"} :schema-flexibility :read})

              ;; By storing historical data in a separate index, datahike has the capability of querying data from any point in time.
              ;; You may control this feature using the `:keep-history?` attribute:
              (create-database {:store {:backend :mem :id \"example\"} :keep-history? false})

              ;; Initial data after creation may be added using the `:initial-tx` attribute, which in this example adds a schema:
              (create-database {:store {:backend :mem :id \"example\"} :initial-tx [{:db/ident :name :db/valueType :db.type/string :db.cardinality/one}]})"}

  create-database
  (fn [& args]
    (let [config (apply storing/create-database args)]
      (when-let [txs (:initial-tx config)]
        (let [conn (dc/connect config)]
          (dc/transact conn txs)
          (dc/release conn))))))

(s/fdef
  delete-database
  :args (s/alt :config (s/cat :config spec/SConfig)
               :nil (s/cat))
  :ret any?)
(def ^{:arglists '([] [config])
       :doc      "Deletes a database given via configuration map. Storage configuration `:store` is mandatory.
                  For more information refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md)"}
  delete-database
  storing/delete-database)

(s/fdef
  transact
  :args (s/cat :conn spec/SConnectionAtom :txs spec/STransactions)
  :ret spec/STransactionReport)
(def ^{:arglists '([conn arg-map])
       :doc      "Applies transaction to the underlying database value and atomically updates the connection reference to point to the result of that transaction, the new db value.

                  Accepts the connection and a map or a vector as argument, specifying the transaction data.

                  Returns transaction report, a map:

                      {:db-before ...       ; db value before transaction
                       :db-after  ...       ; db value after transaction
                       :tx-data   [...]     ; plain datoms that were added/retracted from db-before
                       :tempids   {...}     ; map of tempid from tx-data => assigned entid in db-after
                       :tx-meta   tx-meta } ; the exact value you passed as `tx-meta`

                  Note! `conn` will be updated in-place and is not returned from [[transact]].

                  Usage:

                      ;; add a single datom to an existing entity (1)
                      (transact conn [[:db/add 1 :name \"Ivan\"]])

                      ;; retract a single datom
                      (transact conn [[:db/retract 1 :name \"Ivan\"]])

                      ;; retract single entity attribute
                      (transact conn [[:db.fn/retractAttribute 1 :name]])

                      ;; retract all entity attributes (effectively deletes entity)
                      (transact conn [[:db.fn/retractEntity 1]])

                      ;; create a new entity (`-1`, as any other negative value, is a tempid
                      ;; that will be replaced by Datahike with the next unused eid)
                      (transact conn [[:db/add -1 :name \"Ivan\"]])

                      ;; check assigned id (here `*1` is a result returned from previous `transact` call)
                      (def report *1)
                      (:tempids report) ; => {-1 296, :db/current-tx 536870913}

                      ;; check actual datoms inserted
                      (:tx-data report) ; => [#datahike/Datom [296 :name \"Ivan\" 536870913]]

                      ;; tempid can also be a string
                      (transact conn [[:db/add \"ivan\" :name \"Ivan\"]])
                      (:tempids *1) ; => {\"ivan\" 5, :db/current-tx 536870920}

                      ;; reference another entity (must exist)
                      (transact conn [[:db/add -1 :friend 296]])

                      ;; create an entity and set multiple attributes (in a single transaction
                      ;; equal tempids will be replaced with the same unused yet entid)
                      (transact conn [[:db/add -1 :name \"Ivan\"]
                                      [:db/add -1 :likes \"fries\"]
                                      [:db/add -1 :likes \"pizza\"]
                                      [:db/add -1 :friend 296]])

                      ;; create an entity and set multiple attributes (alternative map form)
                      (transact conn [{:db/id  -1
                                       :name   \"Ivan\"
                                       :likes  [\"fries\" \"pizza\"]
                                       :friend 296}])

                      ;; update an entity (alternative map form). Can’t retract attributes in
                      ;; map form. For cardinality many attrs, value (fish in this example)
                      ;; will be added to the list of existing values
                      (transact conn [{:db/id  296
                                       :name   \"Oleg\"
                                       :likes  [\"fish\"]}])

                      ;; ref attributes can be specified as nested map, that will create a nested entity as well
                      (transact conn [{:db/id  -1
                                       :name   \"Oleg\"
                                       :friend {:db/id -2
                                       :name \"Sergey\"}}])

                      ;; schema is needed for using a reverse attribute
                      (is (transact conn [{:db/valueType :db.type/ref
                                           :db/cardinality :db.cardinality/one
                                           :db/ident :friend}]))

                      ;; reverse attribute name can be used if you want a created entity to become
                      ;; a value in another entity reference
                      (transact conn [{:db/id  -1
                                       :name   \"Oleg\"
                                       :_friend 296}])
                      ;; equivalent to
                      (transact conn [[:db/add  -1 :name   \"Oleg\"]
                                      {:db/add 296 :friend -1]])"}
  transact
  dc/transact)

(s/fdef
  transact!
  :args (s/cat :conn spec/SConnectionAtom :txs spec/STransactions)
  :ret #(s/valid? spec/STransactionReport @%))
(def ^{:arglists '([conn tx-data tx-meta])
       :no-doc    true}
  transact!
  dc/transact!)

(s/fdef
  load-entities
  :args (s/cat :conn spec/SConnectionAtom :txs spec/STransactions)
  :ret #(s/valid? spec/STransactionReport @%)) ; This returns a throwable promise, so we have to dereference it..
(def ^{:arglists '([conn tx-data])
       :doc "Load entities directly"}
  load-entities
  dc/load-entities)

(s/fdef
  release
  :args (s/cat :conn spec/SConnectionAtom)
  :ret nil?)
(def ^{:arglists '([conn])
       :doc      "Releases a database connection"}
  release dc/release)

(s/fdef
  pull
  :args (s/alt :simple (s/cat :db spec/SDB :opts spec/SPullOptions)
               :full (s/cat :db spec/SDB :selector coll? :eid spec/SEId))
  :ret (s/nilable map?))
(def ^{:arglists '([db selector eid] [db arg-map])
       :doc      "Fetches data from database using recursive declarative description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).

                  Unlike [[entity]], returns plain Clojure map (not lazy).

                  Usage:

                      (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1) ; => {:db/id   1,
                                                                                           :name    \"Ivan\"
                                                                                           :likes   [:pizza]
                                                                                           :friends [{:db/id 2, :name \"Oleg\"}]}

                  The arity-2 version takes :selector and :eid in arg-map."}
  pull dp/pull)

(s/fdef
  pull-many
  :args (s/alt :simple (s/cat :db spec/SDB :opts spec/SPullOptions)
               :full (s/cat :db spec/SDB :selector coll? :eid spec/SEId))
  :ret (s/coll-of map?))
(def ^{:arglists '([db selector eids])
       :doc      "Same as [[pull]], but accepts sequence of ids and returns sequence of maps.

                  Usage:

                      (pull-many db [:db/id :name] [1 2]) ; => [{:db/id 1, :name \"Ivan\"}
                                                                {:db/id 2, :name \"Oleg\"}]"}
  pull-many dp/pull-many)

(s/fdef
  q
  :args (s/alt :argmap (s/cat :map spec/SQueryArgs)
               :with-params (s/cat :q (s/or :vec vector? :map map?) :args (s/* any?))) ;; TODO: the doc could show more examples with varargs
  :ret any?)
(def ^{:arglists '([query & args] [arg-map])
       :doc "Executes a datalog query. See [docs.datomic.com/on-prem/query.html](https://docs.datomic.com/on-prem/query.html).

             Usage:

             Query as parameter with additional args:

                 (q '[:find ?value
                      :where [_ :likes ?value]]
                    #{[1 :likes \"fries\"]
                      [2 :likes \"candy\"]
                      [3 :likes \"pie\"]
                      [4 :likes \"pizza\"]}) ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}

             Or query passed in arg-map:

                 (q {:query '[:find ?value
                              :where [_ :likes ?value]]
                     :offset 2
                     :limit 1
                     :args [#{[1 :likes \"fries\"]
                              [2 :likes \"candy\"]
                              [3 :likes \"pie\"]
                              [4 :likes \"pizza\"]}]}) ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}

             Or query passed as map of vectors:

                 (q '{:find [?value] :where [[_ :likes ?value]]}
                    #{[1 :likes \"fries\"]
                      [2 :likes \"candy\"]
                      [3 :likes \"pie\"]
                      [4 :likes \"pizza\"]}) ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}

             Or query passed as string:

                 (q {:query \"[:find ?value :where [_ :likes ?value]]\"
                     :args [#{[1 :likes \"fries\"]
                              [2 :likes \"candy\"]
                              [3 :likes \"pie\"]
                              [4 :likes \"pizza\"]}]})

             Query passed as map needs vectors as values. Query can not be passed as list. The 1-arity function takes a map with the arguments :query and :args and optionally the additional keys :offset and :limit."}
  q dq/q)

(s/fdef
  query-stats
  :args (s/alt :argmap (s/cat :map spec/SQueryArgs)
               :with-params (s/cat :q (s/or :vec vector? :map map?) :args (s/* any?))) ;; TODO: the doc could show more examples with varargs
  :ret map?)
(def ^{:arglists '([query & args] [arg-map])
       :doc "Executes a datalog query and returns the result as well as some execution details.
             Uses the same arguments as q does."}
  query-stats dq/query-stats)

(s/fdef
  datoms
  :args (s/alt :map (s/cat :db spec/SDB :args spec/SIndexLookupArgs)
               :key (s/cat :db spec/SDB :index keyword? :components (s/alt :coll (s/* any?)
                                                                           :nil nil?)))
  :ret (s/nilable spec/SDatoms))
(defmulti datoms {:arglists '([db arg-map] [db index & components])
                  :doc "Index lookup. Returns a sequence of datoms (lazy iterator over actual DB index) which components
                        (e, a, v) match passed arguments. Datoms are sorted in index sort order. Possible `index` values
                        are: `:eavt`, `:aevt`, `:avet`.

                        Accepts db and a map as arguments with the keys `:index` and `:components` provided within the
                        map, or the arguments provided separately.


                        Usage:

                        Set up your database. Beware that for the `:avet` index the index needs to be set to true for
                        the attribute `:likes`.

                            (d/transact db [{:db/ident :name
                                             :db/type :db.type/string
                                             :db/cardinality :db.cardinality/one}
                                            {:db/ident :likes
                                             :db/type :db.type/string
                                             :db/index true
                                             :db/cardinality :db.cardinality/many}
                                            {:db/ident :friends
                                             :db/type :db.type/ref
                                             :db/cardinality :db.cardinality/many}]

                            (d/transact db [{:db/id 4 :name \"Ivan\"
                                            {:db/id 4 :likes \"fries\"
                                            {:db/id 4 :likes \"pizza\"}
                                            {:db/id 4 :friends 5}])

                            (d/transact db [{:db/id 5 :name \"Oleg\"}
                                            {:db/id 5 :likes \"candy\"}
                                            {:db/id 5 :likes \"pie\"}
                                            {:db/id 5 :likes \"pizza\"}])

                        Find all datoms for entity id == 1 (any attrs and values) sort by attribute, then value

                            (datoms @db {:index :eavt
                                         :components [1]}) ; => (#datahike/Datom [1 :friends 2]
                                                           ;     #datahike/Datom [1 :likes \"fries\"]
                                                           ;     #datahike/Datom [1 :likes \"pizza\"]
                                                           ;     #datahike/Datom [1 :name \"Ivan\"])

                        Find all datoms for entity id == 1 and attribute == :likes (any values) sorted by value

                            (datoms @db {:index :eavt
                                         :components [1 :likes]}) ; => (#datahike/Datom [1 :likes \"fries\"]
                                                                  ;     #datahike/Datom [1 :likes \"pizza\"])

                        Find all datoms for entity id == 1, attribute == :likes and value == \"pizza\"

                            (datoms @db {:index :eavt
                                         :components [1 :likes \"pizza\"]}) ; => (#datahike/Datom [1 :likes \"pizza\"])

                        Find all datoms for attribute == :likes (any entity ids and values) sorted by entity id, then value

                            (datoms @db {:index :aevt
                                         :components [:likes]}) ; => (#datahike/Datom [1 :likes \"fries\"]
                                                                ;     #datahike/Datom [1 :likes \"pizza\"]
                                                                ;     #datahike/Datom [2 :likes \"candy\"]
                                                                ;     #datahike/Datom [2 :likes \"pie\"]
                                                                ;     #datahike/Datom [2 :likes \"pizza\"])

                        Find all datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
                        `:likes` must be a unique attr, reference or marked as `:db/index true`

                            (datoms @db {:index :avet
                                         :components [:likes \"pizza\"]}) ; => (#datahike/Datom [1 :likes \"pizza\"]
                                                                          ;     #datahike/Datom [2 :likes \"pizza\"])

                        Find all datoms sorted by entity id, then attribute, then value

                            (datoms @db {:index :eavt}) ; => (...)


                        Useful patterns:

                        Get all values of :db.cardinality/many attribute

                            (->> (datoms @db {:index :eavt
                                              :components [eid attr]})
                                 (map :v))

                        Lookup entity ids by attribute value

                            (->> (datoms @db {:index :avet
                                              :components [attr value]})
                                 (map :e))

                        Find all entities with a specific attribute

                            (->> (datoms @db {:index :aevt
                                              :components [attr]})
                                 (map :e))

                        Find “singleton” entity by its attr

                            (->> (datoms @db {:index :aevt
                                              :components [attr]})
                                 first
                                 :e)

                        Find N entities with lowest attr value (e.g. 10 earliest posts)

                            (->> (datoms @db {:index :avet
                                              :components [attr]})
                                 (take N))

                        Find N entities with highest attr value (e.g. 10 latest posts)

                            (->> (datoms @db {:index :avet
                                              :components [attr]})
                                 (reverse)
                                 (take N))


                        Gotchas:

                        - Index lookup is usually more efficient than doing a query with a single clause.
                        - Resulting iterator is calculated in constant time and small constant memory overhead.
                        - Iterator supports efficient `first`, `next`, `reverse`, `seq` and is itself a sequence.
                        - Will not return datoms that are not part of the index (e.g. attributes with no `:db/index` in schema when querying `:avet` index).
                          - `:eavt` and `:aevt` contain all datoms.
                          - `:avet` only contains datoms for references, `:db/unique` and `:db/index` attributes."}
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/-datoms db index components))

(defmethod datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/-datoms db index [])
    (dbi/-datoms db index components)))

(s/fdef
  seek-atoms
  :args (s/alt :map (s/cat :db spec/SDB :args spec/SIndexLookupArgs)
               :key (s/cat :db spec/SDB :index keyword? :components (s/* any?)))
  :ret (s/nilable spec/SDatoms))
(defmulti seek-datoms {:arglists '([db arg-map] [db index & components])
                       :doc "Similar to [[datoms]], but will return datoms starting from specified components and including rest of the database until the end of the index.

                             If no datom matches passed arguments exactly, iterator will start from first datom that could be considered “greater” in index order.

                             Usage:

                                 (seek-datoms @db {:index :eavt
                                                   :components [1]}) ; => (#datahike/Datom [1 :friends 2]
                                                                     ;     #datahike/Datom [1 :likes \"fries\"]
                                                                     ;     #datahike/Datom [1 :likes \"pizza\"]
                                                                     ;     #datahike/Datom [1 :name \"Ivan\"]
                                                                     ;     #datahike/Datom [2 :likes \"candy\"]
                                                                     ;     #datahike/Datom [2 :likes \"pie\"]
                                                                     ;     #datahike/Datom [2 :likes \"pizza\"])

                                 (seek-datoms @db {:index :eavt
                                                   :components [1 :name]}) ; => (#datahike/Datom [1 :name \"Ivan\"]
                                                                           ;     #datahike/Datom [2 :likes \"candy\"]
                                                                           ;     #datahike/Datom [2 :likes \"pie\"]
                                                                           ;     #datahike/Datom [2 :likes \"pizza\"])

                                 (seek-datoms @db {:index :eavt
                                                   :components [2]}) ; => (#datahike/Datom [2 :likes \"candy\"]
                                                                     ;     #datahike/Datom [2 :likes \"pie\"]
                                                                     ;     #datahike/Datom [2 :likes \"pizza\"])

                             No datom `[2 :likes \"fish\"]`, so starts with one immediately following such in index

                                 (seek-datoms @db {:index :eavt
                                                   :components [2 :likes \"fish\"]}) ; => (#datahike/Datom [2 :likes \"pie\"]
                                                                                     ;     #datahike/Datom [2 :likes \"pizza\"])"}
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod seek-datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/-seek-datoms db index components))

(defmethod seek-datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/-seek-datoms db index [])
    (dbi/-seek-datoms db index components)))

(s/fdef
  tempid
  :args (s/alt :part any?
               :full (s/cat :part any? :x int?))
  :ret neg-int?)
(def ^{:arglists '([part] [part x])
       :doc "Allocates and returns a unique temporary id (a negative integer). Ignores `part`. Returns `x` if it is specified.

             Exists for Datomic API compatibility. Prefer using negative integers directly if possible."}
  tempid
  dcore/tempid)

(s/fdef
  entity
  :args (s/cat :db spec/SDB :eid (s/alt :eid spec/SEId :div any?))
  :ret (s/nilable de/entity?))
(def ^{:arglists '([db eid])
       :doc      "Retrieves an entity by its id from database. Entities are lazy map-like structures to navigate Datahike database content.

                  For `eid` pass entity id or lookup attr:

                      (entity db 1)
                      (entity db [:unique-attr :value])

                  If entity does not exist, `nil` is returned:

                      (entity db -1) ; => nil

                  Creating an entity by id is very cheap, almost no-op, as attr access is on-demand:

                      (entity db 1) ; => {:db/id 1}

                  Entity attributes can be lazily accessed through key lookups:

                      (:attr (entity db 1)) ; => :value
                      (get (entity db 1) :attr) ; => :value

                  Cardinality many attributes are returned sequences:

                      (:attrs (entity db 1)) ; => [:v1 :v2 :v3]

                  Reference attributes are returned as another entities:

                      (:ref (entity db 1)) ; => {:db/id 2}
                      (:ns/ref (entity db 1)) ; => {:db/id 2}

                  References can be walked backwards by prepending `_` to name part of an attribute:

                      (:_ref (entity db 2)) ; => [{:db/id 1}]
                      (:ns/_ref (entity db 2)) ; => [{:db/id 1}]

                  Reverse reference lookup returns sequence of entities unless attribute is marked as `:db/component`:

                      (:_component-ref (entity db 2)) ; => {:db/id 1}

                  Entity gotchas:

                  - Entities print as map, but are not exactly maps (they have compatible get interface though).
                  - Entities are effectively immutable “views” into a particular version of a database.
                  - Entities retain reference to the whole database.
                  - You can't change database through entities, only read.
                  - Creating an entity by id is very cheap, almost no-op (attributes are looked up on demand).
                  - Comparing entities just compares their ids. Be careful when comparing entities taken from different dbs or from different versions of the same db.
                  - Accessed entity attributes are cached on entity itself (except backward references).
                  - When printing, only cached attributes (the ones you have accessed before) are printed. See [[touch]]."}
  entity de/entity)

(s/fdef
  entity-db
  :args (s/cat :entity de/entity?)
  :ret spec/SDB)
(defn entity-db
  "Returns a db that entity was created from."
  [^Entity entity]
  (.-db entity))

(s/fdef
  is-filtered
  :args (s/cat :db spec/SDB)
  :ret boolean?)
(defn is-filtered
  "Returns `true` if this database was filtered using [[filter]], `false` otherwise."
  [db]
  (instance? FilteredDB db))

(s/fdef
  filter
  :args (s/cat :db spec/SDB :pred any?)
  :ret #(is-filtered %))
(def ^{:arglists '([db pred])
       :doc "Returns a view over database that has same interface but only includes datoms for which the `(pred db datom)` is true. Can be applied multiple times.

             Filtered DB gotchas:

             - All operations on filtered database are proxied to original DB, then filter pred is applied.
             - Not cached. You pay filter penalty every time.
             - Supports entities, pull, queries, index access.
             - Does not support hashing of DB.
             - Does not support [[with]] and [[db-with]]."}
  filter
  dcore/filter)

(s/fdef
  with
  :args (s/alt :with-map (s/cat :db spec/SDB :argmap spec/SWithArgs)
               :with-data (s/cat :db spec/SDB :tx-data spec/STransactions)
               :with-meta (s/cat :db spec/SDB :tx-data spec/STransactions :tx-meta spec/STxMeta))
  :ret spec/STransactionReport)
(def ^{:arglists '([db arg-map])
       :doc "Same as [[transact]]`, but applies to an immutable database value. Returns transaction report (see [[transact]]).

             Accepts tx-data and tx-meta as a map.

                 (with @conn {:tx-data [[:db/add 1 :name \"Ivan\"]]}) ; => {:db-before #datahike/DB {:max-tx 536870912 :max-eid 0},
                                                                      ;     :db-after #datahike/DB {:max-tx 536870913 :max-eid 1},
                                                                      ;     :tx-data [#datahike/Datom [1 :name \"Ivan\" 536870913]],
                                                                      ;     :tempids #:db{:current-tx 536870913},
                                                                      ;     :tx-meta nil}

                 (with @conn {:tx-data [[:db/add 1 :name \"Ivan\"]]
                              :tx-meta {:foo :bar}}) ; => {:db-before #datahike/DB {:max-tx 536870912 :max-eid 0},
                                                     ;     :db-after #datahike/DB {:max-tx 536870913 :max-eid 1},
                                                     ;     :tx-data [#datahike/Datom [1 :name \"Ivan\" 536870913]],
                                                     ;     :tempids #:db{:current-tx 536870913},
                                                     ;     :tx-meta {:foo :bar}}"}
  with
  (fn
    ([db arg-map]
     (let [tx-data (if (:tx-data arg-map) (:tx-data arg-map) arg-map)
           tx-meta (if (:tx-meta arg-map) (:tx-meta arg-map) nil)]
       (with db tx-data tx-meta)))
    ([db tx-data tx-meta]
     (if (is-filtered db)
       (dt/raise "Filtered DB cannot be modified" {:error :transaction/filtered})
       (dbt/transact-tx-data (db/map->TxReport
                              {:db-before db
                               :db-after  db
                               :tx-data   []
                               :tempids   {}
                               :tx-meta   tx-meta}) tx-data)))))

(s/fdef
  db-with
  :args (s/cat :db spec/SDB :tx-data spec/STransactions)
  :ret spec/SDB)
(def ^{:arglists '([db tx-data])
       :doc "Applies transaction to an immutable db value, returning new immutable db value. Same as `(:db-after (with db tx-data))`."}
  db-with
  (fn [db tx-data]
    (:db-after (with db tx-data))))

(s/fdef
  db
  :args (s/cat :conn spec/SConnectionAtom)
  :ret spec/SDB)
(defn db
  "Returns the underlying immutable database value from a connection.

   Exists for Datomic API compatibility. Prefer using `@conn` directly if possible."
  [conn]
  @conn)

(s/fdef
  since
  :args (s/cat :db spec/SDB :time-point spec/time-point?)
  :ret spec/SDB)
(def ^{:arglists '([db time-point])
       :doc "Returns the database state since a given point in time (you may use either java.util.Date or a transaction ID as long).
             Be aware: the database contains only the datoms that were added since the date.


                 (transact conn {:tx-data [{:db/ident :name
                                            :db/valueType :db.type/string
                                            :db/unique :db.unique/identity
                                            :db/index true
                                            :db/cardinality :db.cardinality/one}
                                           {:db/ident :age
                                            :db/valueType :db.type/long
                                            :db/cardinality :db.cardinality/one}]})

                 (transact conn {:tx-data [{:name \"Alice\" :age 25} {:name \"Bob\" :age 30}]})

                 (def date (java.util.Date.))

                 (transact conn [{:db/id [:name \"Alice\"] :age 30}])

                 (q '[:find ?n ?a
                      :in $ $since
                      :where
                      [$ ?e :name ?n]
                      [$since ?e :age ?a]]
                      @conn
                      (since @conn date)) ; => #{[\"Alice\" 30]}

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                     :args [@conn]}) ; => #{[\"Alice\" 30] [\"Bob\" 30]}"}
  since
  (fn [db time-point]
    (if (dbi/-temporal-index? db)
      (SinceDB. db time-point)
      (dt/raise "since is only allowed on temporal indexed databases." {:config (dbi/-config db)}))))

(s/fdef
  as-of
  :args (s/cat :db spec/SDB :time-point spec/time-point?)
  :ret spec/SDB)
(def ^{:arglists '([db time-point])
       :doc "Returns the database state at given point in time (you may use either java.util.Date or transaction ID as long).


                 (transact conn {:tx-data [{:db/ident :name
                                            :db/valueType :db.type/string
                                            :db/unique :db.unique/identity
                                            :db/index true
                                            :db/cardinality :db.cardinality/one}
                                           {:db/ident :age
                                            :db/valueType :db.type/long
                                            :db/cardinality :db.cardinality/one}]})

                 (transact conn {:tx-data [{:name \"Alice\" :age 25} {:name \"Bob\" :age 30}]})

                 (def date (java.util.Date.))

                 (transact conn {:tx-data [{:db/id [:name \"Alice\"] :age 35}]})

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                     :args [(as-of @conn date)]}) ; => #{[\"Alice\" 25] [\"Bob\" 30]}

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                     :args [@conn]}) ; => #{[\"Alice\" 35] [\"Bob\" 30]}"}
  as-of
  (fn [db time-point]
    (if (dbi/-temporal-index? db)
      (if (int? time-point)
        (if (<= const/tx0 time-point)
          (AsOfDB. db time-point)
          (dt/raise (str "Invalid transaction ID. Must be bigger than " const/tx0 ".")
                    {:time-point time-point}))
        (AsOfDB. db time-point))
      (dt/raise "as-of is only allowed on temporal indexed databases." {:config (dbi/-config db)}))))

(s/fdef
  history
  :args (s/cat :db spec/SDB)
  :ret coll?)
(def ^{:arglists '([db])
       :doc "Returns the full historical state of the database you may interact with.


                 (transact conn {:tx-data [{:db/ident :name
                                            :db/valueType :db.type/string
                                            :db/unique :db.unique/identity
                                            :db/index true
                                            :db/cardinality :db.cardinality/one}
                                           {:db/ident :age
                                            :db/valueType :db.type/long
                                            :db/cardinality :db.cardinality/one}]})

                 (transact conn {:tx-data [{:name \"Alice\" :age 25} {:name \"Bob\" :age 30}]})

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                     :args [(history @conn)]}) ; => #{[\"Alice\" 25] [\"Bob\" 30]}

                 (transact conn {:tx-data [{:db/id [:name \"Alice\"] :age 35}]})

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                     :args [@conn]}) ; => #{[\"Alice\" 35] [\"Bob\" 30]}

                 (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                       :args [(history @conn)]}) ; => #{[\"Alice\" 25] [\"Bob\" 30]}"}
  history
  (fn [db]
    (if (dbi/-temporal-index? db)
      (HistoricalDB. db)
      (dt/raise "history is only allowed on temporal indexed databases." {:config (dbi/-config db)}))))

(s/fdef
  index-range
  :args (s/cat :db spec/SDB :args spec/SIndexRangeArgs)
  :ret spec/SDatoms)
(def ^{:arglists '([db arg-map])
       :doc "Returns part of `:avet` index between `[_ attr start]` and `[_ attr end]` in AVET sort order.

             Same properties as [[datoms]].

             `attr` must be a reference, unique attribute or marked as `:db/index true`.

             Usage:


                 (transact db {:tx-data [{:db/ident :name
                                          :db/type :db.type/string
                                          :db/cardinality :db.cardinality/one}
                                         {:db/ident :likes
                                          :db/index true
                                          :db/type :db.type/string
                                          :db/cardinality :db.cardinality/many}
                                         {:db/ident :age
                                          :db/unique :db.unique/identity
                                          :db/type :db.type/ref
                                          :db/cardinality :db.cardinality/many}]})

                 (transact db {:tx-data [{:name \"Ivan\"}
                                         {:age 19}
                                         {:likes \"fries\"}
                                         {:likes \"pizza\"}
                                         {:likes \"candy\"}
                                         {:likes \"pie\"}
                                         {:likes \"pizza\"}]})

                 (index-range db {:attrid :likes
                                  :start  \"a\"
                                  :end    \"zzzzzzzzz\"}) ; => '(#datahike/Datom [2 :likes \"candy\"]
                                                          ;      #datahike/Datom [1 :likes \"fries\"]
                                                          ;      #datahike/Datom [2 :likes \"pie\"]
                                                          ;      #datahike/Datom [1 :likes \"pizza\"]
                                                          ;      #datahike/Datom [2 :likes \"pizza\"])

                 (index-range db {:attrid :likes
                                  :start  \"egg\"
                                  :end    \"pineapple\"}) ; => '(#datahike/Datom [1 :likes \"fries\"]
                                                          ;      #datahike/Datom [2 :likes \"pie\"])

             Useful patterns:

                 ; find all entities with age in a specific range (inclusive)
                 (->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))"}
  index-range
  (fn [db {:keys [attrid start end]}]
    (dbi/-index-range db attrid start end)))

(s/fdef
  listen
  :args (s/alt :no-key (s/cat :conn spec/SConnectionAtom :callback fn?)
               :with-key (s/cat :conn spec/SConnectionAtom :key any? :callback fn?))
  :ret any?
  :fn #(if (= :with-key (-> % :args first))
         (= (:ret %) (-> % :args second :key))
         true))
(def ^{:arglists '([conn callback] [conn key callback])
       :doc "Listen for changes on the given connection. Whenever a transaction is applied to the database via
             [[transact]], the callback is called with the transaction report. `key` is any opaque unique value.

             Idempotent. Calling [[listen]] with the same twice will override old callback with the new value.

             Returns the key under which this listener is registered. See also [[unlisten]]."}
  listen
  dcore/listen!)

(s/fdef
  unlisten
  :args (s/cat :conn spec/SConnectionAtom :key any?)
  :ret map?)
(def ^{:arglists '([conn key])
       :doc "Removes registered listener from connection. See also [[listen]]."}
  unlisten
  dcore/unlisten!)

(s/fdef
  schema
  :args (s/cat :db spec/SDB)
  :ret spec/SSchema)
(defn ^{:arglists '([db])
        :doc "Returns current schema definition."}
  schema
  [db]
  (reduce-kv
   (fn [m k v]
     (cond
       (and (keyword? k)
            (not (or (ds/entity-spec-attr? k)
                     (ds/schema-attr? k)
                     (ds/sys-ident? k)))) (update m k #(merge % v))
       (number? k) (update m v #(merge % {:db/id k}))
       :else m))
   {}
   (dbi/-schema db)))

(s/fdef
  reverse-schema
  :args (s/cat :db spec/SDB)
  :ret map?)
(defn ^{:arglists '([db])
        :doc "Returns current reverse schema definition."}
  reverse-schema
  [db]
  (reduce-kv
   (fn [m k v]
     (let [attrs (->> v
                      (remove #(or (ds/entity-spec-attr? %)
                                   (ds/sys-ident? %)
                                   (ds/schema-attr? %)))
                      (into #{}))]
       (if (empty? attrs)
         m
         (assoc m k attrs))))
   {}
   (dbi/-rschema db)))

(s/fdef
  metrics
  :args (s/cat :db spec/SDB)
  :ret spec/SMetrics)
(defn ^{:arglists '([db])
        :doc "Returns database metrics"}
  metrics
  [db]
  (db/metrics db))

(defn ^{:arglists '([])
        :doc "Loads default config for the current environment"}
  load-config
  []
  (config/load-config))
