# Versioning

**This is an experimental feature. Please try it out in test environment and provide feedback.**

Since Datahike has a persistent memory model it can be used similarly to
[git](https://git-scm.com/). While joining databases with different underlying
stores is the most general way to combine data in Datahike and should be
preferred for separate data sets, in cases where you want to evolve a single
database the structural sharing of our indices has unique advantages. Git is
efficient and fast because it does not need to copy shared data on each
operation. So in cases where you want to evolve a database with new data, but
don't want to write it directly into the main database, you can `branch!` and
evolve a copy of the database that behaves like the main branch under `:db`.
After you have evolved the database you can decide what data to retain and then
`merge!` it back. You can also take any in-memory DB value and dump it into a
durable branch with `force-branch!`. To inspect the write history use
`branch-history`.

You can see the following example as an example,

~~~clojure
(require '[superv.async :refer [<?? S]]
         '[datahike.api :as d]
         '[datahike.experimental.versioning :refer [branch! branch-history delete-branch! force-branch! merge!]])

(let [cfg    {:store              {:backend :file
                                   :path    "/tmp/dh-versioning-test"}
              :keep-history?      true
              :schema-flexibility :write
              :index              :datahike.index/persistent-set}
      conn   (do
              (d/delete-database cfg)
              (d/create-database cfg)
              (d/connect cfg))
      schema [{:db/ident       :age
               :db/cardinality :db.cardinality/one
               :db/valueType   :db.type/long}]
      _      (d/transact conn schema)
      store  (:store @conn)]
  (branch! conn :db :foo) ;; new branch :foo, does not create new commit, just copies
  (let [foo-conn (d/connect (assoc cfg :branch :foo))] ;; connect to it
    (d/transact foo-conn [{:age 42}]) ;; transact some data
    ;; extracted data from foo by query
    ;; ...
    ;; and decide to merge it into :db
    (merge! conn #{:foo} [{:age 42}]))
  (count (<?? S (branch-history conn))) ;; => 4 commits now on both branches
  (force-branch! @conn :foo2 #{:foo}) ;; put whatever DB value you have created in memory
  (delete-branch! conn :foo))
~~~

Here we create a database as usual, but then we create a branch `:foo`, write to
it and then merge it back. A simple query to extract all data in transactable
form that is in a `db1` but not `db2` is

~~~clojure
(d/q [:find ?db-add ?e ?a ?v ?t
      :in $ $2 ?db-add
      :where
      [$ ?e ?a ?v ?t]
      [(not= :db/txInstant ?a)]
      (not [$2 ?e ?a ?v ?t])]
      db1 db2 :db/add)
~~~

but you might want to be more selective when creating the data for `merge!`. We
are very interested in what you are interested to do with this functionality, so
please reach out if you have problems or ideas!
