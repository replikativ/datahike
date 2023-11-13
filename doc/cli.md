# Command line interface

*This is work in progress and subject to change.*

We provide the `dthk` native executable to access Datahike databases from
the command line.


# Example usage

First you need to download the precompiled binary, or build it yourself, and put
it on your executable path.

To access a database you need to provide the usual configuration for Datahike.
Put this into a file `myconfig.edn`.

```clojure
{:store  {:backend :file
          :path "/home/USERNAME/dh-shared-db"
          :config {:in-place? true}}
 :keep-history? true
 :schema-flexibility :read}
```

Now you can invoke some of our core API functions on the database. Let us add a
fact to the database (be careful to use single ' if you do not want your shell
to substitute parts of your Datalog ;) ):

```bash
$ dthk transact conn:myconfig.edn '[[:db/add -1 :name "Linus"]]'
 ```

And retrieve it:

```bash
$ dthk query '[:find ?n . :where [?e :name ?n]]' db:myconfig.edn
"Linus" # prints the name
```

Note that the `conn:<file>` argument to `transact` comes before the transaction
value(s), whereas the `db:<file>` argument to `query` comes after the query
value, mirroring the Clojure API. As an added benefit, this also allows passing
multiple db configuration files prefixed with `db:` for joining over arbitrary
many databases or data files with "edn:" or "json:". Everything non-prefixed is
read in as `edn` and passed to the query engine as well.


Provided the filestore is configured with `{:in-place? true}` you can even write
to the same database without a dedicated daemon from different shells:


```bash
$ dthk benchmark db:myconfig.edn 0 50000 100
"Elapsed time: 116335.589411 msecs"
```

Here we use a provided benchmark helper which transacts facts of the form `[eid
:name (random-team-member)]` for `eid=0,...,50000` into the store. `100` denotes
the batch size for each transaction, so here we chunk the 50000 facts into 500
transactions.

In a second shell you can now simultaneously add facts in a different range:

```bash
$ dthk benchmark db:myconfig.edn 50000 100000 100
```


To check that everything has been added and no write operations have overwritten
each other.


```bash
$ dthk query '[:find (count ?e) . :in $ :where [?e :name ?n]]' db:myconfig.edn
100000 # check :)
```

# Memory model

The persistent semantics of Datahike work more like `git` and less like similar
mutable databases such as SQLite or Datalevin. In particular you can always read
and retain snapshots (copies) of the database for free, no matter what else is
happening in the system. The current version is tested with memory and file
storage, but hopefully many other backends will also work with the
`native-image`.

In principle this shared memory access should even work while having a JVM
server, e.g. datahike-server, serving the same database. Note that all reads can
happen in parallel, only the writers experience congestion around exclusive file
locks here. This access pattern does not provide highest throughput, but is
extremely flexible and easy to start with.

## Forking and pulling

Forking is easy, it is enough to copy the folder of the store (even if the
database is currently being written to). The only thing you need to take care of
is to copy the DB root first and place it into the target directory last, it is
the file `0594e3b6-9635-5c99-8142-412accf3023b.ksv`. Then you can use e.g.
`rsync` (or `git`) to copy all other (immutable) files into your new folder. In
the end you copy the root file in there as well, making sure that all files it
is referencing are reachable. Note that this will ensure that you only copy new
data each time.

## Merging

Now here comes the cool part. You do not need anything more for merging than
Datalog itself. You can use a query like this to extract all new facts that are
in `db1` but not in `db2` like this:

```bash
dthk query '[:find ?e ?a ?v ?t :in $ $2 :where [$ ?e ?a ?v ?t] (not [$2 ?e ?a ?v ?t])]' db:config1.edn db:config2.edn
```

Since we cannot update transaction metadata, we should filter out
`:db/txInstant`s. We can also use a trick to add `:db/add` to each element in
the results, yielding valid transactions that we can then feed into `db2`.


```bash
dthk query '[:find ?db-add ?e ?a ?v ?t :in $ $2 ?db-add :where [$ ?e ?a ?v ?t] [(not= :db/txInstant ?a)] (not [$2 ?e ?a ?v ?t])]' db:config1.edn db:config2.edn ":db/add" | transact db:config2.edn
```

Note that this very simple strategy assumes that the entity ids that have been
added to `db1` do not overlap with potentially new ones added to `db2`. You can
encode conflict resolution strategies and id mappings with Datalog as well and
we are exploring several such strategies at the moment. This strategy is fairly
universal, as [CRDTs can be expressed in pure
Datalog](https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog).
While it is not the most efficient way to merge, we plan to provide fast paths
for common patterns in Datalog. Feel free to contact us if you are interested in
complex merging strategies or have related cool ideas.
