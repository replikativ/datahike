# Command line interface

**Status: Stable** - The CLI is production-ready but may receive updates and improvements.

We provide the `dthk` native executable to access Datahike databases from
the command line.

## Installation

1. Download the precompiled binary for your platform from the [Datahike releases page](https://github.com/replikativ/datahike/releases)
2. Unzip the archive
3. Add the `dthk` executable to your PATH

Supported platforms:
- Linux (amd64, aarch64)
- macOS (aarch64/Apple Silicon)

For other platforms, build from source using GraalVM native-image (see project documentation).

## Commands

The CLI provides hierarchical commands organized by category. All commands mirror the Clojure API functions. To see all available commands, run:

```bash
$ dthk --help
```

Key command categories:
- **Database Operations**: `db create-database`, `db delete-database`, `db exists`
- **Connection Operations**: `conn connect`
- **Transaction Operations**: `tx transact`, `tx load-entities`, `tx db-with`, `tx with`
- **Query Operations**: `q`, `pull`, `pull-many`, `entity`, `datoms`, `seek-datoms`, `index-range`, `is-filtered`, `query-stats`
- **Schema Operations**: `schema schema`, `schema reverse-schema`
- **Diagnostics**: `metrics`
- **Maintenance**: `gc-storage`

Commands use prefix syntax for database access:
- `db:config.edn` - Dereferences a connection to get the current database value
- `conn:config.edn` - Creates a connection for transacting
- `edn:file.edn` - Reads EDN data from a file
- `json:file.json` - Reads JSON data from a file
- `asof:timestamp:config.edn` - Creates a database snapshot as-of timestamp
- `since:timestamp:config.edn` - Creates a database view since timestamp
- `history:config.edn` - Returns the history database

## Example usage

To access a database you need to provide the usual configuration for Datahike.
Put this into a file `myconfig.edn`.

```clojure
{:store  {:backend :file
          :path "/home/USERNAME/dh-shared-db"
          :id #uuid "550e8400-e29b-41d4-a716-446655440000"
          :config {:in-place? true}}
 :keep-history? true
 :schema-flexibility :read}
```

Now you can invoke some of our core API functions on the database. Let us add a
fact to the database (be careful to use single ' if you do not want your shell
to substitute parts of your Datalog ;) ):

```bash
$ dthk tx transact conn:myconfig.edn '[[:db/add -1 :name "Linus"]]'
 ```

And retrieve it:

```bash
$ dthk q '[:find ?n . :where [?e :name ?n]]' db:myconfig.edn
"Linus" # prints the name
```

Note that the `conn:<file>` argument to `tx transact` comes before the transaction
value(s), whereas the `db:<file>` argument to `q` comes after the query
value, mirroring the Clojure API. As an added benefit, this also allows passing
multiple db configuration files prefixed with `db:` for joining over arbitrary
many databases or data files with "edn:" or "json:". Everything non-prefixed is
read in as `edn` and passed to the query engine as well:

```bash
$ dthk q '[:find ?e . :in $ ?name :where [?e :name ?name]]' db:myconfig.edn '"Linus"'
123
```

When passing strings as EDN, make sure to enclose double quotes as part of the
command-line arg value. Otherwise it will be parsed as a symbol.

Provided the filestore is configured with `{:in-place? true}` you can even write
to the same database without a dedicated daemon from different shells:

```bash
# In the first shell
$ dthk tx transact conn:myconfig.edn '[[:db/add -1 :name "Alice"]]'

# In a second shell simultaneously
$ dthk tx transact conn:myconfig.edn '[[:db/add -2 :name "Bob"]]'
```

To check that everything has been added and no write operations have overwritten
each other:

```bash
$ dthk q '[:find (count ?e) . :in $ :where [?e :name ?n]]' db:myconfig.edn
2 # check :)
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
is to copy the DB root first and place it into the target directory last. The root
file is a konserve internal storage file with a UUID name like `0594e3b6-9635-5c99-8142-412accf3023b.ksv`
(the actual UUID will match your database's `:id` configuration). Then you can use e.g.
`rsync` (or `git`) to copy all other (immutable) files into your new folder. In
the end you copy the root file in there as well, making sure that all files it
is referencing are reachable. Note that this will ensure that you only copy new
data each time.

## Merging

Now here comes the cool part. You do not need anything more for merging than
Datalog itself. You can use a query like this to extract all new facts that are
in `db1` but not in `db2` like this:

```bash
dthk q '[:find ?e ?a ?v ?t :in $ $2 :where [$ ?e ?a ?v ?t] (not [$2 ?e ?a ?v ?t])]' db:config1.edn db:config2.edn
```

Since we cannot update transaction metadata, we should filter out
`:db/txInstant`s. We can also use a trick to add `:db/add` to each element in
the results, yielding valid transactions that we can then feed into `db2`.


```bash
dthk q '[:find ?db-add ?e ?a ?v ?t :in $ $2 ?db-add :where [$ ?e ?a ?v ?t] [(not= :db/txInstant ?a)] (not [$2 ?e ?a ?v ?t])]' db:config1.edn db:config2.edn ":db/add" | dthk tx transact db:config2.edn
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
