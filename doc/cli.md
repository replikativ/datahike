# Command line interface

*This is work in progress and subject to change.*

We provide the `datahike` native executable to access Datahike databases from
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
fact to the database:

```bash
$ datahike transact db:myconfig.edn "[[:db/add -1 :name \"Linus\"]]"
 ```
 
And retrieve it:
 
```bash 
$ datahike query "[:find ?n . :where [?e :name ?n]]" db:myconfig.edn 
"Linus" # prints the name
```

By prefixing the path with `db:` to the query engine you can pass multiple db
configuration files and join over arbitrary many databases. Everything else is
read in as `edn` and passed to the query engine as well.


Provided the filestore is configured with `{:in-place? true}` you can even write
to the same database without a dedicated daemon from different shells,


```bash
$ datahike benchmark db:myconfig.edn 0 50000 100
"Elapsed time: 116335.589411 msecs"
```

Here we use a provided benchmark helper which transacts facts of the form `[eid
:name (random-team-member)]` for `eid=0,...,50000` into the store. `100` denotes
the batch size for each transaction, so here we chunk the 50000 facts into 500
transactions.

In a second shell you can now simultaneously add facts in a different range

```bash
$ datahike benchmark db:myconfig.edn 50000 100000 100
```


To check that everything has been added and no write operations have overwritten
each other.


```bash
$ datahike query "[:find (count ?e) . :in $ :where [?e :name ?n]]" db:myconfig.edn
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

