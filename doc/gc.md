# Garbage collection

**This is an experimental feature. Please try it out in a test environment and provide feedback.**

Datahike uses persistent data structures to update its memory. In a persistent
memory model a copy is efficiently created on each update to the database. A
good explanation of how shared persistent data structures share structure and
are updated can be found
[here](https://hypirion.com/musings/understanding-persistent-vector-pt-1). That
means that you can always access all old versions of Datahike databases and
Datahike provides a distinct set of historical query abilities, both in form of
its [historical indices](./time_variances.md) that support queries over the full
history and in form of its [git-like versioning functionality](./versioning.md)
for individual snapshots. A potential downside of the latter functionality is
that you need to keep all old versions of your database around and therefore
storage requirements grow with usage. To remove old versions of a Datahike
database you can apply garbage collection.

Provided no process reads anymore from a database it can be considered garbage.
To remove these versions you can use the garbage collector. You can run it on a
database `db` as follows

~~~clojure
(require '[datahike.experiemntal.gc :refer [gc!]]
         '[superv.async :refer [<?? S]])

(<?? S (gc! db))
~~~

This will garbage collect any branches you might have created and deleted by
now, but otherwise will not delete any old db values (snapshots) that are still
in the store. You will retrieve a set of all deleted storage blobs. You can just
run the collector concurrently in the background by removing the blocking
operator `<??`. It requires no coordination, operates on metadata only and
should not slow down the transactor or other readers.

Next let's assume that you do not want to keep any old data around much longer
and want to invalidate all readers that still access trees older than the
current `db`.

~~~clojure
(let [now (java.util.Date.)]
  (gc! db now))
~~~

Datahike provides open, uncoordinated and scalable read access to the indices
and therefore you need to be aware that there might be long running processes
that still need access to old versions and pick conservative grace periods in
such cases. The garbage collector will make sure that any value that was
accessible in the time window provided will stay accessible.

The garbage collector is tested, but there might still be problems, so please
reach out if you experience any!
