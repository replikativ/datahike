# Garbage collection

**This is an experimental feature. Please try it out in test environment and provide feedback.**

Updates to a Datahike database leave behind tree fragments that can become not
reachable anymore from the new root node of each index tree (a good explanation
of how shared persistent tree data structures are updated can be found
[here](https://hypirion.com/musings/understanding-persistent-vector-pt-1)). As
soon as no other processes read from these old trees anymore they can be
considered garbage. To clean up this garbage as it accumulates over time you can
use the garbage collector. You can run it on db as follows

~~~clojure
(require '[datahike.experiemntal.gc :refer [gc!]]
         '[superv.async :refer [<?? S]])

(<?? S (gc! db))
~~~

This will garbage collect any branches you might have created and deleted by
now, but otherwise will not delete any old db values (snapshots) that are still
in the store. You will retrieve a set of all deleted entries, but you can also
just run the collector in the background, it requires no coordination and should
not slow down the transactor or other readers.

Next we assume that we do not need to keep any old data around much longer and
invalidate all readers that still access trees older than the current `db`.

~~~clojure
(let [now (java.util.Date.)]
  (gc! db now))
~~~

Note that even if data has not been written to in a long time erasing it can
invalidate newer snapshots that also refer to it, so you should pick a
conservative time window for garbage collection if you expect readers still
accessing old snapshots. This is intentional since Datahike provides open,
uncoordinated and scalable read access to the indices. The garbage collector
will make sure that any value that was accessible in the time window provided
will stay accessible.

The garbage collector is tested, but there might still problems, so please reach
out if you experience them!
