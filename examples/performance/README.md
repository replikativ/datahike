# performance

Basic performance tests are run.

See `core` namespace for more information

## Usage

Make sure a `PostgreSQL` and `Datomic` instance is running, adjust configuration
accordingly. 

Set up databases using `docker-compose`:
``` bash
docker-compose up
```

You can clean up the containers with:
``` bash
docker-compose down --volumes
```


Build the project and run it locally for best performance results:

``` bash
lein uberjar
java -jar -Xmx2g -server target/performance-0.1.0-SNAPSHOT-standalone.jar
```

Wait and see the results as csv in `./data` and as charts in `./plots`.

## Execution Problems

connect.clj
- "Datomic Mem" cannot be measured since reconnection after db/release not possible

rand-query.clj
- "Datomic Mem" and "Datomic Free" cannot be measured
   - reason: java.lang.IllegalStateException: :db.error/connection-released The connection has been released.
- "LevelDB" cannot be measured since IOerror occcurs, saying "lock already held by different process" 


