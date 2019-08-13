# performance

Basic performance tests are run.

See `core` namespace for more information

## Usage

Make sure a `PostgreSQL` and `Datomic` instance is running, adjust configuration
accordingly. Build the project and run it locally for best performance results:

``` clojure
lein uberjar
java -jar -Xmx2g -server target/performance-0.1.0-SNAPSHOT-standalone.jar
```

Wait and see the results as csv in `./data` and as charts in `./plots`.
