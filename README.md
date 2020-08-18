# datahike-fdb

This is a preview release of [Datahike](https://github.com/replikativ/datahike) with [FoundationDB](https://www.foundationdb.org) as data storage.

## Installation

[Install FoundationDB](https://www.foundationdb.org/download/) for your system.

## Usage


```clojure
(require '[datahike.core :as d])

;; Create an new db
(def db (d/empty-db))

;; Populate the database
(d/db-with db [ { :db/id 1, :name  "Ivan", :age   15 }
                            { :db/id 2, :name  "Petr", :age   37 }
                            { :db/id 3, :name  "Ivan", :age   37 }
                                { :db/id 4, :age 15 }])

;; Search the data
(d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                         [?e :age ?v]] db)
=> #{[1 15] [3 37]}                         




## Limitations

- datahike-fdb is not an immutable store, therefore all history related Datahike APIs will not work.
- Java 8 only.

## License

Copyright © 2014–2020 Chrislain Razafimahefa, Konrad Kühne, Christian Weilbach, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
