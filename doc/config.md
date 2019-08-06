# Datahike database configuration

Starting with version `0.2.0` datahike supports features that can be be
configured based on the application's requirements. As of version `0.2.0`
datahike supports configuration for the name of the database, the storage 
engine, the schema flexibility, historical data, and the data index. 
Be aware: all these features can be set at database creation but not changed 
afterwards.

The configuration can be encoded within a base uri we can connect to. It has the
following scheme:

datahike:<storage-type>://<storage-config>?<db-config>

```clojure
(:require '[datahike.api :as d])
(def uri)
```


## storage engine

## schema flexibility

## 
