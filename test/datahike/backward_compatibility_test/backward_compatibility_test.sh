#!/usr/bin/env bash



# Run by being in the dir where this file is defined.


# Write to db using the latest released version of datahike
clojure -Sdeps '{:deps {io.replikativ/datahike {:mvn/version "RELEASE"}}}'  -X backward-test/write


# Read the db with the current datahike code living in this repository
clojure -Sdeps '{:deps {io.replikativ/datahike {:local/root "../../.."}}}'  -X backward-test/read
