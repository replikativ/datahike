(ns datahike.http.backends
  "Backend registrations bundled by the standalone HTTP server.

   Konserve dispatches by multimethod, so putting a backend jar on the
   classpath is not enough: its namespace must be loaded. Keeping those
   requires in one namespace makes the artifact's supported surface explicit
   and leaves the Datahike library jar free of optional backend dependencies."
  (:require [konserve-azure-blob.core]
            [konserve-dynamodb.core]
            [konserve-gcs.core]
            [konserve-jdbc.core]
            [konserve-redis.core]
            [konserve-s3.core]))
