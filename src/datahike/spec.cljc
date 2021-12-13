(ns datahike.spec
  ^:no-doc
  (:require [clojure.spec.alpha :as s])
  (:import [clojure.lang Atom]))

(s/def :tx/sync? boolean?)
(s/def :connection/config (s/keys :opt [:tx/sync?]))

(s/def :datahike/connection #(instance? Atom %))
