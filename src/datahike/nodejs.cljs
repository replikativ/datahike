(ns datahike.nodejs
  (:require [konserve.node-filestore :as fs]
            [environ.core :refer [env]]
            [zufall.core :refer [rand-german-mammal]]
            [datahike.tools :as dt]
            [clojure.spec.alpha :as s]
            [datahike.store :refer [store-identity
                                    empty-store
                                    delete-store
                                    connect-store
                                    default-config
                                    config-spec]]))

;; file
(defmethod store-identity :file [config]
  [:file (:scope config) (:path config)])

(defmethod empty-store :file [{:keys [path config opts]}]
  (fs/connect-fs-store path :opts (or opts {:sync? true}) :config config))

(defmethod delete-store :file [{:keys [path]}]
  (fs/delete-store path))

(defmethod connect-store :file [{:keys [path config opts]}]
  (fs/connect-fs-store path :opts (or opts {:sync? true}) :config config))

(defn- get-working-dir []
  ((.-cwd js/process)))

(defmethod default-config :file [config]
  (merge
   {:path  (:datahike-store-path env (str (get-working-dir) "/datahike-db-" (rand-german-mammal)))
    :scope (dt/get-hostname)}
   config))

(s/def ::path string?)
(s/def ::file (s/keys :req-un [::path]))

(defmethod config-spec :file [_] ::file)
