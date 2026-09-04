(ns datahike.http.stores
  "What a client may ask the server to create.

   `create-database` takes the client's configuration, `:store` included, so
   without a policy a client with `:create` may point the server at any
   backend it has loaded (a JDBC URL of its choosing, an S3 bucket, a file
   path anywhere the process may write). The server's `:create-database`
   option restricts that, optionally:

     {:create-database {:backends #{:memory :file}}}          ; only these backends
     {:create-database {:store {:backend :file
                                :path \"/var/lib/datahike/databases\"}}}  ; the server chooses

   With `:store`, the client's store is replaced: the server keeps the id
   the client chose (or picks one) and places the database under its own
   root, as the Kabel listener does. With `:backends`, the client's store is
   kept but its backend must be one of the set. Both may be given."
  (:require [clojure.java.io :as io]))

(defn validate-policy
  "The policy as given, or an exception for a shape the server cannot act on."
  [policy]
  (when (some? policy)
    (when-not (map? policy)
      (throw (ex-info ":create-database must be a map with :backends and/or :store"
                      {:type :datahike.http/invalid-config :create-database policy})))
    (let [{:keys [backends store]} policy]
      (when (and (some? backends)
                 (not (and (set? backends) (seq backends) (every? keyword? backends))))
        (throw (ex-info ":create-database :backends must be a nonempty set of backend keywords"
                        {:type :datahike.http/invalid-config :backends backends})))
      (when (some? store)
        (case (:backend store)
          :memory nil
          :file (when-not (and (string? (:path store)) (seq (:path store)))
                  (throw (ex-info ":create-database :store with :backend :file needs a nonblank :path"
                                  {:type :datahike.http/invalid-config :store store})))
          (throw (ex-info ":create-database :store must have :backend :memory or :file"
                          {:type :datahike.http/invalid-config :store store}))))
      policy)))

(defn- refuse [msg data]
  (throw (ex-info msg (assoc data :type :datahike.http/store-refused))))

(defn assign
  "The configuration a create proceeds with: the client's, with its `:store`
   checked against and, with a `:store` template, replaced by `policy`.
   Throws `:datahike.http/store-refused` for a store the policy does not
   allow. Without a policy the configuration is returned as is."
  [policy config]
  (if (nil? policy)
    config
    (let [{:keys [backends store]} policy
          requested (:store config)
          id (:id requested)]
      (when (and backends (not (contains? backends (:backend requested))))
        (refuse (str "This server creates databases only on " (pr-str backends)
                     ", not " (pr-str (:backend requested)))
                {:backend (:backend requested) :allowed backends}))
      (when (and (some? id) (not (uuid? id)))
        (refuse "A database id must be a uuid" {:id id}))
      (if store
        (let [id (or id (random-uuid))]
          (assoc config :store
                 (case (:backend store)
                   :memory {:backend :memory :id id}
                   :file   {:backend :file
                            :path (.getPath (io/file (:path store) (str id)))
                            :id id})))
        config))))
