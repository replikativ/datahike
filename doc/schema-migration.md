# Schema Migration

Schema migration with Datahike is the evolution of your current schema into a future schema. When we are speaking of changes to your schema, these should always add new definitions and never change existing definitions. In case you want to change existing data to a new format you will have to create a new schema and transact your existing data transformed again.

## How to migrate

First create a folder of your choice, for now let's call it `migrations`. In this folder you create a new file with an edn-extension like `001-my-migration.edn`. Preferably you name the file beginning with a number. Please be aware that the name of your file will be the id of your migration. Taking into account that you might create some more migrations in the future you might want to left-pad the name with zeros to keep a proper sorting. Keep in mind that your migrations are transacted sorted after your chosen ids one after another.

Second write the transactions itself into your newly created file. The content of the file needs to be an edn-map with one or both of the keys `:tx-data` and `tx-fn`. `:tx-data` is just transaction data in the form of a vector of datoms, `:tx-fn` is a funtion that you can run during the migration to migrate data for example. This function needs to be from a loadable namespace and will be evaled during preparation and needs to return transactions. These will be transacted with `:tx-data` together in one transaction.

Example of a migration:
```clojure
{:tx-data [{:db/doc         "Place of occupation"
            :db/ident       :character/place-of-occupation
            :db/valueType   :db.type/string
            :db/cardinality :db.cardinality/one}]
 :tx-fn   datahike.norm.norm/neutral-fn}
 ```

Third, when you are sufficiently confident that your migration will work you usually will want to store it into some kind of version control system. To avoid conflicts with fellow colleagues we implemented a security net. Run the function `update-checksums!` from the `datahike.norm.norm` namespace to create or update a `checksums.edn` file. This file contains the names and checksums of your migration-files. In case a colleague of yours checked in a migration that you have not been aware of, your VCS should avoid merging the conflicting `checksums.edn` files.

Last, run the `datahike.norm.norm/ensure-norms!` function to run your migrations. For each migration that already ran there will be a `:tx/norm` attribute stored with the name of your migration so it doesn't have to run twice.

Be aware that your chosen migration-folder will include all subfolders for migrations. Don't store other things in your migration-folder than your migrations!
