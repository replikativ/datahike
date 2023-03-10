# Schema Migration

Schema migration with Datahike is the evolution of your current schema into a future schema.

## Why using the schema-migration tool?
You could use the `transact`-fn of the api-ns to apply your schema, but with our
`norm`-ns you can define your migrations centrally and they will be applied once and only
once to your database.

This helps when your production database has limited accessibility from your developer
machines and you want to apply the migrations from a server next to your production code.
In case you are setting up your database from scratch for e.g. development purpose you can
rely on your schema to be up-to-date with your production environment because you are
keeping your original schema along with your migrations in a central repository.

## How to migrate a database schema
When we are speaking of changes to your schema, these should always add new definitions and
never change existing definitions. In case you want to change existing data to a new format
you will have to create a new schema and transact your existing data transformed again. A
good intro to this topic [can be found here](https://docs.datomic.com/cloud/schema/schema-change.html).

## Transaction-functions
Your transaction functions need to be on your classpath to be called and they need to take
one argument, the connection to your database. Each function needs to return a vector of
transactions so that they can be applied during migration.

Please be aware that with transaction-functions you will create transactions that need to be
held in memory. Very large migrations might exceed your memory.

## Norms?
Like [conformity for Datomic](https://github.com/avescodes/conformity) we are using the term
norm for our tool. You can use it to declare expectations about the state of your database
and enforce those idempotently without repeatedly transacting schema. These expectations
can be the form of your schema, data in a certain format or pre-transacted data for e.g.
a development database.

## Migration folder
Preferably create a folder in your project resources called `migrations`. You can however
use any folder you like even outside your resources. If you don't want to package the
migrations into a jar you can just run the migration-functions with a path as string passed.
In your migration-folder you store your migration-files. Be aware that your chosen
migration-folder will include all subfolders for reading the migrations. Don't store
other files in your migration-folder besides your migrations!

## How to migrate

1. Create a folder of your choice, for now let's call it `migrations`. In this folder you
create a new file with an edn-extension like `001-my-first-norm.edn`. Preferably you name the
file beginning with a number. Please be aware that the name of your file will be the id of
your migration. Taking into account that you might create some more migrations in the future
you should left-pad the names with zeros to keep a proper sorting. Keep in mind that your
migrations are transacted sorted after your chosen ids one after another. Spaces will be
replaced with dashes to compose the id.

2. Write the transactions itself into your newly created file. The content of the file needs
to be an edn-map with one or both of the keys `:tx-data` and `tx-fn`. `:tx-data` is just
transaction data in the form of a vector, `:tx-fn` is a function that you can run during the
execution to migrate data from one attribute to another for example. This function needs to
be qualified and callable from the classpath. It will be evaluated during the migration and
needs to return transactions. These transactions will be transacted with `:tx-data` together
in one transaction.

Example of a migration:
```clojure
{:tx-data [{:db/doc         "Place of occupation"
            :db/ident       :character/place-of-occupation
            :db/valueType   :db.type/string
            :db/cardinality :db.cardinality/one}]
 :tx-fn   my-transactions.my-project/my-first-tx-fn}
 ```

3. When you are sufficiently confident that your migrations will work you usually want to store
it in some kind of version control system. To avoid conflicts with fellow colleagues we
implemented a security net. Run the function `update-checksums!` from the `datahike.norm.norm`
namespace to create or update a `checksums.edn` file inside your migrations-folder. This file
contains the names and checksums of your migration-files. In case a colleague of yours
checked in a migration that you have not been aware of, your VCS should avoid merging the
conflicting `checksums.edn` file.

4. To apply your migrations you most likely want to package the migrations into a jar together
with datahike and a piece of code that actually runs your migrations and run it on a server.
You should check the correctness of the checksums with `datahike.norm.norm/verify-checksums`
and finally run the `datahike.norm.norm/ensure-norms!` function to apply your migrations. For
each migration that is already applied there will be a `:tx/norm` attribute stored with the
id of your migration so it will not be applied twice.
