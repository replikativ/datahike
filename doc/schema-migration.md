# Schema Migration or Norms

Schema migration with Datahike is the evolution of your current schema into a future schema.
We are calling the schema migrations 'norms' to avoid misunderstandings with a migration
from an older version of Datahike to a newer version.

When we are speaking of changes to your schema, these should always add new definitions and
never change existing definitions. In case you want to change existing data to a new format
you will have to create a new schema and transact your existing data transformed again. A
good intro to this topic [can be found here](https://docs.datomic.com/cloud/schema/schema-change.html).

## How to migrate

1. Create a folder of your choice, for now let's call it `migrations`. In this folder you
create a new file with an edn-extension like `001-my-first-norm.edn`. Preferably you name the
file beginning with a number. Please be aware that the name of your file will be the id of
your norm. Taking into account that you might create some more norms in the future
you should left-pad the names with zeros to keep a proper sorting. Keep in mind that your
norms are transacted sorted after your chosen ids one after another. Spaces will be replaced
with dashes to compose the id.

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

3. When you are sufficiently confident that your norm will work you usually want to store
it into some kind of version control system. To avoid conflicts with fellow colleagues we
implemented a security net. Run the function `update-checksums!` from the `datahike.norm.norm`
namespace to create or update a `checksums.edn` file inside your norms-folder. This file
contains the names and checksums of your migration-files. In case a colleague of yours
checked in a migration that you have not been aware of, your VCS should avoid merging the
conflicting `checksums.edn` file.

4. Run the `datahike.norm.norm/ensure-norms!` function to apply your norms. For each norm
that is already applied there will be a `:tx/norm` attribute stored with the id of your
norm so it will not be applied twice.

Be aware that your chosen norm-folder will include all subfolders for reading the norms.
Don't store other files in your norm-folder besides your norms!
