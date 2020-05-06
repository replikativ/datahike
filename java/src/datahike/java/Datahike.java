package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import clojure.lang.RT;

import java.util.*;

import static datahike.java.Util.deref;

/**
 * Defines methods for interacting with a Datahike database.
 */
public class Datahike {
    /**
     * Imports the Clojure 'datahike.api' namespace.
     */
    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.api"));
    }

    private static final IFn deleteDatabaseFn = Clojure.var("datahike.api", "delete-database");
    private static final IFn createDatabaseFn = Clojure.var("datahike.api", "create-database");
    private static final IFn connectFn = Clojure.var("datahike.api", "connect");
    private static final IFn transactFn = Clojure.var("datahike.api", "transact");
    private static final IFn dbFn = Clojure.var("datahike.api", "db");
    private static final IFn qFn = Clojure.var("datahike.api", "q");
    private static final IFn historyFn = Clojure.var("datahike.api", "history");
    private static final IFn asOfFn = Clojure.var("datahike.api", "as-of");
    private static final IFn sinceFn = Clojure.var("datahike.api", "since");
    private static final IFn pullFn = Clojure.var("datahike.api", "pull");
    private static final IFn releaseFn = Clojure.var("datahike.api", "release");
    private static final IFn pullManyFn = Clojure.var("datahike.api", "pull-many");
    private static final IFn seekDatomsFn = Clojure.var("datahike.api", "seek-datoms");
    private static final IFn tempIdFn = Clojure.var("datahike.api", "tempid");
    private static final IFn entityFn = Clojure.var("datahike.api", "entity");
    private static final IFn entityDbFn = Clojure.var("datahike.api", "entity-db");
    private static final IFn isFilteredFn = Clojure.var("datahike.api", "is-filtered");
    private static final IFn filterFn = Clojure.var("datahike.api", "filter");
    private static final IFn withFn = Clojure.var("datahike.api", "with");
    private static final IFn dbWithFn = Clojure.var("datahike.api", "db-with");
    private static final IFn databaseExistsFn = Clojure.var("datahike.api", "database-exists?");

    /**
     * Forbids instances creation.
     */
    private Datahike() {}

    /**
     * Returns the current state of the database.
     *
     * @param conn a connection to the database.
     * @return a dereferenced version of the database.
     */
    public static Object dConn(Object conn) {
        return deref.invoke(conn);
    };

    /**
     * Returns true if a database exists for the given map configuration or the given uri.
     *
     * @param uri_or_map an uri or a configuration map.
     * @return true if database exists
     */
    public static boolean databaseExists(Object uri_or_map)  {
        return (boolean) databaseExistsFn.invoke(uri_or_map);
    };

    /**
     * Deletes a database.
     *
     * @param uri the database uri.
     */
    public static void deleteDatabase(String uri) {
        deleteDatabaseFn.invoke(uri);
    }

    /**
     * Creates a database at the given 'uri'.
     *
     * @param uri the database uri.
     */
    public static void createDatabase(String uri) {
        createDatabaseFn.invoke(uri);
    }

    /**
     * Creates a database at the given 'uri' and parameterizes its creation through argument 'args'.
     *
     * @param uri
     * the location of the database creation
     *
     * @param args
     * If providing initial data to transact  pass the keyword 'k(":initial-tx")' as the first argument followed by the data. Data could for instance be a schema as shown in the following example:
     * Datahike.createDatabase(uri, k(":initial-tx"), schema);
     *
     * Use keyword 'k(":schema-on-read")' to define whether the database uses a schema or not. By default, the database requires a schema. Example:
     * Datahike.createDatabase(uri, k(":schema-on-read"), false);
     *
     * With keyword k(':temporal-index') at true you can query the data from any point in time. To not use this feature, write:
     * Datahike.createDatabase(uri, k(":temporal-index"), false);
     *
     */
    public static void createDatabase(String uri, Object... args) {
        List argsCopy = new ArrayList(Arrays.asList(args));
        argsCopy.add(0, uri);
        createDatabaseFn.applyTo(RT.seq(argsCopy));
    }

    /**
     * Connects to the Datahike database given by 'uri'.
     *
     * Example: {@code conn = Datahike.connect("datahike:file:///Users/paul/temp/datahike-db");}
     *
     * @param uri the database uri
     * @return a connection to the database.
     */
    public static Object connect(String uri) {
        return connectFn.invoke(uri);
    }

    /**
     * Returns the current state of the database.
     *
     * @param conn the connection to the database
     * @return the latest state of the database
     */
    public static Object db(Object conn) {
        return dbFn.invoke(conn);
    }

    /**
     * Executes a query.
     *
     * @param query A string of the query expressed in Clojure syntax.
     * @param inputs the arguments to the query, such as the database(s) and/or additional inputs.
     * @return the result of the query
     */
    public static Set<PersistentVector> q(String query, Object... inputs) {
        List argsCopy = new ArrayList(Arrays.asList(inputs));
        argsCopy.add(0, Clojure.read(query));
        return (Set<PersistentVector>)qFn.applyTo(RT.seq(argsCopy));
    }

    /**
     * Transacts data.
     *
     * @param conn the connection to the database.
     * @param txData the data to be transacted.
     * @return a transaction report.
     */
    public static Object transact(Object conn, PersistentVector txData) {
        return transactFn.invoke(conn, txData);
    }

    /**
     * Returns the full historical state of a database.
     *
     * @param dConn a database
     * @return the full history of the database
     */
    public static Object history(Object dConn) { return historyFn.invoke(dConn); };

    /**
     * Returns the database state at the given date.
     *
     * @param dConn the database
     * @param date either java.util.Date or Epoch Time as long
     * @return the database state at the given date
     */
    public static Object asOf(Object dConn, Date date) {
        return asOfFn.invoke(dConn, date);
    }

    /**
     * Returns the state of the database since the given date.
     * Be aware: the database contains only the datoms that were added since the date.
     *
     * @param dConn the database
     * @param date either java.util.Date or Epoch Time as long
     * @return  the state of the database since the given date.
     */
    public static Object since(Object dConn, Date date) {
        return sinceFn.invoke(dConn, date);
    }

    /**
     * Fetches data from database using recursive declarative description.
     * @see <a href="https://docs.datomic.com/on-prem/pull.html">docs.datomic.com/on-prem/pull.html</a>
     *
     * @param dConn the database
     * @param selector the criteria for the pull query
     * @param eid the entity id
     * @return the result of the query as map
     */
    public static Map pull(Object dConn, String selector, int eid) {
        return (Map) pullFn.invoke(dConn, Clojure.read(selector), eid);
    }

    /**
     * Fetches data from database using recursive declarative description.
     * @see <a href="https://docs.datomic.com/on-prem/pull.html">docs.datomic.com/on-prem/pull.html</a>
     *
     * @param dConn the database
     * @param selector the criteria for the pull query
     * @param eid an entity id
     * @return the result of the query as a map
     */
    public static Map pull(Object dConn, String selector, PersistentVector eid) {
        return (Map) pullFn.invoke(dConn, Clojure.read(selector), eid);
    }

    /**
     * Same as pull but takes multiple entities as input.
     *
     * @param dConn the database
     * @param selector the criteria for the pull query
     * @param eids a vector of entity ids
     * @return the result of the query as a list of maps
     */
    public static List pullMany(Object dConn, String selector, PersistentVector eids) {
        return (List) pullManyFn.invoke(dConn, Clojure.read(selector), eids);
    }

    /**
     * Releases the connection to the database.
     *
     * @param conn the connection to the database.
     */
    public static void release(Object conn) {
        releaseFn.invoke(conn);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param dConn the database
     * @param index a keyword describing the type of index. E.g. {@code k(":eavt")}
     * @param c1 the first element of a datom used as the criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object dConn, Keyword index, Object c1) {
        return (List)seekDatomsFn.invoke(dConn, index, c1);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param dConn the database
     * @param index a keyword describing the type of index. E.g. {@code k(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object dConn, Keyword index, Object c1, Object c2) {
        return (List)seekDatomsFn.invoke(dConn, index, c1, c2);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param dConn the database
     * @param index a keyword describing the type of index. E.g. {@code k(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @param c3 the third element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object dConn, Keyword index, Object c1, Object c2, Object c3) {
        return (List)seekDatomsFn.invoke(dConn, index, c1, c2, c3);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param dConn the database
     * @param index a keyword describing the type of index. E.g. {@code k(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @param c3 the third element of a datom used as criteria for filtering
     * @param c4 the fourth element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object dConn, Keyword index, Object c1, Object c2, Object c3, Object c4) {
        return (List)seekDatomsFn.invoke(dConn, index, c1, c2, c3, c4);
    }


    /**
     * Allocates and returns an unique temporary id (a negative integer). Ignores the keyword argument `k`.
     * Exists for Datomic API compatibility. Prefer using negative integers directly if possible."
     *
     * @param k an ignored argument
     * @return a negative integer
     */
    public static Long tempId(Keyword k) {
        return (Long) tempIdFn.invoke(k);
    }

    /**
     * Allocates and returns an unique temporary id (a negative integer). Ignores the keyword argument `k`.
     * Returns the argument `i`.
     * Exists for Datomic API compatibility. Prefer using negative integers directly if possible."
     *
     * @param k an ignored argument
     * @param i an integer
     * @return 'i'
     */
    public static Long tempId(Keyword k, Long i) {
        return (Long)tempIdFn.invoke(k, i);
    }

    /**
     * Returns the entity corresponding to the entity id 'eid'.
     *
     * @param dConn the database
     * @param eid an entity id
     * @return an entity
     */
    public static IEntity entity(Object dConn, int eid) {
        return (IEntity)entityFn.invoke(dConn, eid);
    }

    /**
     * Returns the db that created entity 'entity'.
     *
     * @param entity an entity
     * @return the database that created the entity
     */
    public static Object entityDb(IEntity entity) {
        return entityDbFn.invoke(entity);
    }

    /**
     * Returns true if the database is filtered using 'filter'.
     *
     * @param dConn a database
     * @return true if database is filtered, false otherwise.
     */
    public static boolean isFiltered(Object dConn) {
        return (Boolean)isFilteredFn.invoke(dConn);
    }

    /**
     * Filters the database given a predicate.
     *
     * Example: {@code Object filteredDB = Datahike.filter(dConn(conn), Clojure.read("(fn [_ datom] (not= :age (:a datom)))"));}
     *
     * @param dConn a database
     * @param pred the predicate used for filtering
     * @return a filtered version of the database
     */
    public static Object filter(Object dConn, Object pred) {
        return filterFn.invoke(dConn, pred);
    }


    // TODO: Not fully usable right now. Needs the missing definition of an ITxReport and implementation of
    // dbAfter, dbBefore, etc... on the Clojure side
    /**
     * Applies transaction to an immutable db value, returning a transaction report.
     *
     * @param dConn a database
     * @param txData the transaction data
     * @return
     */
/*
    public static Object with(Object dConn, Object txData) {
        return withFn.invoke(dConn, txData);
    }
*/

    /**
     * Applies transaction to an immutable db value, returning new immutable db value.
     *
     * @param dConn a database
     * @param txData the transaction data
     * @return a new immutable db value with the transaction applied.
     */
    public static Object dbWith(Object dConn, Object txData) {
        return dbWithFn.invoke(dConn, txData);
    }
}
