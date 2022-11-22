package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.APersistentVector;
import clojure.lang.APersistentMap;
import clojure.lang.RT;

import java.util.*;

import static datahike.java.Util.derefFn;

/**
 * Defines methods for interacting with a Datahike database.
 *
 * The <a href="https://github.com/replikativ/datahike/blob/main/doc/config.md">documentation</a> for the configuration can be constructed either with {@code datahike.java.Util.ednFromString} or piecewise with the {@code map} and {@code kwd} (keyword) constructors in the Util class, e.g. as {@code map(kwd(":store"), map(kwd(":backend"), kwd(":mem")), kwd(":name"), "test-empty-db-java")}
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
    public static Object deref(Object conn) {
        return derefFn.invoke(conn);
    };

    /**
     * Returns true if a database exists.
     *
     * @param config a database configuration map.
     * @return true if database exists
     */
    public static boolean databaseExists(APersistentMap config)  {
        return (boolean) databaseExistsFn.invoke(config);
    };

    /**
     * Deletes a database.
     *
     * @param config a database configuration map.
     */
    public static void deleteDatabase(APersistentMap config) {
        deleteDatabaseFn.invoke(config);
    }

    /**
     * Creates a database.
     *
     * @param config a database config.
     */
    public static void createDatabase(APersistentMap config) {
        createDatabaseFn.invoke(config);
    }

    /**
     * Creates connection to the Datahike database.
     * To access the current database value use {@link #deref(Object) deref}.
     *
     * @param config the database configuration
     * @return a connection to the database.
     */
    public static Object connect(APersistentMap config) {
        return connectFn.invoke(config);
    }

    /**
     * Executes a query.
     *
     * @param query A string of the query expressed in Clojure syntax.
     * @param inputs the arguments to the query, such as the database(s) and/or additional inputs.
     * @return the result of the query
     */
    public static Object q(String query, Object... inputs) {
        List argsCopy = new ArrayList(Arrays.asList(inputs));
        argsCopy.add(0, Clojure.read(query));
        return qFn.applyTo(RT.seq(argsCopy));
    }

    /**
     * Transacts data.
     *
     * @param conn the connection to the database.
     * @param txData the data to be transacted.
     * @return a transaction report.
     */
    public static Map transact(Object conn, Iterable txData) {
        return (Map)transactFn.invoke(conn, txData);
    }

    /**
     * Returns the full historical state of a database.
     *
     * @param db a database
     * @return the full history of the database
     */
    public static Object history(Object db) { return historyFn.invoke(db); };

    /**
     * Returns the database state at the given date.
     *
     * @param db the database
     * @param date either java.util.Date or Epoch Time as long
     * @return the database state at the given date
     */
    public static Object asOf(Object db, Date date) {
        return asOfFn.invoke(db, date);
    }

    /**
     * Returns the state of the database since the given date.
     * Be aware: the database contains only the datoms that were added since the date.
     *
     * @param db the database
     * @param date either java.util.Date or Epoch Time as long
     * @return  the state of the database since the given date.
     */
    public static Object since(Object db, Date date) {
        return sinceFn.invoke(db, date);
    }

    /**
     * Fetches data from database using recursive declarative description.
     * @see <a href="https://docs.datomic.com/on-prem/pull.html">docs.datomic.com/on-prem/pull.html</a>
     *
     * @param db the database
     * @param selector the criteria for the pull query
     * @param eid the entity id
     * @return the result of the query as map
     */
    public static APersistentMap pull(Object db, String selector, int eid) {
        return (APersistentMap)pullFn.invoke(db, Clojure.read(selector), eid);
    }

    /**
     * Fetches data from database using recursive declarative description.
     * @see <a href="https://docs.datomic.com/on-prem/pull.html">docs.datomic.com/on-prem/pull.html</a>
     *
     * @param db the database
     * @param selector the criteria for the pull query
     * @param eid an entity id
     * @return the result of the query as a map
     */
    public static APersistentMap pull(Object db, String selector, Iterable eid) {
        return (APersistentMap)pullFn.invoke(db, Clojure.read(selector), eid);
    }

    /**
     * Same as pull but takes multiple entities as input.
     *
     * @param db the database
     * @param selector the criteria for the pull query
     * @param eids a vector of entity ids
     * @return the result of the query as a list of maps
     */
    public static List pullMany(Object db, String selector, Iterable eids) {
        return (List)pullManyFn.invoke(db, Clojure.read(selector), eids);
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
     * @param db the database
     * @param index a keyword describing the type of index. E.g. {@code kwd(":eavt")}
     * @param c1 the first element of a datom used as the criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object db, Keyword index, Object c1) {
        return (List)seekDatomsFn.invoke(db, index, c1);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param db the database
     * @param index a keyword describing the type of index. E.g. {@code kwd(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object db, Keyword index, Object c1, Object c2) {
        return (List)seekDatomsFn.invoke(db, index, c1, c2);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param db the database
     * @param index a keyword describing the type of index. E.g. {@code kwd(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @param c3 the third element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object db, Keyword index, Object c1, Object c2, Object c3) {
        return (List)seekDatomsFn.invoke(db, index, c1, c2, c3);
    }

    /**
     * Returns datoms starting from specified components criteria and including rest of the database until the end of the index.
     *
     * @param db the database
     * @param index a keyword describing the type of index. E.g. {@code kwd(":eavt")}
     * @param c1 the first element of a datom used as criteria for filtering
     * @param c2 the second element of a datom used as criteria for filtering
     * @param c3 the third element of a datom used as criteria for filtering
     * @param c4 the fourth element of a datom used as criteria for filtering
     * @return the list of datoms
     */
    public static List seekdatoms(Object db, Keyword index, Object c1, Object c2, Object c3, Object c4) {
        return (List)seekDatomsFn.invoke(db, index, c1, c2, c3, c4);
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
     * @param db the database
     * @param eid an entity id
     * @return an entity
     */
    public static IEntity entity(Object db, int eid) {
        return (IEntity)entityFn.invoke(db, eid);
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
     * @param db a database
     * @return true if database is filtered, false otherwise.
     */
    public static boolean isFiltered(Object db) {
        return (Boolean)isFilteredFn.invoke(db);
    }

    /**
     * Filters the database given a predicate.
     *
     * Example: {@code Object filteredDB = Datahike.filter(deref(conn), Clojure.read("(fn [_ datom] (not= :age (:a datom)))"));}
     *
     * @param db a database
     * @param pred the predicate Clojure function used for filtering
     * @return a filtered version of the database
     */
    public static Object filter(Object db, Object pred) {
        return filterFn.invoke(db, pred);
    }

    /**
     * Applies transaction to an immutable db value, returning new immutable db value.
     *
     * @param db a database
     * @param txData the transaction data
     * @return a new immutable db value with the transaction applied.
     */
    public static Object dbWith(Object db, Object txData) {
        return dbWithFn.invoke(db, txData);
    }
}
