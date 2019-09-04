package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentVector;
import clojure.lang.RT;

import java.util.*;

import static datahike.java.Util.deref;

public class Datahike {
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

    /**
     * @return a de-referenced version of the connection
     */
    public static Object dConn(Object conn) {
        return deref.invoke(conn);
    };

    public static void deleteDatabase(String uri) {
        deleteDatabaseFn.invoke(uri);
    }

    public static void createDatabase(String uri) {
        createDatabaseFn.invoke(uri);
    }

    public static void createDatabase(String uri, Object... args) {
        List argsCopy = new ArrayList(Arrays.asList(args));
        argsCopy.add(0, uri);
        createDatabaseFn.applyTo(RT.seq(argsCopy));
    }

    public static Object connect(String uri) {
        return connectFn.invoke(uri);
    }

    public static Object db(Object conn) {
        return dbFn.invoke(conn);
    }

    public static Set q(String query, Object dConn) {
        return (Set)qFn.invoke(Clojure.read(query), dConn);
    }

    public static Object transact(Object conn, PersistentVector txData) {
        return transactFn.invoke(conn, txData);
    }

    /** dConn: the dereferenced conn object */
    public static Object history(Object dConn) { return historyFn.invoke(dConn); };

    public static Object asOf(Object dConn, Date date) {
        return asOfFn.invoke(dConn, date);
    }

    public static Object since(Object dConn, Date date) {
        return sinceFn.invoke(dConn, date);
    }

    public static Map pull(Object dConn, String selector, int eid) {
        return (Map) pullFn.invoke(dConn, Clojure.read(selector), eid);
    }

    public static Map pull(Object dConn, String selector, PersistentVector eid) {
        return (Map) pullFn.invoke(dConn, Clojure.read(selector), eid);
    }


    public static List pullMany(Object dConn, String selector, PersistentVector eids) {
        return (List) pullManyFn.invoke(dConn, Clojure.read(selector), eids);
    }

    public static void release(Object conn) {
        releaseFn.invoke(conn);
    }
}
