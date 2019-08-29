package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentVector;

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
    public  static final IFn deref = Clojure.var("clojure.core", "deref");


    public static void deleteDatabase(String uri) {
        deleteDatabaseFn.invoke(uri);
    }

    public static void createDatabase(String uri) {
        createDatabaseFn.invoke(uri);
    }

    public static Object connect(String uri) {
        return connectFn.invoke(uri);
    }

    public static Object db(Object conn) {
        return dbFn.invoke(conn);
    }

    public static Object q(Object query, Object db) {
        return qFn.invoke(query, db);
    }

    public static Object transact(Object conn, PersistentVector txData) {
        return transactFn.invoke(conn, txData);
    }
}
