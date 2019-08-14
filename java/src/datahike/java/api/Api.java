package datahike.java.api;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import clojure.lang.RT;

public class Api {
    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.api"));
    }

    private static final IFn createDatabaseFn = Clojure.var("datahike.api", "create-database");
    private static final IFn connectFn = Clojure.var("datahike.api", "connect");
    private static final IFn transactFn = Clojure.var("datahike.api", "transact!");

    private static final IFn hashMapFn = Clojure.var("clojure.core", "hash-map");
    private static final IFn vectorFn = Clojure.var("clojure.core", "vec");

    // Move this to a Util.java
    /** Creates a new clojure HashMap */
    public static PersistentHashMap map(Object... keyVals) {
        return (PersistentHashMap) hashMapFn.applyTo(RT.seq(keyVals));
    }

    public static PersistentVector vector(Object... items) {
        // return (PersistentVector) vectorFn.applyTo(RT.seq(items));
        return (PersistentVector) vectorFn.invoke(items);
    }

    public static void deleteDatabase(String uri) {
        deleteDatabaseFn.invoke(uri);
    }

    public static void createDatabase(String uri) {
        createDatabaseFn.invoke(uri);
    }

    public static Object connect(String uri) {
        return connectFn.invoke(uri);
    }

    public static Object transact(Object conn, PersistentHashMap txData) {
        return transactFn.invoke(conn, vector(txData));
    }
}
