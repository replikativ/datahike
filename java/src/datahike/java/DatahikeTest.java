package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.PersistentHashSet;
import clojure.lang.APersistentMap;
import clojure.lang.APersistentVector;
import clojure.lang.PersistentVector;
import org.junit.Test;

import java.util.*;
import java.util.stream.Stream;

import static datahike.java.Datahike.deref;
import static datahike.java.Util.*;
import static org.junit.Assert.*;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;



public class DatahikeTest {

    private Object schema = Clojure.read(" [{:db/ident :name\n" +
            "                 :db/valueType :db.type/string\n" +
            "                 :db/unique :db.unique/identity\n" +
            "                 :db/index true\n" +
            "                 :db/cardinality :db.cardinality/one}\n" +
            "                {:db/ident :age\n" +
            "                 :db/valueType :db.type/long\n" +
            "                 :db/cardinality :db.cardinality/one}]");

    private APersistentMap config() {
        UUID connId = UUID.randomUUID();
        return map(kwd(":store"), map(kwd(":backend"), kwd(":memory"),
                                      kwd(":id"), connId),
                   kwd(":initial-tx"), this.schema);
    }

    private Date firstDate;
    private String query;

    private Object transactOnce() {
        APersistentMap config = config();
        Datahike.createDatabase(config);

        Object conn = Datahike.connect(config);
        query = "[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]";

        Datahike.transact(conn, vec(
                map(kwd(":name"), "Alice", kwd(":age"), 25L),
                map(kwd(":name"), "Bob", kwd(":age"), 30L)));
        return conn;
    }

    @org.junit.Test
    public void databaseExists() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        assertTrue(Datahike.databaseExists(config));
        APersistentMap configNotExisting = (APersistentMap)ednFromString("{:store {:backend :memory :id #uuid \"00000000-0000-0000-0000-000000000000\"}}");
        assertFalse(Datahike.databaseExists(configNotExisting));
    }

    @org.junit.Test
    public void queryWithDBAndInput() {
        Object conn = transactOnce();
        query = "[:find ?n ?a :in $ [?n] :where [?e :name ?n] [?e :age ?a]]";
        Set<APersistentVector> res = (Set<APersistentVector>)Datahike.q(query, deref(conn), Clojure.read("[\"Alice\"]"));
        Object[] names = res.stream().map(vec -> vec.get(0)).toArray();
        assertTrue(names[0].equals("Alice"));
    }

    @org.junit.Test
    public void queryWithJavaArrayInput() {
        Object conn = transactOnce();
        query = "[:find ?n ?a :in $ [?n] :where [?e :name ?n] [?e :age ?a]]";
        Set<APersistentVector> res = (Set<APersistentVector>)Datahike.q(query, deref(conn), new String[] {"Alice"});
        Object[] names = res.stream().map(vec -> vec.get(0)).toArray();
        assertTrue(names[0].equals("Alice"));
    }

    @org.junit.Test
    public void queryWithLocalInputDB() {
        Object input = Clojure.read("[[1 :name 'Ivan'] [1 :age  19] [1 :aka  \"dragon_killer_94\"] [1 :aka  '-=autobot=-']]");
        Set res = (Set)Datahike.q("[:find  ?n ?a :where [?e :aka \"dragon_killer_94\"] [?e :name ?n] [?e :age  ?a]]", input);
        assertTrue(res.size() == 1);
    }

    @org.junit.Test
    public void queryWithDB() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);

        // Transacting new schema
        Datahike.transact(conn, vec(map(kwd(":db/ident"), kwd(":name"),
                kwd(":db/valueType"), kwd(":db.type/string"),
                kwd(":db/cardinality"), kwd(":db.cardinality/one"))));

        // Transacting with schema presence
        Datahike.transact(conn, vec(map(kwd(":name"), "Alice")));

        Object deref = Datahike.deref(conn);

        Set res = (Set<APersistentVector>)Datahike.q("[:find ?e :where [?e :name]]", deref);
        assertTrue(res.size() == 1);

        res = (Set<APersistentVector>)Datahike.q("[:find ?v :where [_ :name ?v]]", deref);
        assertEquals(PersistentHashSet.create(Arrays.asList(PersistentVector.create("Alice"))), res);
    }

    @org.junit.Test
    public void history() {
        Object conn = transactOnce();

        Set<APersistentVector> res = (Set<APersistentVector>)Datahike.q((String) query, Datahike.history(deref(conn)));
        Object[] names = res.stream().map(pv -> pv.get(0)).toArray();
        assertArrayEquals(new String[] {"Alice", "Bob"}, names);
    }

    @Test
    public void asOfAndSince() {
        Object conn = transactOnce();

        // Make sure transaction has older timestamp than firstDate
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }

        firstDate = new Date();
        Datahike.transact(conn, vec(map(
                kwd(":db/id"), vec(kwd(":name"), "Alice"),
                kwd(":age"), 30L)));

        Set res = (Set<APersistentVector>)Datahike.q(query, Datahike.asOf(deref(conn), firstDate));
        assertEquals(2, res.size());

        res = (Set<APersistentVector>)Datahike.q(query, Datahike.since(deref(conn), firstDate));
        // 0, because :name was transacted before the first date
        assertEquals(0, res.size());
    }

    @Test
    public void pullAndPullMany() {
        Object conn = transactOnce();

        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                                        kwd(":name"), "Joe",
                                        kwd(":age"), 50L)));
        Map res = Datahike.pull(deref(conn), "[*]", 10);
        assertEquals("Joe", res.get(kwd(":name")));

        Datahike.transact(conn, vec(map(kwd(":db/id"), 20,
                kwd(":name"), "Jane",
                kwd(":age"), 25L)));
        List list = Datahike.pullMany(deref(conn), "[*]", vec(10, 20));
        assertEquals(2, list.size());
    }

    @Test
    public void release() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);
        Datahike.release(conn);
    }

    @Test
    public void seekDatoms() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);

        Datahike.transact(conn, (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]"));

        // Test seekdatoms convenience method (component-based)
        Object db = deref(conn);
        Iterable<?> datomsResult = Datahike.seekdatoms(db, kwd(":eavt"), 10);
        assertNotNull(datomsResult);

        // Just verify the API works - detailed datom testing is covered elsewhere
        boolean foundDatoms = false;
        for (Object item : datomsResult) {
            foundDatoms = true;
            break; // At least one datom found
        }
        assertTrue(foundDatoms);
    }

    @Test
    public void tempId() {
        Object id = Datahike.tempid(kwd(":db.part/user"));
        assertTrue(((Number)id).longValue() < 0);
        id = Datahike.tempid(kwd(":db.part/user"), -10000);
        assertEquals(-10000L, ((Number)id).longValue());
    }

    @Test
    public void entity() {
        Object conn = transactOnce();
        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                kwd(":name"), "Joe",
                kwd(":age"), 50L)));

        IEntity entity = (IEntity) Datahike.entity(deref(conn), 10);
        Object res = entity.valAt(kwd(":name"));
        assertEquals("Joe", res);
    }

    @Test
    public void entityDb() {
        Object conn = transactOnce();
        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                kwd(":name"), "Joe",
                kwd(":age"), 50L)));
        IEntity entity = (IEntity) Datahike.entity(deref(conn), 10);

        Object db = Datahike.entityDb(entity);
        assertNotNull(db);
    }

    @Test
    public void filterAndIsFiltered() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);
        assertFalse(Datahike.isFiltered(deref(conn)));

        Datahike.transact(conn, (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]"));
        Object filteredDB = Datahike.filter(deref(conn), Clojure.read("(fn [_ datom] (not= :age (:a datom)))"));
        assertTrue(Datahike.isFiltered(filteredDB));
    }

    @Test
    public void dbWith() {
        APersistentMap config = config();
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);
        APersistentVector txData = (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]");
        Object dbAfter = Datahike.dbWith(deref(conn), txData);
        query = "[:find ?a :in $ :where [?e :age ?a]]";
        Set res = (Set)Datahike.q(query, dbAfter);
        assertTrue(res.size() == 3);
    }

    /* TODO: This requires a durable backend */
    // @Test
    // public void gcStorage() {
    //     APersistentMap config = config();
    //     Datahike.createDatabase(config);
    //     Object conn = Datahike.connect(config);
    //     Set<UUID> res = (Set<UUID>)deref(Datahike.gcStorage(conn));
    //     assertTrue(res.size() == 0);
    // }

    /**
     * Called by Datahike's Clojure tests and runs the above Junit tests.
     */
    public static boolean run() {
        Result result = JUnitCore.runClasses(DatahikeTest.class);

        System.out.println("\n");
        List<Failure> failures = result.getFailures();
        for (Failure failure : failures) {
            System.out.println("Junit Failure: " + failure.toString());
        }
        System.out.println("Java Bindings test failure count: " + failures.size());

        return result.wasSuccessful();
    }
}
