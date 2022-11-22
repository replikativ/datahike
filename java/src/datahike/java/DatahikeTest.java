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

    private Object conn;

    private Object schema = Clojure.read(" [{:db/ident :name\n" +
            "                 :db/valueType :db.type/string\n" +
            "                 :db/unique :db.unique/identity\n" +
            "                 :db/index true\n" +
            "                 :db/cardinality :db.cardinality/one}\n" +
            "                {:db/ident :age\n" +
            "                 :db/valueType :db.type/long\n" +
            "                 :db/cardinality :db.cardinality/one}]");

    private APersistentMap config = map(kwd(":store"), map(kwd(":backend"), kwd(":mem")),
                                        kwd(":name"), "test-empty-db-java",
                                        kwd(":initial-tx"), this.schema);

    private Date firstDate;
    private String query;

    @org.junit.Before
    public void setUp() throws Exception {
        Datahike.deleteDatabase(config);
    }

    @org.junit.After
    public void tearDown() throws Exception {
        Datahike.deleteDatabase(config);
    }

    private void transactOnce() {
        Datahike.createDatabase(config);

        conn = Datahike.connect(config);
        query = "[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]";

        Datahike.transact(conn, vec(
                map(kwd(":name"), "Alice", kwd(":age"), 25L),
                map(kwd(":name"), "Bob", kwd(":age"), 30L)));
    }

    // @org.junit.Test
    /* public void db() {
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);
        assertEquals(Datahike.db(conn), deref(conn));
        } */

    @org.junit.Test
    public void databaseExists() {
        Datahike.createDatabase(config);
        assertTrue(Datahike.databaseExists(config));
        APersistentMap configNotExisting = (APersistentMap)ednFromString("{:store {:backend :mem :id \"it-does-not-exist\"}}");
        assertFalse(Datahike.databaseExists(configNotExisting));
    }

    @org.junit.Test
    public void queryWithDBAndInput() {
        transactOnce();
        query = "[:find ?n ?a :in $ [?n] :where [?e :name ?n] [?e :age ?a]]";
        Set<APersistentVector> res = (Set<APersistentVector>)Datahike.q(query, deref(conn), Clojure.read("[\"Alice\"]"));
        Object[] names = res.stream().map(vec -> vec.get(0)).toArray();
        assertTrue(names[0].equals("Alice"));
    }

    @org.junit.Test
    public void queryWithJavaArrayInput() {
        transactOnce();
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
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);

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
        transactOnce();

        Set<APersistentVector> res = (Set<APersistentVector>)Datahike.q((String) query, Datahike.history(deref(conn)));
        Object[] names = res.stream().map(pv -> pv.get(0)).toArray();
        assertEquals(new String[] {"Alice", "Bob"}, names);
    }

    @Test
    public void asOfAndSince() {
        transactOnce();

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
        transactOnce();

        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                                        kwd(":name"), "Joe",
                                        kwd(":age"), 50L)));
        Map res = Datahike.pull(deref(conn), "[*]", 10);
        assertEquals("Joe", res.get(kwd(":name")));

        res = Datahike.pull(deref(conn), "[*]", vec(kwd(":name"), "Alice"));
        assertEquals("Alice", res.get(kwd(":name")));


        Datahike.transact(conn, vec(map(kwd(":db/id"), 20,
                kwd(":name"), "Jane",
                kwd(":age"), 25L)));
        List list = Datahike.pullMany(deref(conn), "[*]", vec(10, 20));
        assertEquals(2, list.size());
    }

    @Test
    public void release() {
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);
        Datahike.release(conn);
    }

    @Test
    public void seekDatoms() {
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);

        Datahike.transact(conn, (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]"));
        List<APersistentVector> res = Datahike.seekdatoms(deref( conn), kwd(":eavt"), 10);
        res.stream().map(vec -> {assertTrue((int)vec.get(0) >= 10); return null;});

        res = Datahike.seekdatoms(deref( conn), kwd(":eavt"), 10, kwd(":name"));
        res.stream().map(vec -> {
                int entityId = (int)vec.get(0);
                assertTrue(entityId == 10 && vec.get(1).equals(":name") ||
                           entityId > 10);
                return null;
            });

        res = Datahike.seekdatoms(deref( conn), kwd(":eavt"), 30, kwd(":name"), "Sergey");
        res.stream().map(vec -> {
                int entityId = (int)vec.get(0);
                assertTrue(entityId == 30 && vec.get(1).equals(":name") && vec.get(2).equals("Sergey") ||
                           entityId > 30);
                return null;
            });
    }

    @Test
    public void tempId() {
        Long id = Datahike.tempId(kwd(":db.part/user"));
        assertTrue(id < 0);
        id = Datahike.tempId(kwd(":db.part/user"), -10000L);
        assertEquals(-10000L, (long)id);
    }

    @Test
    public void entity() {
        transactOnce();
        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                kwd(":name"), "Joe",
                kwd(":age"), 50L)));

        IEntity entity = Datahike.entity(deref(conn), 10);
        Object res = entity.valAt(kwd(":name"));
        assertEquals("Joe", res);
    }

    @Test
    public void entityDb() {
        transactOnce();
        Datahike.transact(conn, vec(map(kwd(":db/id"), 10,
                kwd(":name"), "Joe",
                kwd(":age"), 50L)));
        IEntity entity = Datahike.entity(deref(conn), 10);

        Object db = Datahike.entityDb(entity);
        assertNotNull(db);
    }

    @Test
    public void filterAndIsFiltered() {
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);
        assertFalse(Datahike.isFiltered(deref(conn)));

        Datahike.transact(conn, (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]"));
        Object filteredDB = Datahike.filter(deref(conn), Clojure.read("(fn [_ datom] (not= :age (:a datom)))"));
        assertTrue(Datahike.isFiltered(filteredDB));
    }

    @Test
    public void dbWith() {
        Datahike.createDatabase(config);
        conn = Datahike.connect(config);
        APersistentVector txData = (APersistentVector)Clojure.read("[{:db/id 10 :name \"Petr\" :age 44} {:db/id 20 :name \"Ivan\" :age 25} {:db/id 30 :name \"Sergey\" :age 11}]");
        Object dbAfter = Datahike.dbWith(deref(conn), txData);
        query = "[:find ?a :in $ :where [?e :age ?a]]";
        Set res = (Set)Datahike.q(query, dbAfter);
        assertTrue(res.size() == 3);
    }


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
