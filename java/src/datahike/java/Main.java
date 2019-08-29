package datahike.java;

import clojure.java.api.Clojure;
import datahike.java.Util;
import datahike.java.Datahike;

import java.util.Set;

import static datahike.java.Util.k;
import static datahike.java.Util.deref;

// To run it: java -cp target/datahike-0.2.0-standalone.jar datahike.java.Main
public class Main {
    public static void main(String[] args) {
        // Transacting new schema
        String uri = "datahike:mem://test-empty-db";
        Datahike.deleteDatabase(uri);
        Datahike.createDatabase(uri);
        Object conn = Datahike.connect(uri);
        Datahike.transact(conn, Util.vector(Util.map(k(":db/ident"), k(":name"),
                                                k(":db/valueType"), k(":db.type/string"),
                                                k(":db/cardinality"), k(":db.cardinality/one"))));
        System.out.println("Done!");

        // Transacting with schema present
        Datahike.transact(conn, Util.vector(Util.map(k(":name"), "Alice")));
        
        Object dConn = deref.invoke(conn);

        Set res = Datahike.q(Clojure.read("[:find ?e :where [?e :name]]"), dConn);
        System.out.println(res);

        res = Datahike.q(Clojure.read("[:find ?v :where [_ :name ?v]]"), dConn);
        System.out.println(res);
    }
}
