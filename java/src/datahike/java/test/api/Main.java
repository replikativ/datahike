package datahike.java.test.api;

import datahike.java.Util;
import datahike.java.api.Api;
import static datahike.java.Util.k;

// To run it: java -cp target/datahike-0.2.0-beta3-standalone.jar datahike.java.test.api.Main
public class Main {
    public static void main(String[] args) {
        String uri = "datahike:mem://test-empty-db";
        Api.deleteDatabase(uri);
        Api.createDatabase(uri);
        Object conn = Api.connect(uri);
        Api.transact(conn, Util.vector(Util.map(k(":db/ident"), k(":name"),
                                                k(":db/valueType"), k(":db.type/string"),
                                                k(":db/cardinality"), k(":db.cardinality/one"))));
        System.out.println("Done!");
    }
}
