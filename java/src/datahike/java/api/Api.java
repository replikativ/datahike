package datahike.java.api;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class Api {
    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.api"));
    }

    private static final IFn connectFn = Clojure.var("datahike.api", "connect");

    public static void connect(String uri) {
        connectFn.invoke(uri);
    }
}
