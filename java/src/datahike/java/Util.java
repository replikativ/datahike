package datahike.java;

import clojure.java.api.Clojure;

public class Util {
    /** Converts a keyword given as a string into a clojure keyword */
    public static Object k(String str) {
        return Clojure.read(str);
    }
}
