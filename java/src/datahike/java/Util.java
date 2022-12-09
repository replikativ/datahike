package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.APersistentMap;
import clojure.lang.APersistentVector;
import clojure.lang.Keyword;
import clojure.lang.RT;

public class Util {
    public  static final IFn derefFn = Clojure.var("clojure.core", "deref");
    private static final IFn hashMapFn = Clojure.var("clojure.core", "hash-map");
    private static final IFn vectorFn = Clojure.var("clojure.core", "vec");
    private static final IFn readStringFn = Clojure.var("clojure.edn", "read-string");

    /** Converts a keyword given as a string into a clojure keyword */
    public static Keyword kwd(String str) {
        return (Keyword)Clojure.read(str);
    }

    /** Creates a new clojure HashMap from the arguments of the form: key1, val1, key2, val2, ... */
    // TODO: parse each arg and if it starts with : then convert it into a clj keyword
    public static APersistentMap map(Object... keyVals) {
        return (APersistentMap) hashMapFn.applyTo(RT.seq(keyVals));
    }

    /** Creates a new clojure vector */
    public static APersistentVector vec(Object... items) {
        // No need to use 'applyTo here, as 'vectorFn taking a collection as an argument will produce a clj vector
        // containing *each* of the collection elements.
        return (APersistentVector) vectorFn.invoke(items);
    }



    /** Read string into edn data structure. */
    public static Object ednFromString(String str) {
        return readStringFn.invoke(str);
    }
}
