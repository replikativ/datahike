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
    public static APersistentMap map(Object... keyVals) {
        Object[] converted = new Object[keyVals.length];
        for (int i = 0; i < keyVals.length; i += 2) {
            // Convert keys (always keywordize)
            Object key = keyVals[i];
            if (key instanceof String) {
                String strKey = (String) key;
                converted[i] = strKey.startsWith(":") ? kwd(strKey) : Keyword.intern(strKey);
            } else {
                converted[i] = key;
            }

            // Convert values (apply EDN rules)
            if (i + 1 < keyVals.length) {
                converted[i + 1] = convertValue(keyVals[i + 1]);
            }
        }
        return (APersistentMap) hashMapFn.applyTo(RT.seq(converted));
    }

    /** Creates a new clojure vector with EDN conversion applied to items */
    public static APersistentVector vec(Object... items) {
        // Convert each item using EDN rules
        Object[] converted = new Object[items.length];
        for (int i = 0; i < items.length; i++) {
            converted[i] = convertValue(items[i]);
        }
        return vecRaw(converted);
    }

    /** Creates a new clojure vector without conversion (internal use) */
    private static APersistentVector vecRaw(Object... items) {
        // No need to use 'applyTo here, as 'vectorFn taking a collection as an argument will produce a clj vector
        // containing *each* of the collection elements.
        return (APersistentVector) vectorFn.invoke(items);
    }

    /**
     * Converts a value following universal EDN conversion rules:
     * - String starting with ":" → keyword
     * - String starting with "\\:" → literal string with ":" (escaped)
     * - String without ":" → string
     * - Numbers, booleans, null → as-is
     * - Maps → recursively convert
     * - Lists/Arrays → recursively convert
     *
     * This is public to allow reuse from Clojure interop layer.
     */
    public static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }

        // Handle strings with EDN conversion rules
        if (value instanceof String) {
            String str = (String) value;

            // Escaped colon - strip backslash and return as string
            if (str.startsWith("\\:")) {
                return str.substring(1);  // Return ":literal" from "\\:literal"
            }

            // Colon prefix - convert to keyword
            if (str.startsWith(":")) {
                return kwd(str);
            }

            // Regular string - return as-is
            return str;
        }

        // Recursively convert nested maps
        if (value instanceof java.util.Map && !(value instanceof APersistentMap)) {
            return mapToPersistentMap((java.util.Map<String, Object>) value);
        }

        // Recursively convert lists/arrays
        if (value instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) value;
            Object[] converted = new Object[list.size()];
            for (int i = 0; i < list.size(); i++) {
                converted[i] = convertValue(list.get(i));
            }
            return vecRaw(converted);
        }

        if (value instanceof Object[]) {
            Object[] arr = (Object[]) value;
            Object[] converted = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) {
                converted[i] = convertValue(arr[i]);
            }
            return vecRaw(converted);
        }

        // Numbers, booleans, and other types - return as-is
        return value;
    }

    /** Converts a Java Map to a Clojure persistent map */
    public static APersistentMap mapToPersistentMap(java.util.Map<String, Object> javaMap) {
        if (javaMap == null) {
            return null;
        }
        Object[] keyVals = new Object[javaMap.size() * 2];
        int i = 0;
        for (java.util.Map.Entry<String, Object> entry : javaMap.entrySet()) {
            Object key = entry.getKey();
            // If key is already a Keyword, use it as-is
            if (key instanceof Keyword) {
                keyVals[i++] = key;
            } else if (key instanceof String) {
                String strKey = (String) key;
                // Convert string keys to keywords (auto-prefix with : if not present)
                keyVals[i++] = strKey.startsWith(":") ? kwd(strKey) : Keyword.intern(strKey);
            } else {
                // Use key as-is for other types
                keyVals[i++] = key;
            }

            // Convert values using universal EDN rules
            keyVals[i++] = convertValue(entry.getValue());
        }
        return (APersistentMap) hashMapFn.applyTo(RT.seq(keyVals));
    }

    /**
     * Convert Clojure persistent map to Java HashMap.
     * Used for return values that need to be Java-friendly.
     *
     * @param clojureMap Clojure persistent map
     * @return Java HashMap with string keys
     */
    public static java.util.Map<String, Object> clojureMapToJavaMap(APersistentMap clojureMap) {
        if (clojureMap == null) {
            return null;
        }
        java.util.Map<String, Object> javaMap = new java.util.HashMap<>();
        for (Object entry : clojureMap) {
            java.util.Map.Entry<?, ?> e = (java.util.Map.Entry<?, ?>) entry;
            String key = e.getKey().toString();
            javaMap.put(key, e.getValue());
        }
        return javaMap;
    }

    /**
     * Normalize Java collections to Clojure collections for API processing.
     *
     * <p><b>Escape hatch (automatic):</b> If data is already a Clojure persistent
     * collection, returns immediately with O(1) instanceof check. Advanced users
     * can use {@link #vec} and {@link #map} for zero-overhead calls.</p>
     *
     * <p><b>Conversion rules (applied recursively):</b></p>
     * <ul>
     *   <li>java.util.List → clojure.lang.PersistentVector</li>
     *   <li>java.util.Map → clojure.lang.PersistentHashMap</li>
     *   <li>Map keys (String) → keywords ("name" → :name, ":name" → :name)</li>
     *   <li>Values (String) → keywords only if prefixed with ":"</li>
     *   <li>Primitives, Clojure types → unchanged</li>
     * </ul>
     *
     * <p>This function is general and consistent - applied uniformly to all
     * Java API method parameters that accept collections.</p>
     *
     * @param data The data to normalize (List, Map, or other types)
     * @return Clojure-compatible data structure
     */
    public static Object normalizeCollections(Object data) {
        // Escape hatch: already Clojure persistent collection - O(1) check
        if (data instanceof clojure.lang.IPersistentCollection) {
            return data;
        }

        // Java List → Clojure vector with recursive conversion
        if (data instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) data;
            Object[] converted = new Object[list.size()];
            for (int i = 0; i < list.size(); i++) {
                converted[i] = convertValue(list.get(i));
            }
            return vecRaw(converted);
        }

        // Java Map → Clojure map with recursive conversion
        if (data instanceof java.util.Map && !(data instanceof APersistentMap)) {
            return mapToPersistentMap((java.util.Map<String, Object>) data);
        }

        // Other types → apply EDN value conversion rules
        return convertValue(data);
    }

    /** Read string into edn data structure. */
    public static Object ednFromString(String str) {
        return readStringFn.invoke(str);
    }
}
