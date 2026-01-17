package datahike.impl;

import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CConst;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import clojure.lang.APersistentMap;
import clojure.lang.Keyword;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Base64;

/**
 * Base infrastructure for native C bindings.
 *
 * This class provides the core utilities for:
 * - Exception handling and formatting
 * - Input format parsing (db, history, since, asof, json, edn, cbor)
 * - Output format serialization (json, edn, cbor)
 * - C type conversions
 *
 * The actual C entry points are generated in LibDatahike.java.
 */
public class LibDatahikeBase {

    // =========================================================================
    // Callback Interface
    // =========================================================================

    /**
     * C function pointer interface for reading return values.
     * All native entry points use this callback pattern to return results.
     */
    public interface OutputReader extends CFunctionPointer {
        @InvokeCFunctionPointer
        void call(@CConst CCharPointer output);
    }

    // =========================================================================
    // Exception Handling
    // =========================================================================

    /**
     * Format an exception as a C string for callback.
     * Format: "exception:{message}\nStacktrace:\n{stacktrace}"
     */
    protected static @CConst CCharPointer toException(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String st = sw.toString();
        return toCCharPointer("exception:".concat(e.toString()).concat("\nStacktrace:\n").concat(st));
    }

    // =========================================================================
    // C Type Conversions
    // =========================================================================

    /**
     * Parse EDN configuration string to Clojure map.
     */
    public static APersistentMap readConfig(CCharPointer db_config) {
        return (APersistentMap) Util.ednFromString(CTypeConversion.toJavaString(db_config));
    }

    /**
     * Convert Java String to C char pointer.
     */
    public static @CConst CCharPointer toCCharPointer(String result) {
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(result);
        CCharPointer value = holder.get();
        return value;
    }

    /**
     * Convert byte array to Base64-encoded C char pointer.
     * Used for CBOR output format.
     */
    public static @CConst CCharPointer toCCharPointer(byte[] result) {
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCBytes(Base64.getEncoder().encode(result));
        CCharPointer value = holder.get();
        return value;
    }

    // =========================================================================
    // Input Format Handling
    // =========================================================================

    /**
     * Load input based on format specification.
     *
     * Supported formats:
     * - "db" : Connect and deref to get current database
     * - "history" : Get full history database
     * - "since:{timestamp_ms}" : Get database since timestamp
     * - "asof:{timestamp_ms}" : Get database as-of timestamp
     * - "json" : Parse as JSON
     * - "edn" : Parse as EDN
     * - "cbor" : Parse as CBOR (base64 encoded)
     */
    protected static Object loadInput(@CConst CCharPointer input_format,
                                       @CConst CCharPointer raw_input) {
        String format = CTypeConversion.toJavaString(input_format);
        String formats[] = format.split(":");
        switch (formats[0]) {
        case "db":
            return Datahike.deref(Datahike.connect(readConfig(raw_input)));
        case "history":
            return Datahike.history(Datahike.deref(Datahike.connect(readConfig(raw_input))));
        case "since":
            return Datahike.since(Datahike.deref(Datahike.connect(readConfig(raw_input))),
                                  new Date(Long.parseLong(formats[1])));
        case "asof":
            return Datahike.asOf(Datahike.deref(Datahike.connect(readConfig(raw_input))),
                                 new Date(Long.parseLong(formats[1])));
        case "json":
            return libdatahike.parseJSON(CTypeConversion.toJavaString(raw_input));
        case "edn":
            return libdatahike.parseEdn(CTypeConversion.toJavaString(raw_input));
        case "cbor":
            return libdatahike.parseCBOR(CTypeConversion.toJavaString(raw_input).getBytes());
        default:
            throw new IllegalArgumentException("Input format not supported: " + format);
        }
    }

    /**
     * Load multiple inputs for query operations.
     */
    protected static Object[] loadInputs(@CConst long num_inputs,
                                          @CConst CCharPointerPointer input_formats,
                                          @CConst CCharPointerPointer raw_inputs) {
        Object[] inputs = new Object[(int) num_inputs];
        for (int i = 0; i < num_inputs; i++) {
            inputs[i] = loadInput(input_formats.read(i), raw_inputs.read(i));
        }
        return inputs;
    }

    // =========================================================================
    // Output Format Handling
    // =========================================================================

    /**
     * Serialize output based on format specification.
     *
     * Supported formats:
     * - "json" : Serialize as JSON string
     * - "edn" : Serialize as EDN string
     * - "cbor" : Serialize as CBOR (base64 encoded)
     */
    protected static @CConst CCharPointer toOutput(@CConst CCharPointer output_format,
                                                    Object output) {
        String format = CTypeConversion.toJavaString(output_format);
        switch (format) {
        case "json":
            return toCCharPointer(libdatahike.toJSONString(output));
        case "edn":
            return toCCharPointer(libdatahike.toEdnString(output));
        case "cbor":
            return toCCharPointer(libdatahike.toCBOR(output));
        default:
            throw new IllegalArgumentException("Output format not supported: " + format);
        }
    }

    // =========================================================================
    // Helper Methods for Generated Code
    // =========================================================================

    /**
     * Parse keyword from EDN string (e.g., ":eavt" -> Keyword).
     */
    protected static Keyword parseKeyword(CCharPointer kwd_edn) {
        return Util.kwd(CTypeConversion.toJavaString(kwd_edn));
    }

    /**
     * Parse EDN to Iterable (for eids, etc).
     */
    protected static Iterable<?> parseIterable(CCharPointer edn) {
        return (Iterable<?>) libdatahike.parseEdn(CTypeConversion.toJavaString(edn));
    }

    /**
     * Convert datoms to vectors for serialization.
     */
    protected static Iterable<?> datomsToVecs(Iterable<?> datoms) {
        return libdatahike.datomsToVecs(datoms);
    }

    /**
     * Convert entity to map for serialization.
     */
    protected static Object entityToMap(Object entity) {
        return libdatahike.intoMap(entity);
    }
}
