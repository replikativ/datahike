package datahike.impl;

import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.word.PointerBase;
import com.oracle.svm.core.c.CConst;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import clojure.lang.IPersistentVector;
import clojure.lang.APersistentMap;

public final class LibDatahike {

    // C representations

    private static @CConst CCharPointer toException(Exception e) {
        return toCCharPointer("exception:".concat(e.toString()));
    }

    public static APersistentMap readConfig(CCharPointer db_config) {
        return (APersistentMap)Util.ednFromString(CTypeConversion.toJavaString(db_config));
    }

    public static @CConst CCharPointer toCCharPointer(String result) {
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(result);
        CCharPointer value = holder.get();
        return value;
    }

    public static @CConst CCharPointer toCCharPointer(byte[] result) {
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCBytes(result);
        CCharPointer value = holder.get();
        return value;
    }

    private static Object loadInput(@CConst CCharPointer input_format,
                                    @CConst CCharPointer raw_input) {
        String format = CTypeConversion.toJavaString(input_format);
        switch (format) {
        case "db": return Datahike.deref(Datahike.connect(readConfig(raw_input)));
        case "json": return libdatahike.parseJSON(CTypeConversion.toJavaString(raw_input));
        case "edn": return libdatahike.parseEdn(CTypeConversion.toJavaString(raw_input));
        case "cbor": return libdatahike.parseCBOR(CTypeConversion.toJavaString(raw_input).getBytes());
        default: throw new IllegalArgumentException("Input format not supported: " + format);
        }
    }

    private static Object[] loadInputs(@CConst long num_inputs,
                                       @CConst CCharPointerPointer input_formats,
                                       @CConst CCharPointerPointer raw_inputs) {
        Object[] inputs = new Object[(int)num_inputs];
        for (int i=0; i<num_inputs; i++){
            inputs[i] = loadInput(input_formats.read(i), raw_inputs.read(i));
        }
        return inputs;
    }

    private static @CConst CCharPointer toOutput(@CConst CCharPointer output_format,
                                                 Object output) {
        String format = CTypeConversion.toJavaString(output_format);
        switch (format) {
        case "json": return toCCharPointer(libdatahike.toJSONString(output));
        case "edn": return toCCharPointer(libdatahike.toEdnString(output));
        case "cbor": return toCCharPointer(libdatahike.toCBOR(output));
        default: throw new IllegalArgumentException("Input format not supported: " + format);
        }
    }

    // core API

    @CEntryPoint(name = "database_exists")
    public static @CConst CCharPointer database_exists (@CEntryPoint.IsolateThreadContext long isolateId,
                                                        @CConst CCharPointer db_config) {
        try {
            return toCCharPointer(libdatahike.toJSONString(Datahike.databaseExists(readConfig(db_config))));
        } catch (Exception e) {
            return toException(e);
        }
    }

    @CEntryPoint(name = "delete_database")
    public static @CConst CCharPointer delete_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                                        @CConst CCharPointer db_config) {
        try {
            Datahike.deleteDatabase(readConfig(db_config));
            return toCCharPointer("");
        } catch (Exception e) {
            return toException(e);
        }
    }

    @CEntryPoint(name = "create_database")
    public static @CConst CCharPointer create_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                                        @CConst CCharPointer db_config) {
        try {
            Datahike.createDatabase(readConfig(db_config));
            return toCCharPointer("");
        } catch (Exception e) {
            return toException(e);
        }
    }

    @CEntryPoint(name = "query")
    public static @CConst CCharPointer query (@CEntryPoint.IsolateThreadContext long isolateId,
                                              @CConst CCharPointer query,
                                              long num_inputs,
                                              @CConst CCharPointerPointer input_formats,
                                              @CConst CCharPointerPointer raw_inputs,
                                              @CConst CCharPointer output_format) {
        try {
            Object[] inputs = loadInputs(num_inputs, input_formats, raw_inputs);
            return toOutput(output_format, Datahike.q(CTypeConversion.toJavaString(query), inputs));
        } catch (Exception e) {
            return toException(e);
        }
    }

    @CEntryPoint(name = "transact")
    public static @CConst CCharPointer transact (@CEntryPoint.IsolateThreadContext long isolateId,
                                                 @CConst CCharPointer db_config,
                                                 @CConst CCharPointer tx_format,
                                                 @CConst CCharPointer tx_data,
                                                 @CConst CCharPointer output_format) {
        try {
            Object conn = Datahike.connect(readConfig(db_config));
            Iterable txData = (Iterable)loadInput(tx_format, tx_data);
            return toOutput(output_format, Datahike.transact(conn, txData).get(Util.kwd(":tx-meta")));
        } catch (Exception e) {
            return toException(e);
        }
    }

    // memory handling

    @CEntryPoint(name = "libdatahike_free")
    public static void libdatahike_free(@CEntryPoint.IsolateThreadContext long isolateId, @CConst PointerBase ptr) {
        UnmanagedMemory.free(ptr);
    }
}
