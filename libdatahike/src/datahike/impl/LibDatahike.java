package datahike.impl;

import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CConst;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import clojure.lang.IPersistentVector;
import clojure.lang.APersistentMap;
import clojure.lang.Keyword;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.charset.StandardCharsets;

public final class LibDatahike {

    // C representations

    private static @CConst CCharPointer toException(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String st = sw.toString();
        return toCCharPointer("exception:".concat(e.toString()).concat("\nStacktrace:\n").concat(st));
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
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCBytes(Base64.getEncoder().encode(result));
        CCharPointer value = holder.get();
        return value;
    }

    private static Object loadInput(@CConst CCharPointer input_format,
                                    @CConst CCharPointer raw_input) {
        String format = CTypeConversion.toJavaString(input_format);
        String formats[] = format.split(":");
        switch (formats[0]) {
        case "db": return Datahike.deref(Datahike.connect(readConfig(raw_input)));
        case "history": return Datahike.history(Datahike.deref(Datahike.connect(readConfig(raw_input))));
        case "since": return Datahike.since(Datahike.deref(Datahike.connect(readConfig(raw_input))),
                                            new Date(Long.parseLong(formats[1])));
        case "asof": return Datahike.asOf(Datahike.deref(Datahike.connect(readConfig(raw_input))),
                                          new Date(Long.parseLong(formats[1])));
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

    // callback function to read return value
    interface OutputReader extends CFunctionPointer {
        @InvokeCFunctionPointer
        void call(@CConst CCharPointer output);
    }

    // core API

    @CEntryPoint(name = "database_exists")
    public static void database_exists (@CEntryPoint.IsolateThreadContext long isolateId,
                                        @CConst CCharPointer db_config,
                                        @CConst CCharPointer output_format,
                                        @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format,
                                        Datahike.databaseExists(readConfig(db_config))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "delete_database")
    public static void delete_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                        @CConst CCharPointer db_config,
                                        @CConst CCharPointer output_format,
                                        @CConst OutputReader output_reader) {
        try {
            Datahike.deleteDatabase(readConfig(db_config));
            output_reader.call(toOutput(output_format, ""));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "create_database")
    public static void create_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                        @CConst CCharPointer db_config,
                                        @CConst CCharPointer output_format,
                                        @CConst OutputReader output_reader) {
        try {
            Datahike.createDatabase(readConfig(db_config));
            output_reader.call(toOutput(output_format, ""));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "query")
    public static void query (@CEntryPoint.IsolateThreadContext long isolateId,
                              @CConst CCharPointer query_edn,
                              long num_inputs,
                              @CConst CCharPointerPointer input_formats,
                              @CConst CCharPointerPointer raw_inputs,
                              @CConst CCharPointer output_format,
                              @CConst OutputReader output_reader) {
        try {
            Object[] inputs = loadInputs(num_inputs, input_formats, raw_inputs);
            output_reader.call(toOutput(output_format, Datahike.q(CTypeConversion.toJavaString(query_edn), inputs)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "transact")
    public static void transact (@CEntryPoint.IsolateThreadContext long isolateId,
                                 @CConst CCharPointer db_config,
                                 @CConst CCharPointer tx_format,
                                 @CConst CCharPointer tx_data,
                                 @CConst CCharPointer output_format,
                                 @CConst OutputReader output_reader) {
        try {
            Object conn = Datahike.connect(readConfig(db_config));
            Iterable txData = (Iterable)loadInput(tx_format, tx_data);
            output_reader.call(toOutput(output_format, Datahike.transact(conn, txData).get(Util.kwd(":tx-meta"))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "pull")
    public static void pull (@CEntryPoint.IsolateThreadContext long isolateId,
                             @CConst CCharPointer input_format,
                             @CConst CCharPointer raw_input,
                             @CConst CCharPointer selector_edn,
                             long eid,
                             @CConst CCharPointer output_format,
                             @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            String selector = CTypeConversion.toJavaString(selector_edn);
            output_reader.call(toOutput(output_format, Datahike.pull(db, selector, eid)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "pull_many")
    public static void pull_many (@CEntryPoint.IsolateThreadContext long isolateId,
                                  @CConst CCharPointer input_format,
                                  @CConst CCharPointer raw_input,
                                  @CConst CCharPointer selector_edn,
                                  @CConst CCharPointer eids_edn,
                                  @CConst CCharPointer output_format,
                                  @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            String selector = CTypeConversion.toJavaString(selector_edn);
            Iterable eids = (Iterable)libdatahike.parseEdn(CTypeConversion.toJavaString(eids_edn));
            output_reader.call(toOutput(output_format, Datahike.pullMany(db, selector, eids)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "entity")
    public static void entity (@CEntryPoint.IsolateThreadContext long isolateId,
                               @CConst CCharPointer input_format,
                               @CConst CCharPointer raw_input,
                               long eid,
                               @CConst CCharPointer output_format,
                               @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            output_reader.call(toOutput(output_format, libdatahike.intoMap(Datahike.entity(db, eid))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "datoms")
    public static void datoms(@CEntryPoint.IsolateThreadContext long isolateId,
                              @CConst CCharPointer input_format,
                              @CConst CCharPointer raw_input,
                              @CConst CCharPointer index_edn,
                              @CConst CCharPointer output_format,
                              @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            Keyword index = Util.kwd(CTypeConversion.toJavaString(index_edn));
            // convert datoms to flat clojure vectors for better serialization
            Iterable datoms = libdatahike.datomsToVecs(Datahike.datoms(db, index));
            output_reader.call(toOutput(output_format, datoms));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "schema")
    public static void schema(@CEntryPoint.IsolateThreadContext long isolateId,
                              @CConst CCharPointer input_format,
                              @CConst CCharPointer raw_input,
                              @CConst CCharPointer output_format,
                              @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            output_reader.call(toOutput(output_format, Datahike.schema(db)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "reverse_schema")
    public static void reverse_schema(@CEntryPoint.IsolateThreadContext long isolateId,
                              @CConst CCharPointer input_format,
                              @CConst CCharPointer raw_input,
                              @CConst CCharPointer output_format,
                              @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            output_reader.call(toOutput(output_format, Datahike.reverseSchema(db)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "metrics")
    public static void metrics(@CEntryPoint.IsolateThreadContext long isolateId,
                              @CConst CCharPointer input_format,
                              @CConst CCharPointer raw_input,
                              @CConst CCharPointer output_format,
                              @CConst OutputReader output_reader) {
        try {
            Object db = loadInput(input_format, raw_input);
            output_reader.call(toOutput(output_format, Datahike.metrics(db)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    @CEntryPoint(name = "gc_storage")
    public static void gc_storage(@CEntryPoint.IsolateThreadContext long isolateId,
                                 @CConst CCharPointer db_config,
                                 long before_tx_unix_time_ms,
                                 @CConst CCharPointer output_format,
                                 @CConst OutputReader output_reader) {
        try {
            Object db = Datahike.connect(readConfig(db_config));
            output_reader.call(toOutput(output_format, Datahike.gcStorage(db, new Date(before_tx_unix_time_ms))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }

    // seekdatoms not supported yet because we would always realize the iterator until the end

    // release we do not expose connection objects yet, but create them internally on the fly
}
