package datahike.impl;

import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import com.oracle.svm.core.c.CConst;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import clojure.lang.IPersistentVector;
import clojure.lang.APersistentMap;

public final class LibDatahike {

    public static APersistentMap readConfig(CCharPointer configString) {
        return (APersistentMap)Util.ednFromString(CTypeConversion.toJavaString(configString));
    }

    public static @CConst CCharPointer toCCharPointer(String result) {
        CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(result);
        CCharPointer value = holder.get();
        return value;
    }

    @CEntryPoint(name = "database_exists")
    public static @CConst CCharPointer database_exists (@CEntryPoint.IsolateThreadContext long isolateId,
                                                        @CConst CCharPointer configString) {
        return toCCharPointer(libdatahike.toJSONString(Datahike.databaseExists(readConfig(configString))));
    }

    @CEntryPoint(name = "delete_database")
    public static void delete_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                        @CConst CCharPointer configString) {
        Datahike.deleteDatabase(readConfig(configString));
    }

    @CEntryPoint(name = "create_database")
    public static void create_database (@CEntryPoint.IsolateThreadContext long isolateId,
                                        @CConst CCharPointer configString) {
        Datahike.createDatabase(readConfig(configString));
    }

    @CEntryPoint(name = "q_json")
    public static @CConst CCharPointer q_json (@CEntryPoint.IsolateThreadContext long isolateId,
                                               @CConst CCharPointer query,
                                               @CConst CCharPointer configString) {
        Object db = Datahike.deref(Datahike.connect(readConfig(configString)));
        Object[] inputs = new Object[]{db};
        return toCCharPointer(libdatahike.toJSONString(Datahike.q(CTypeConversion.toJavaString(query), inputs)));
    }

    @CEntryPoint(name = "transact_json")
    public static @CConst CCharPointer transact_json (@CEntryPoint.IsolateThreadContext long isolateId,
                                                      @CConst CCharPointer configString,
                                                      @CConst CCharPointer txDataJson) {
        Object conn = Datahike.connect(readConfig(configString));
        Iterable txData = libdatahike.JSONAsTxData(CTypeConversion.toJavaString(txDataJson),
                                                   Datahike.deref(conn));
        return toCCharPointer(libdatahike.toJSONString(Datahike.transact(conn, txData).get(Util.kwd(":tx-meta"))));
    }

}
