package datahike.impl;

import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CConst;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import datahike.java.Datahike;
import datahike.java.Util;
import datahike.impl.libdatahike;
import java.util.Date;

/**
 * Generated C entry points for libdatahike.
 *
 * This file is auto-generated from datahike.api.specification.
 * DO NOT EDIT MANUALLY - changes will be overwritten.
 *
 * To regenerate: bb codegen-native
 *
 * All entry points use callback-based return (OutputReader) to:
 * - Avoid shared mutable memory between native and JVM
 * - Support multiple output formats (json, edn, cbor)
 * - Enable proper exception handling
 *
 * Input format strings for temporal queries:
 * - "db" : Current database state
 * - "history" : Full history including retractions
 * - "since:{timestamp_ms}" : Database since timestamp
 * - "asof:{timestamp_ms}" : Database as-of timestamp
 */
public final class LibDatahike extends LibDatahikeBase {
    /**
     * Deletes a database given via configuration map.
   * 
   * Examples:
   *   Delete database:
   *     (delete-database {:store {:backend :memory :id "example"}})
     */
    @CEntryPoint(name = "delete_database")
    public static void delete_database(
            @CEntryPoint.IsolateThreadContext long isolateId,
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
    /**
     * Fetches data using recursive declarative pull pattern.
   * 
   * Examples:
   *   Pull with pattern:
   *     (pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)
   *   Pull with arg-map:
   *     (pull db {:selector [:db/id :name] :eid 1})
     */
    @CEntryPoint(name = "pull")
    public static void pull(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer selector_edn,
            long eid,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.pull(loadInput(input_format, raw_input), CTypeConversion.toJavaString(selector_edn), eid)));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Retrieves an entity by its id. Returns lazy map-like structure.
   * 
   * Examples:
   *   Get entity by id:
   *     (entity db 1)
   *   Get entity by lookup ref:
   *     (entity db [:email "alice@example.com"])
   *   Navigate entity attributes:
   *     (:name (entity db 1))
     */
    @CEntryPoint(name = "entity")
    public static void entity(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            long eid,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, entityToMap(Datahike.entity(loadInput(input_format, raw_input), eid))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Returns database metrics (datom counts, index sizes, etc).
   * 
   * Examples:
   *   Get metrics:
   *     (metrics @conn)
     */
    @CEntryPoint(name = "metrics")
    public static void metrics(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.metrics(loadInput(input_format, raw_input))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Returns reverse schema definition (attribute id to ident mapping).
   * 
   * Examples:
   *   Get reverse schema:
   *     (reverse-schema @conn)
     */
    @CEntryPoint(name = "reverse_schema")
    public static void reverse_schema(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.reverseSchema(loadInput(input_format, raw_input))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Index lookup. Returns sequence of datoms matching index components.
   * 
   * Examples:
   *   Find all datoms for entity:
   *     (datoms db {:index :eavt :components [1]})
   *   Find datoms for entity and attribute:
   *     (datoms db {:index :eavt :components [1 :likes]})
   *   Find by attribute and value (requires :db/index):
   *     (datoms db {:index :avet :components [:likes "pizza"]})
     */
    @CEntryPoint(name = "datoms")
    public static void datoms(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer index_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, datomsToVecs((Iterable<?>)Datahike.datoms(loadInput(input_format, raw_input), parseKeyword(index_edn)))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Executes a datalog query.
   * 
   * Examples:
   *   Query with vector syntax:
   *     (q '[:find ?value :where [_ :likes ?value]] db)
   *   Query with map syntax:
   *     (q '{:find [?value] :where [[_ :likes ?value]]} db)
   *   Query with pagination:
   *     (q {:query '[:find ?value :where [_ :likes ?value]]
                           :args [db]
                           :offset 2
                           :limit 10})
     */
    @CEntryPoint(name = "q")
    public static void q(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer query_edn,
            long num_inputs,
            @CConst CCharPointerPointer input_formats,
            @CConst CCharPointerPointer raw_inputs,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.q(CTypeConversion.toJavaString(query_edn), loadInputs(num_inputs, input_formats, raw_inputs))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Returns current schema definition.
   * 
   * Examples:
   *   Get schema:
   *     (schema @conn)
     */
    @CEntryPoint(name = "schema")
    public static void schema(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.schema(loadInput(input_format, raw_input))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Returns part of :avet index between start and end values.
   * 
   * Examples:
   *   Find datoms in value range:
   *     (index-range db {:attrid :likes :start "a" :end "z"})
   *   Find entities with age in range:
   *     (->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))
     */
    @CEntryPoint(name = "index_range")
    public static void index_range(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer attrid_edn,
            @CConst CCharPointer start_edn,
            @CConst CCharPointer end_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, datomsToVecs((Iterable<?>)Datahike.indexRange(loadInput(input_format, raw_input), Util.map(Util.kwd(":attrid"), parseKeyword(attrid_edn), Util.kwd(":start"), libdatahike.parseEdn(CTypeConversion.toJavaString(start_edn)), Util.kwd(":end"), libdatahike.parseEdn(CTypeConversion.toJavaString(end_edn)))))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Same as pull, but accepts sequence of ids and returns sequence of maps.
   * 
   * Examples:
   *   Pull multiple entities:
   *     (pull-many db [:db/id :name] [1 2 3])
     */
    @CEntryPoint(name = "pull_many")
    public static void pull_many(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer selector_edn,
            @CConst CCharPointer eids_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.pullMany(loadInput(input_format, raw_input), CTypeConversion.toJavaString(selector_edn), parseIterable(eids_edn))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Invokes garbage collection on connection's store. Removes old snapshots before given time point.
   * 
   * Examples:
   *   GC all old snapshots:
   *     (gc-storage conn)
   *   GC snapshots before date:
   *     (gc-storage conn (java.util.Date.))
     */
    @CEntryPoint(name = "gc_storage")
    public static void gc_storage(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            long before_tx_unix_time_ms,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.gcStorage(Datahike.connect(readConfig(db_config)), new Date(before_tx_unix_time_ms))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Creates a database via configuration map.
   * 
   * Examples:
   *   Create empty database:
   *     (create-database {:store {:backend :memory :id "example"}})
   *   Create with schema-flexibility :read:
   *     (create-database {:store {:backend :memory :id "example"} :schema-flexibility :read})
   *   Create without history:
   *     (create-database {:store {:backend :memory :id "example"} :keep-history? false})
   *   Create with initial schema:
   *     (create-database {:store {:backend :memory :id "example"}
                                          :initial-tx [{:db/ident :name
                                                        :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one}]})
     */
    @CEntryPoint(name = "create_database")
    public static void create_database(
            @CEntryPoint.IsolateThreadContext long isolateId,
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
    /**
     * Checks if a database exists via configuration map.
   * 
   * Examples:
   *   Check if in-memory database exists:
   *     (database-exists? {:store {:backend :memory :id "example"}})
   *   Check with default config:
   *     (database-exists?)
     */
    @CEntryPoint(name = "database_exists")
    public static void database_exists(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, Datahike.databaseExists(readConfig(db_config))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Applies transaction to the database and updates connection.
   * 
   * Examples:
   *   Add single datom:
   *     (transact conn [[:db/add 1 :name "Ivan"]])
   *   Retract datom:
   *     (transact conn [[:db/retract 1 :name "Ivan"]])
   *   Create entity with tempid:
   *     (transact conn [[:db/add -1 :name "Ivan"]])
   *   Create entity (map form):
   *     (transact conn [{:db/id -1 :name "Ivan" :likes ["fries" "pizza"]}])
   *   Read from stdin (CLI):
   *     
     */
    @CEntryPoint(name = "transact")
    public static void transact(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer db_config,
            @CConst CCharPointer tx_format,
            @CConst CCharPointer tx_data,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            Object conn = Datahike.connect(readConfig(db_config));
            Object txData = loadInput(tx_format, tx_data);

            // Apply schema-aware type conversions for JSON format
            // (e.g., Integer → Long for :db.type/long attributes)
            String format = CTypeConversion.toJavaString(tx_format);
            if ("json".equals(format)) {
                txData = libdatahike.transformJSONForTx(txData, conn);
            }

            output_reader.call(toOutput(output_format, ((java.util.Map)Datahike.transact(conn, (java.util.List)txData)).get(Util.kwd(":tx-meta"))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    /**
     * Like datoms, but returns datoms starting from specified components through end of index.
   * 
   * Examples:
   *   Seek from entity:
   *     (seek-datoms db {:index :eavt :components [1]})
     */
    @CEntryPoint(name = "seek_datoms")
    public static void seek_datoms(
            @CEntryPoint.IsolateThreadContext long isolateId,
            @CConst CCharPointer input_format,
            @CConst CCharPointer raw_input,
            @CConst CCharPointer index_edn,
            @CConst CCharPointer output_format,
            @CConst OutputReader output_reader) {
        try {
            output_reader.call(toOutput(output_format, datomsToVecs((Iterable<?>)Datahike.seekDatoms(loadInput(input_format, raw_input), parseKeyword(index_edn)))));
        } catch (Exception e) {
            output_reader.call(toException(e));
        }
    }
    // Note: history/since/as-of are handled via input format strings
    // e.g., input_format="history" or "since:1234567890"

    // seekdatoms returns lazy sequence - results are fully realized
    // release not exposed - connections are created internally per call
}
