/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.RelationKind;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TypeDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * The storage part of the conformance kit: every kit type through INSERT, out-of-order merge, WAL
 * apply, column tops, dedup, ALTER COLUMN TYPE and partition-to-Parquet conversion, read through
 * record cursors and through page frames, over the table shapes of {@link TypeConformanceValues},
 * and as the key of LATEST ON ... PARTITION BY on each table mode (the SQL path
 * {@code sql.latest_by_key}, run here because this class has the WAL and non-partitioned tables).
 * {@code storage.parquet_convert} converts a Parquet partition's VARCHAR column into the type: the
 * values as they print go into a VARCHAR column, the partition goes to Parquet, the column's type
 * changes to the type, which a read converts on the fly, and the partition comes back to native
 * storage, which converts it for good.
 * <p>
 * Each path runs in the modes that change it (WAL and non-WAL, in-order and out-of-order,
 * partitioned and not), and every mode must give the same recording
 * ({@link TypeConformanceRecording}); a difference between modes is a failure. The page-frame
 * sections print each row's stored bytes, so a change in the column files fails here byte for
 * byte. Types registered later are checked by {@link TypeConformanceInvariants} on the paths their
 * resource line lists.
 * <p>
 * The paths include these edge cases: ADD COLUMN between WAL transactions that one
 * {@code drainWalQueue} applies together, out-of-order writes into partitions with column tops,
 * and the SYMBOL table after NULL writes.
 */
@RunWith(Parameterized.class)
public class TypeConformanceStorageTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final Pattern INCOMPATIBLE = Pattern.compile("error: alter: \\[(\\d+)] incompatible column type change \\[existing=([^,\\]]+), new=([^\\]]+)]");
    // the refusal of a row that goes back in time on a non-partitioned non-WAL table
    private static final String OUT_OF_ORDER_REFUSAL = "cannot insert rows out of order to non-partitioned table";
    // the guarded site the conversion of a Parquet partition's column into the type reaches
    private static final String PARQUET_SITE = "Parquet conversion";
    private static final String[] PARTITIONED_MODES = {"nonwal-day", "wal-day"};
    private static final Map<String, String> RECORDINGS = new HashMap<>();
    private static final long SECOND = TypeConformanceValues.SECOND;
    private static final String[] TABLE_MODES = {"nonwal-none", "nonwal-day", "wal-day"};
    private final ObjList<TypeConformanceValues.Row> rows;
    private final TypeConformanceTypes.Entry type;

    public TypeConformanceStorageTest(String label) {
        this.type = TypeConformanceTypes.byLabel(label);
        this.rows = TypeConformanceValues.rowsOf(type);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return TypeConformanceTypes.parameters();
    }

    @Test
    public void testAlterColumnType() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : PARTITIONED_MODES) {
                if (!TypeConformanceInvariants.isEnabled(type, "storage.alter", mode)) {
                    continue;
                }
                if (type.isLater()) {
                    checkLaterAlter(mode);
                    continue;
                }
                // one header line of row labels, then one line of values per target
                final StringSink section = new StringSink();
                section.put("target");
                for (String prefix : new String[]{"d0:", "d1:"}) {
                    for (int i = 0, n = rows.size(); i < n; i++) {
                        section.put(prefix.equals("d0:") && i == 0 ? '\t' : '|').put(prefix).put(rows.getQuick(i).label);
                    }
                }
                section.put('\n');
                for (int t = 0, n = TypeConformanceTypes.ALL.size(); t < n; t++) {
                    final TypeConformanceTypes.Entry target = TypeConformanceTypes.ALL.getQuick(t);
                    if (target.isLater()) {
                        continue;
                    }
                    final String table = "alter_" + (t < 10 ? "0" : "") + t;
                    final StringSink steps = new StringSink();
                    // day 0 is a column top of v, day 1 holds every value row
                    createTable(table, mode, "k VARCHAR", steps);
                    insertRows(table, mode, "d0:", 0, false, steps);
                    step(table, "add column", "ALTER TABLE " + table + " ADD COLUMN v " + type.ddl, mode, steps);
                    insertRows(table, mode, "d1:", DAY, true, steps);
                    if (steps.length() > 0) {
                        // the source column cannot be set up: no target can run
                        section.put("*\t").put(oneLine(steps)).put('\n');
                        execute("DROP TABLE IF EXISTS " + table);
                        break;
                    }
                    section.put(target.label).put('\t');
                    final StringSink alter = new StringSink();
                    step(table, "alter", "ALTER TABLE " + table + " ALTER COLUMN v TYPE " + target.ddl, mode, alter);
                    if (alter.length() > 0) {
                        section.put(abbreviate(oneLine(alter), target)).put('\n');
                    } else {
                        section.put(valuesLine("SELECT k, v FROM " + table)).put('\n');
                    }
                    execute("DROP TABLE IF EXISTS " + table);
                }
                assertSection("alter", mode, section);
            }
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : PARTITIONED_MODES) {
                if (!TypeConformanceInvariants.isEnabled(type, "storage.tops", mode)) {
                    continue;
                }
                final String table = "tops_" + code(mode);
                final StringSink steps = new StringSink();
                // day 0: absent partition; day 1: the column is added half way; day 2: full
                createTable(table, mode, "k VARCHAR", steps);
                final int half = rows.size() / 2;
                final boolean isWal = isWal(mode);
                insertRowsRange(table, "d0:", 0, 0, rows.size(), false, steps);
                insertRowsRange(table, "d1:", DAY, 0, half, false, steps);
                // WAL: the ADD COLUMN lands between WAL transactions that one drain applies together
                final int beforeAdd = steps.length();
                stepNoDrain("add column", "ALTER TABLE " + table + " ADD COLUMN v " + type.ddl, steps);
                if (steps.length() > beforeAdd) {
                    if (isWal) {
                        drainWalQueue();
                    }
                    failed(mode, steps, "tops", "tops-frames", "tops-o3", "tops-o3-frames");
                    continue;
                }
                if (type.isLater()) {
                    writeLaterRows(table, "d1:", DAY, half, rows.size(), 2, steps);
                    writeLaterRows(table, "d2:", 2 * DAY, 0, rows.size(), 2, steps);
                } else {
                    insertRowsRange(table, "d1:", DAY, half, rows.size(), true, steps);
                    insertRowsRange(table, "d2:", 2 * DAY, 0, rows.size(), true, steps);
                }
                if (isWal) {
                    drainWalQueue();
                }
                if (type.isLater()) {
                    checkLaterTops(table, mode, steps);
                    continue;
                }
                assertSection("tops", mode, steps + query("SELECT k, v FROM " + table));
                assertSection("tops-frames", mode, frames("SELECT k, v FROM " + table));
                // out-of-order rows into the partition with a column top and into the absent one
                final StringSink o3Steps = new StringSink();
                insertRowsRange(table, "o3d1:", DAY + SECOND / 2, 0, rows.size(), true, o3Steps);
                insertRowsRange(table, "o3d0:", SECOND / 2, 0, rows.size(), true, o3Steps);
                if (isWal) {
                    drainWalQueue();
                }
                assertSection("tops-o3", mode, o3Steps + query("SELECT k, v FROM " + table));
                assertSection("tops-o3-frames", mode, frames("SELECT k, v FROM " + table));
            }
        });
    }

    @Test
    public void testDedup() throws Exception {
        assertMemoryLeak(() -> {
            final String mode = "wal-day";
            if (!TypeConformanceInvariants.isEnabled(type, "storage.dedup", mode)) {
                return;
            }
            if (type.isLater()) {
                checkLaterDedup(mode);
                return;
            }
            final String table = "dedup_t";
            final StringSink steps = new StringSink();
            step(table, "create", "CREATE TABLE " + table + " (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)", mode, steps);
            if (steps.length() == 0) {
                insertRows(table, mode, "", 0, true, steps);
                // the same keys again: upserts replace k
                insertRows(table, mode, "dup:", 0, true, steps);
                // same timestamps, each with the next row's value: new keys unless the values are equal
                final ObjList<TypeConformanceValues.Row> literalRows = new ObjList<>();
                for (int i = 0, n = rows.size(); i < n; i++) {
                    if (rows.getQuick(i).literal != null) {
                        literalRows.add(rows.getQuick(i));
                    }
                }
                final StringSink values = new StringSink();
                for (int i = 0, n = literalRows.size(); i < n; i++) {
                    if (i > 0) {
                        values.put(", ");
                    }
                    values.put("('shift:").put(literalRows.getQuick(i).label).put("', ")
                            .put(literalRows.getQuick((i + 1) % n).literal).put(", ").put(i * SECOND).put("::TIMESTAMP)");
                }
                step(table, "insert shift:", "INSERT INTO " + table + " (k, v, ts) VALUES " + values, mode, steps);
            }
            assertSection("dedup", mode, steps + query("SELECT k, v FROM " + table));
            assertSection("dedup-frames", mode, frames("SELECT k, v FROM " + table));
        });
    }

    @Test
    public void testInsert() throws Exception {
        assertMemoryLeak(() -> {
            for (String tableMode : TABLE_MODES) {
                for (int o3 = 0; o3 < 2; o3++) {
                    final boolean isO3 = o3 == 1;
                    final String mode = isO3 ? tableMode + "-o3" : tableMode;
                    if (!TypeConformanceInvariants.isEnabled(type, "storage.insert", mode)) {
                        continue;
                    }
                    final String table = "ins_" + code(mode);
                    final StringSink steps = new StringSink();
                    // a non-partitioned non-WAL table refuses out-of-order rows: its own sections;
                    // out-of-order writes store SYMBOL keys in another order: own frames section
                    final String insertSection = "nonwal-none-o3".equals(mode) ? "insert-o3-none" : "insert";
                    final String framesSection = "nonwal-none-o3".equals(mode) ? "frames-o3-none" : isO3 ? "frames-o3" : "frames";
                    if (!createTable(table, tableMode, "k VARCHAR, v " + type.ddl, steps, insertSection, framesSection)) {
                        continue;
                    }
                    if (type.isLater()) {
                        if (isO3) {
                            // even rows first, then the odd rows go back in time
                            writeLaterRows(table, "", 0, 0, rows.size(), 2, steps);
                            if ("nonwal-none-o3".equals(mode)) {
                                // the non-partitioned non-WAL table refuses the odd rows
                                final StringSink o3Steps = new StringSink();
                                writeLaterRows(table, "", 0, 1, rows.size(), 2, o3Steps);
                                assertOutOfOrderRefused(table, mode, 1, 2, o3Steps);
                            } else {
                                writeLaterRows(table, "", 0, 1, rows.size(), 2, steps);
                            }
                        } else {
                            writeLaterRows(table, "", 0, 0, rows.size(), 1, steps);
                        }
                        if (isWal(tableMode)) {
                            drainWalQueue();
                        }
                        checkLaterRows(table, "", "storage.insert", mode, steps);
                        continue;
                    }
                    if (isO3) {
                        insertRowsStep(table, tableMode, "", 0, 0, 2, steps);
                        insertRowsStep(table, tableMode, "", 0, 1, 2, steps);
                    } else {
                        insertRows(table, tableMode, "", 0, true, steps);
                    }
                    assertSection(insertSection, mode, steps + query("SELECT k, v FROM " + table));
                    assertSection(framesSection, mode, frames("SELECT k, v FROM " + table));
                }
            }
        });
    }

    @Test
    public void testLatestByKey() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : TABLE_MODES) {
                final String path = "sql.latest_by_key";
                if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                    continue;
                }
                final String table = "lb_" + code(mode);
                final StringSink steps = new StringSink();
                if (!createTable(table, mode, "k VARCHAR, v " + type.ddl, steps, "latest_by_key")) {
                    continue;
                }
                // every value row twice: a: first, then b: one second after the last a: row
                final long base = rows.size() * SECOND;
                if (type.isLater()) {
                    writeLaterRows(table, "a:", 0, 0, rows.size(), 1, steps);
                    writeLaterRows(table, "b:", base, 0, rows.size(), 1, steps);
                    if (isWal(mode)) {
                        drainWalQueue();
                    }
                    checkLaterLatestByKey(table, mode, steps);
                    continue;
                }
                insertRows(table, mode, "a:", 0, true, steps);
                insertRows(table, mode, "b:", base, true, steps);
                assertSection("latest_by_key", mode, steps + query("SELECT k, v FROM (" + table + " LATEST ON ts PARTITION BY v) ORDER BY k"));
            }
        });
    }

    @Test
    public void testParquet() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : PARTITIONED_MODES) {
                if (!TypeConformanceInvariants.isEnabled(type, "storage.parquet", mode)) {
                    continue;
                }
                final String table = "pq_" + code(mode);
                final StringSink steps = new StringSink();
                if (!createTable(table, mode, "k VARCHAR, v " + type.ddl, steps, "parquet", "parquet-frames", "parquet-native", "parquet-native-frames")) {
                    continue;
                }
                if (type.isLater()) {
                    writeLaterRows(table, "d0:", 0, 0, rows.size(), 1, steps);
                    writeLaterRows(table, "d1:", DAY, 0, rows.size(), 1, steps);
                    if (isWal(mode)) {
                        drainWalQueue();
                    }
                } else {
                    insertRows(table, mode, "d0:", 0, true, steps);
                    insertRows(table, mode, "d1:", DAY, true, steps);
                }
                // the last partition is active and stays native
                step(table, "to parquet", "ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'", mode, steps);
                if (type.isLater()) {
                    checkLaterRows(table, "d0:", "storage.parquet", mode, steps);
                } else {
                    assertSection("parquet", mode, steps + query("SELECT k, v FROM " + table));
                    assertSection("parquet-frames", mode, frames("SELECT k, v FROM " + table));
                }
                final StringSink back = new StringSink();
                step(table, "to native", "ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE WHERE ts < '1970-01-02'", mode, back);
                if (type.isLater()) {
                    // a NOT NULL type refused its NULL row with the writes above, not in this step
                    final StringSink nativeSteps = new StringSink();
                    nativeSteps.put(steps).put(back);
                    checkLaterRows(table, "d0:", "storage.parquet", mode + "-native", nativeSteps);
                } else {
                    assertSection("parquet-native", mode, back + query("SELECT k, v FROM " + table));
                    assertSection("parquet-native-frames", mode, frames("SELECT k, v FROM " + table));
                }
            }
        });
    }

    @Test
    public void testParquetConvert() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : PARTITIONED_MODES) {
                final String path = "storage.parquet_convert";
                if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                    continue;
                }
                final String table = "pc_" + code(mode);
                final StringSink steps = new StringSink();
                // the values as they print, and which rows read as NULL, from a table of the type
                final String source = "pcs_" + code(mode);
                if (!createTable(source, "nonwal-day", "k VARCHAR, v " + type.ddl, steps, "parquet_convert")) {
                    continue;
                }
                if (type.isLater()) {
                    writeLaterRows(source, "", 0, 0, rows.size(), 1, steps);
                } else {
                    insertRows(source, "nonwal-day", "", 0, true, steps);
                }
                final Map<String, String> texts = new HashMap<>();
                final Map<String, long[]> sourceBits = new HashMap<>();
                if (type.isLater()) {
                    readLater(source, texts, sourceBits);
                } else {
                    final StringSink sink = new StringSink();
                    printSql("SELECT k, v FROM " + source, sink);
                    final String[] lines = sink.toString().split("\n");
                    for (int i = 1; i < lines.length; i++) {
                        final int tab = lines[i].indexOf('\t');
                        texts.put(lines[i].substring(0, tab), lines[i].substring(tab + 1));
                    }
                }
                final StringSink nulls = new StringSink();
                try {
                    printSql("SELECT k FROM " + source + " WHERE v IS NULL", nulls);
                } catch (Throwable e) {
                    // a type without IS NULL (the arrays) has only the NULL row as NULL
                    nulls.clear();
                    nulls.put("k\nnull\n");
                }
                stepNoDrain("drop", "DROP TABLE " + source, steps);
                if (!createTable(table, mode, "k VARCHAR, v VARCHAR", steps, "parquet_convert")) {
                    continue;
                }
                final StringSink values = new StringSink();
                for (int i = 0, n = rows.size(); i < n; i++) {
                    final String label = rows.getQuick(i).label;
                    final String text = texts.get(label);
                    values.put(values.length() > 0 ? ", " : "").put("('").put(label).put("', ");
                    if (text == null || ("\n" + nulls).contains("\n" + label + "\n")) {
                        values.put("NULL");
                    } else {
                        values.put('\'').put(text.replace("'", "''")).put('\'');
                    }
                    values.put(", ").put(i * SECOND).put("::TIMESTAMP)");
                }
                // a row on the next day keeps the partition of the value rows inactive
                step(table, "insert", "INSERT INTO " + table + " (k, v, ts) VALUES " + values + ", ('active', NULL, " + DAY + "::TIMESTAMP)", mode, steps);
                step(table, "to parquet", "ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'", mode, steps);
                step(table, "alter", "ALTER TABLE " + table + " ALTER COLUMN v TYPE " + type.ddl, mode, steps);
                final String read = "SELECT k, v FROM " + table + " WHERE ts < '1970-01-02'";
                // read through the Parquet partition, which converts the column on the fly
                final String parquetRead = type.isLater() ? null : query(read);
                final StringSink conversion = new StringSink();
                step(table, "to native", "ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE WHERE ts < '1970-01-02'", mode, conversion);
                if (type.isLater()) {
                    if (TypeConformanceInvariants.assertDeclaredRefusal(type, "-", path, mode, conversion.length() > 0 ? conversion : null, PARQUET_SITE)) {
                        continue;
                    }
                    steps.put(conversion);
                    checkLaterParquetConvert(table, mode, steps, sourceBits);
                    continue;
                }
                assertSection("parquet_convert", mode, steps + "parquet\n" + parquetRead + conversion + "native\n" + query(read));
            }
        });
    }

    @Test
    public void testShapes() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : TABLE_MODES) {
                if (!TypeConformanceInvariants.isEnabled(type, "storage.shapes", mode)) {
                    continue;
                }
                final String suffix = code(mode);
                final String empty = "empty_" + suffix;
                final StringSink steps = new StringSink();
                if (!createTable(empty, mode, "k VARCHAR, v " + type.ddl, steps, TypeConformanceValues.SHAPE_EMPTY_TABLE,
                        TypeConformanceValues.SHAPE_EMPTY_TABLE + "-frames", TypeConformanceValues.SHAPE_SINGLE_ROW,
                        TypeConformanceValues.SHAPE_SINGLE_ROW + "-frames", TypeConformanceValues.SHAPE_EMPTY_PARTITION,
                        TypeConformanceValues.SHAPE_EMPTY_PARTITION + "-frames")) {
                    continue;
                }
                if (type.isLater()) {
                    assertNoRows(empty, "SELECT k, v FROM " + empty, TypeConformanceValues.SHAPE_EMPTY_TABLE, mode, steps);
                } else {
                    assertSection(TypeConformanceValues.SHAPE_EMPTY_TABLE, mode, steps + query("SELECT k, v FROM " + empty));
                    assertSection(TypeConformanceValues.SHAPE_EMPTY_TABLE + "-frames", mode, frames("SELECT k, v FROM " + empty));
                }

                final String single = "single_" + suffix;
                steps.clear();
                createTable(single, mode, "k VARCHAR, v " + type.ddl, steps);
                if (type.isLater()) {
                    writeLaterRows(single, "", 0, 0, 1, 1, steps);
                    if (isWal(mode)) {
                        drainWalQueue();
                    }
                    checkLaterRows(single, "", "storage.shapes", mode, steps);
                } else {
                    insertRowsRange(single, "", 0, 0, 1, true, steps);
                    if (isWal(mode)) {
                        drainWalQueue();
                    }
                    assertSection(TypeConformanceValues.SHAPE_SINGLE_ROW, mode, steps + query("SELECT k, v FROM " + single));
                    assertSection(TypeConformanceValues.SHAPE_SINGLE_ROW + "-frames", mode, frames("SELECT k, v FROM " + single));
                }

                // day 1 lies between two partitions; the afternoon of day 0 inside one, with no row
                final String gap = "gap_" + suffix;
                steps.clear();
                createTable(gap, mode, "k VARCHAR, v " + type.ddl, steps);
                if (type.isLater()) {
                    writeLaterRows(gap, "d0:", 0, 0, rows.size(), 1, steps);
                    writeLaterRows(gap, "d2:", 2 * DAY, 0, rows.size(), 1, steps);
                    if (isWal(mode)) {
                        drainWalQueue();
                    }
                } else {
                    insertRows(gap, mode, "d0:", 0, true, steps);
                    insertRows(gap, mode, "d2:", 2 * DAY, true, steps);
                }
                final String between = "SELECT k, v FROM " + gap + " WHERE ts IN '1970-01-02'";
                final String inside = "SELECT k, v FROM " + gap + " WHERE ts IN '1970-01-01T12'";
                if (type.isLater()) {
                    final StringSink noSteps = new StringSink();
                    assertNoRows(gap, between, TypeConformanceValues.SHAPE_EMPTY_PARTITION, mode, noSteps);
                    assertNoRows(gap, inside, TypeConformanceValues.SHAPE_EMPTY_PARTITION, mode, noSteps);
                } else {
                    assertSection(TypeConformanceValues.SHAPE_EMPTY_PARTITION, mode, steps + query(between) + query(inside));
                    assertSection(TypeConformanceValues.SHAPE_EMPTY_PARTITION + "-frames", mode, frames(between) + frames(inside));
                }
            }
        });
    }

    @Test
    public void testSymbolNullAppenderRefuses() throws Exception {
        // the table and WAL writers write a SYMBOL NULL themselves, the key and the symbol map's
        // NULL flag together; the type driver's generic appender, which would write the key alone,
        // refuses to be built
        Assume.assumeTrue("the symbol map exists for SYMBOL only", type.columnType == ColumnType.SYMBOL);
        assertMemoryLeak(() -> {
            try (MemoryCARW mem = Vm.getCARWInstance(4096, 1, MemoryTag.NATIVE_DEFAULT)) {
                try {
                    ColumnType.getTypeDriver(type.columnType).newNullAppender(mem, null);
                    Assert.fail("a generic SYMBOL NULL appender must refuse");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "through the symbol map");
                }
            }
        });
    }

    @Test
    public void testSymbolTable() throws Exception {
        Assume.assumeTrue("the symbol table exists for SYMBOL only", type.columnType == ColumnType.SYMBOL);
        assertMemoryLeak(() -> {
            for (String mode : TABLE_MODES) {
                final String table = "sym_" + code(mode);
                final StringSink steps = new StringSink();
                createTable(table, mode, "k VARCHAR, v " + type.ddl, steps);
                insertRows(table, mode, "", 0, true, steps);
                final StringSink sink = new StringSink();
                sink.put(steps);
                try (TableReader reader = getReader(table)) {
                    final SymbolTable symbols = reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("v"));
                    final int count = reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("v")).getSymbolCount();
                    sink.put("symbol_count\t").put(count).put('\n');
                    for (int key = 0; key < count; key++) {
                        sink.put(key).put('\t').put(symbols.valueOf(key)).put('\n');
                    }
                    sink.put("null key\t").put(symbols.valueOf(SymbolTable.VALUE_IS_NULL)).put('\n');
                }
                sink.put(query("SELECT count_distinct(v) FROM " + table));
                assertSection("symbols", mode, TypeConformanceRecording.escape(sink));
            }
        });
    }

    private static void appendHex(StringSink sink, long address, long size) {
        for (long i = 0; i < size; i++) {
            final int b = Unsafe.getByte(address + i) & 0xFF;
            sink.put(Character.forDigit(b >> 4, 16)).put(Character.forDigit(b & 0xF, 16));
        }
    }

    /**
     * A table-name suffix of the same length for every mode, so that error positions and
     * table names in messages do not differ between modes.
     */
    private static String code(String mode) {
        final boolean isO3 = mode.endsWith("-o3");
        final String base = isO3 ? mode.substring(0, mode.length() - 3) : mode;
        final String code = switch (base) {
            case "nonwal-none" -> "n0";
            case "nonwal-day" -> "n1";
            case "wal-day" -> "w1";
            default -> throw new AssertionError(mode);
        };
        return code + (isO3 ? "o" : "a");
    }

    private static boolean contains(short[] row, short tag) {
        for (short t : row) {
            if (t == tag) {
                return true;
            }
        }
        return false;
    }

    private static boolean isWal(String mode) {
        return mode.startsWith("wal");
    }

    /**
     * Shortens the refusal every incompatible pair gives, {@code error: alter: [p] incompatible
     * column type change [existing=S, new=T]} with this section's S and T, to
     * {@code incompatible [p]}; any other text stays as it is, so the recording loses nothing.
     */
    private String abbreviate(String error, TypeConformanceTypes.Entry target) {
        final Matcher matcher = INCOMPATIBLE.matcher(error);
        if (matcher.matches()
                && matcher.group(2).equals(ColumnType.nameOf(type.columnType))
                && matcher.group(3).equals(ColumnType.nameOf(target.columnType))) {
            return "incompatible [" + matcher.group(1) + "]";
        }
        return error;
    }

    private static String oneLine(CharSequence text) {
        final String s = text.toString();
        return s.endsWith("\n") ? s.substring(0, s.length() - 1).replace('\n', ' ') : s.replace('\n', ' ');
    }


    // a widening (rule W) after ALTER gives each row's value by the declared tier
    private void addWideningGaps(String table, String pair, TypeConformanceTypes.Entry target, ObjList<String> gaps) throws Exception {
        if (type.laterTier == null) {
            return;
        }
        final RelationKind targetKind = TypeConformanceInvariants.kindOf(target.columnType);
        final int targetWidth = TypeConformanceInvariants.widthOf(target.columnType);
        if (targetWidth <= 0) {
            return;
        }
        final Map<String, long[]> actual = new HashMap<>();
        try (
                RecordCursorFactory factory = select("SELECT k, v FROM " + table);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                actual.put(record.getVarcharA(0).toString(), TypeConformanceValues.readBits(record, 1, targetWidth));
            }
        }
        final boolean isSentinelNull = TypeConformanceInvariants.POLICY_SENTINEL.equals(TypeConformanceInvariants.policyOf(type));
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            final long[] value = actual.get("d1:" + row.label);
            if (row.isNull() || value == null || (isSentinelNull && "sentinel".equals(row.label))) {
                continue;
            }
            final long[] expected = TypeConformanceInvariants.widened(type, row.bits, targetKind, targetWidth);
            if (expected != null && !TypeConformanceInvariants.isSameValue(targetKind, targetWidth, expected, value)) {
                gaps.add(pair + ": row " + row.label + " converts to " + Arrays.toString(value) + ", tier " + type.laterTier
                        + " gives " + Arrays.toString(expected));
            }
        }
    }

    // a WAL apply that failed suspends the table; the failure names the step and the apply error
    private void assertApplied(String table, String step, String path, String mode) throws Exception {
        final Map<String, String> status = texts("SELECT suspended::STRING || ' ' || coalesce(errorMessage, '') k, 'x' v FROM wal_tables() WHERE name = '" + table + "'");
        for (String line : status.keySet()) {
            if (line.startsWith("true")) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": the WAL apply of " + step
                        + " suspended the table: " + line.substring(5));
            }
        }
    }

    private void assertNoRows(String table, String sql, String shape, String mode, StringSink steps) throws Exception {
        if (steps.length() > 0) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", "storage.shapes", mode) + ": " + table + ": " + steps);
        }
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            if (cursor.hasNext()) {
                throw new AssertionError(TypeConformanceInvariants.context(type, cursor.getRecord().getVarcharA(0).toString(), "storage.shapes", mode)
                        + ": " + shape + " must read no row");
            }
        }
    }

    /**
     * {@code storage.insert} on the non-partitioned non-WAL table for a type registered later: the
     * table refuses rows that go back in time, as the existing types' {@code insert-o3-none}
     * sections record, so every step of the out-of-order write is that refusal and none of the
     * rows {@code lo, lo + step, ...} is in the table. The rows written in order are checked as in
     * every other mode.
     */
    private void assertOutOfOrderRefused(String table, String mode, int lo, int step, CharSequence o3Steps) throws Exception {
        final String path = "storage.insert";
        if (o3Steps.length() == 0) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": the rows that go back in time were written");
        }
        for (String line : o3Steps.toString().split("\n")) {
            if (!line.startsWith("error: ") || !line.contains(OUT_OF_ORDER_REFUSAL)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                        + ": the rows that go back in time must be refused as out of order, but: " + o3Steps);
            }
        }
        final Map<String, String> texts = new HashMap<>();
        readLater(table, texts, new HashMap<>());
        for (int i = lo, n = rows.size(); i < n; i += step) {
            final String label = rows.getQuick(i).label;
            if (texts.containsKey(label)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, label, path, mode) + ": the row goes back in time, but the table holds it");
            }
        }
    }

    private void assertSection(String path, String mode, CharSequence actual) {
        // mask: the database root of the test run
        final String masked = actual.toString().replace(root, "<dbRoot>");
        TypeConformanceRecording.assertSection(type, path, mode, RECORDINGS.get(type.label), masked);
    }

    /**
     * ALTER COLUMN TYPE of a type registered later, from its declared relations. For every
     * persisted kit type as the target: a conversion rule A does not admit is refused; one it
     * admits succeeds, reads the column top and the NULL row as the target's NULL literal reads
     * (where the type stores NULL), and for a widening (rule W) gives each row's value by the
     * declared tier. A failure names the pair and the rule, so a missing converter fails loudly.
     */
    private void checkLaterAlter(String mode) throws Exception {
        final String path = "storage.alter";
        final String policy = TypeConformanceInvariants.policyOf(type);
        final boolean isNullStored = TypeConformanceInvariants.POLICY_SENTINEL.equals(policy);
        final ObjList<String> gaps = new ObjList<>();
        for (int t = 0, n = TypeConformanceTypes.ALL.size(); t < n; t++) {
            final TypeConformanceTypes.Entry target = TypeConformanceTypes.ALL.getQuick(t);
            final short targetTag = ColumnType.tagOf(target.columnType);
            if (target.isLater() || targetTag == ColumnType.tagOf(type.columnType) || !ColumnType.isPersisted(targetTag)) {
                continue;
            }
            final String table = "alter_later_" + (t < 10 ? "0" : "") + t;
            final StringSink steps = new StringSink();
            // day 0 is a column top of v, day 1 holds every value row
            createTable(table, mode, "k VARCHAR", steps);
            insertRows(table, mode, "d0:", 0, false, steps);
            step(table, "add column", "ALTER TABLE " + table + " ADD COLUMN v " + type.ddl, mode, steps);
            insertRows(table, mode, "d1:", DAY, true, steps);
            try {
                TypeConformanceInvariants.nullRowWriteError(type, path, mode, steps);
            } catch (AssertionError e) {
                execute("DROP TABLE IF EXISTS " + table);
                throw e;
            }
            final boolean isAdmitted = contains(RelationRules.alter(ColumnType.tagOf(type.columnType)), targetTag);
            final String pair = type.label + " -> " + target.label + (isAdmitted ? " (rule A)" : " (not in rule A)");
            final StringSink alter = new StringSink();
            step(table, "alter", "ALTER TABLE " + table + " ALTER COLUMN v TYPE " + target.ddl, mode, alter);
            try {
                if (!isAdmitted) {
                    if (alter.length() == 0) {
                        gaps.add(pair + ": ALTER converted it");
                    }
                    continue;
                }
                if (alter.length() > 0) {
                    gaps.add(pair + ": no implementation: " + oneLine(alter));
                    continue;
                }
                final Map<String, String> texts = texts("SELECT k, v FROM " + table);
                final String nullLiteral = texts("SELECT 'null' k, CAST(NULL AS " + target.ddl + ") v FROM long_sequence(1)").get("null");
                for (int i = 0, m = rows.size(); i < m; i++) {
                    final String label = rows.getQuick(i).label;
                    final String top = texts.get("d0:" + label);
                    if (top != null && !top.equals(nullLiteral)) {
                        gaps.add(pair + ": the column-top row d0:" + label + " converts to " + top + ", a NULL literal reads " + nullLiteral);
                        break;
                    }
                }
                final String nullRow = texts.get("d1:null");
                if (isNullStored && nullRow != null && !nullRow.equals(nullLiteral)) {
                    gaps.add(pair + ": the NULL row converts to " + nullRow + ", a NULL literal reads " + nullLiteral);
                }
                if ("W".equals(TypeConformanceInvariants.castRule(type.columnType, target.columnType))) {
                    addWideningGaps(table, pair, target, gaps);
                }
            } finally {
                execute("DROP TABLE IF EXISTS " + table);
            }
        }
        if (gaps.size() > 0) {
            final StringBuilder message = new StringBuilder(TypeConformanceInvariants.context(type, "-", path, mode))
                    .append(": ").append(gaps.size()).append(" conversions break an invariant:");
            for (int i = 0, n = gaps.size(); i < n; i++) {
                message.append("\n  ").append(gaps.getQuick(i));
            }
            throw new AssertionError(message.toString());
        }
    }

    /**
     * Dedup on a type registered later. SQL refuses only arrays as dedup keys, so every persisted
     * type except an array can be one. Writing the rows again replaces k; rows at the same
     * timestamps with the next row's value replace k where the two values are one key, and add a
     * row where they are not. Two values are one key when their bits are equal; the NULL row is the
     * sentinel-pattern row's key under SENTINEL and the zero row's under NONE.
     */
    private void checkLaterDedup(String mode) throws Exception {
        final String path = "storage.dedup";
        final String table = "dedup_t";
        final StringSink steps = new StringSink();
        step(table, "create", "CREATE TABLE " + table + " (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)", mode, steps);
        if (steps.length() > 0) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                    + ": a persisted type that is no array is a dedup key, but CREATE refused it: " + oneLine(steps));
        }
        try {
            final ObjList<TypeConformanceValues.Row> keyRows = rows;
            final StringSink writeErrors = new StringSink();
            final int n = keyRows.size();
            TypeConformanceValues.writeRows(engine, sqlExecutionContext, table, keyRows, "", 0, 0, n, 1, true, writeErrors);
            if (writeErrors.length() > 0) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + oneLine(writeErrors));
            }
            drainWalQueue();
            assertApplied(table, "the first write", path, mode);
            final Map<String, String> first = texts("SELECT k, v FROM " + table);
            TypeConformanceValues.writeRows(engine, sqlExecutionContext, table, keyRows, "dup:", 0, 0, n, 1, true, writeErrors);
            if (writeErrors.length() > 0) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + oneLine(writeErrors));
            }
            drainWalQueue();
            assertApplied(table, "the same rows again", path, mode);
            // row i's timestamp with row i + 1's value
            final ObjList<TypeConformanceValues.Row> shifted = new ObjList<>();
            for (int i = 0; i < n; i++) {
                shifted.add(TypeConformanceValues.Row.relabel(keyRows.getQuick((i + 1) % n), keyRows.getQuick(i).label));
            }
            TypeConformanceValues.writeRows(engine, sqlExecutionContext, table, shifted, "shift:", 0, 0, n, 1, true, writeErrors);
            if (writeErrors.length() > 0) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + oneLine(writeErrors));
            }
            drainWalQueue();
            assertApplied(table, "the rows with the next row's value", path, mode);
            final ObjList<String> expected = new ObjList<>();
            final Map<String, long[]> expectedValues = new HashMap<>();
            for (int i = 0; i < n; i++) {
                final TypeConformanceValues.Row row = keyRows.getQuick(i);
                final TypeConformanceValues.Row next = keyRows.getQuick((i + 1) % n);
                final boolean isWritten = first.containsKey(row.label);
                final boolean isNextWritten = first.containsKey(next.label);
                if (isWritten && (!isNextWritten || !isOneKey(row, next))) {
                    expected.add("dup:" + row.label);
                    expectedValues.put("dup:" + row.label, row.bits);
                }
                if (isNextWritten) {
                    expected.add("shift:" + row.label);
                    expectedValues.put("shift:" + row.label, next.bits);
                }
            }
            expected.sort(String::compareTo);
            final Map<String, long[]> actual = new HashMap<>();
            final Map<String, String> ignoredTexts = new HashMap<>();
            readLater(table, ignoredTexts, actual);
            final ObjList<String> actualKeys = new ObjList<>();
            for (String key : actual.keySet()) {
                actualKeys.add(key);
            }
            actualKeys.sort(String::compareTo);
            if (!expected.equals(actualKeys)) {
                throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": rows after the upserts are "
                        + actualKeys + ", the keys give " + expected);
            }
            for (int i = 0, m = expected.size(); i < m; i++) {
                final String key = expected.getQuick(i);
                final long[] bits = expectedValues.get(key);
                if (bits != null) {
                    TypeConformanceInvariants.assertReadsBackAsWritten(type, key, path, mode, bits, actual.get(key));
                }
            }
        } finally {
            execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * {@code sql.latest_by_key} for a type registered later: LATEST ON ... PARTITION BY the
     * column keeps one row per distinct value, the b: copy written last, reading back as written.
     * Values that are one key (two NULL forms, as dedup tells them) are one partition.
     */
    private void checkLaterLatestByKey(String table, String mode, StringSink steps) throws Exception {
        final String path = "sql.latest_by_key";
        TypeConformanceInvariants.nullRowWriteError(type, path, mode, steps);
        final Map<String, long[]> bits = new HashMap<>();
        try (
                RecordCursorFactory factory = select("SELECT k, v FROM (" + table + " LATEST ON ts PARTITION BY v)");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bits.put(record.getVarcharA(0).toString(), TypeConformanceValues.readValue(record, 1, type));
            }
        } catch (Throwable e) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + e.getMessage(), e);
        }
        // the rows the table holds, and of each group of one key, the last of them
        int expected = 0;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            boolean isLastOfKey = true;
            for (int j = i + 1; j < n && isLastOfKey; j++) {
                isLastOfKey = !isOneKey(row, rows.getQuick(j));
            }
            if (!isLastOfKey) {
                continue;
            }
            expected++;
            final long[] read = bits.get("b:" + row.label);
            if (read == null) {
                throw new AssertionError(TypeConformanceInvariants.context(type, row.label, path, mode)
                        + ": the latest row of this key, b:" + row.label + ", is missing: " + bits.keySet());
            }
            if (!row.isNull()) {
                TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, read);
            }
        }
        if (bits.size() != expected) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode)
                    + ": " + bits.size() + " rows, one per key expected (" + expected + "): " + bits.keySet());
        }
    }

    /**
     * {@code storage.parquet_convert} for a type registered later: every step succeeds and every
     * value row, written as it prints into the VARCHAR column, reads back as written once the
     * partition is native again.
     */
    private void checkLaterParquetConvert(String table, String mode, StringSink steps, Map<String, long[]> sourceBits) throws Exception {
        final String path = "storage.parquet_convert";
        if (steps.toString().contains("error: ")) {
            throw new AssertionError(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + steps);
        }
        final Map<String, String> texts = new HashMap<>();
        final Map<String, long[]> bits = new HashMap<>();
        readLater(table, texts, bits);
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull() || !sourceBits.containsKey(row.label)) {
                continue;
            }
            TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, sourceBits.get(row.label), bits.get(row.label));
        }
    }

    private void checkLaterRows(String table, String prefix, String path, String mode, StringSink steps) throws Exception {
        final String nullError = TypeConformanceInvariants.nullRowWriteError(type, path, mode, steps);
        final Map<String, String> texts = new HashMap<>();
        final Map<String, long[]> bits = new HashMap<>();
        readLater(table, texts, bits);
        TypeConformanceValues.Row sentinel = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            final String key = prefix + row.label;
            if (row.isNull()) {
                continue;
            }
            if (!texts.containsKey(key)) {
                continue;
            }
            TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, bits.get(key));
            if ("sentinel".equals(row.label)) {
                sentinel = row;
            }
        }
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (label.startsWith("sentinel_") && texts.containsKey(prefix + label)) {
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, mode, texts.get(prefix + "null"), texts.get(prefix + label));
            }
        }
        if (sentinel != null) {
            final String nullKey = prefix + "null";
            TypeConformanceInvariants.assertNullPolicy(
                    type,
                    path,
                    mode,
                    texts.get(nullKey),
                    bits.get(nullKey),
                    nullError,
                    texts.get(prefix + "sentinel"),
                    bits.get(prefix + "sentinel"),
                    sentinel.bits
            );
        }
    }

    private void checkLaterTops(String table, String mode, StringSink steps) throws Exception {
        checkLaterRows(table, "d2:", "storage.tops", mode, steps);
        // a column-top row reads as the NULL row the type writes explicitly
        final Map<String, String> texts = new HashMap<>();
        final Map<String, long[]> bits = new HashMap<>();
        readLater(table, texts, bits);
        final String nullText = texts.get("d2:null");
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            final String key = "d0:" + label;
            if (texts.containsKey(key) && nullText != null && !nullText.equals(texts.get(key))) {
                throw new AssertionError(TypeConformanceInvariants.context(type, key, "storage.tops", mode)
                        + ": a column-top row reads " + texts.get(key) + ", the NULL row " + nullText);
            }
        }
    }

    /**
     * Creates a table; when that fails (a type that cannot be stored), asserts every section of
     * the path as the recorded error and returns false, so the path stops there.
     */
    private boolean createTable(String table, String mode, String columns, StringSink steps, String... sections) {
        final String partitionBy = mode.endsWith("none") ? "NONE" : "DAY";
        final String wal = isWal(mode) ? "WAL" : "BYPASS WAL";
        final int before = steps.length();
        stepNoDrain("create", "CREATE TABLE " + table + " (" + columns + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY " + partitionBy + " " + wal, steps);
        return steps.length() == before || failed(mode, steps, sections);
    }

    /**
     * Asserts every section of a path as the steps' errors, for a path that cannot go on.
     * Returns false.
     */
    private boolean failed(String mode, StringSink steps, String... sections) {
        for (String section : sections) {
            assertSection(section, mode, steps);
        }
        return false;
    }

    /**
     * Prints every row of the query's page frames: the value row's label (column k, read through
     * the frame's record) and the stored bytes of column v, or {@code top} for a column top.
     * A var-size column prints its aux entry and its data slice.
     */
    private String frames(String sql) {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select(sql)) {
            if (!factory.supportsPageFrameCursor()) {
                return "no page frames\n";
            }
            final RecordMetadata metadata = factory.getMetadata();
            final int kIndex = metadata.getColumnIndex("k");
            final int vIndex = metadata.getColumnIndex("v");
            final TypeDriver driver = ColumnType.getTypeDriver(metadata.getColumnType(vIndex));
            try (
                    PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool memoryPool = new PageFrameMemoryPool(configuration)
            ) {
                addressCache.of(metadata, frameCursor.getColumnMapping(), frameCursor.isExternal());
                memoryPool.of(addressCache);
                final PageFrameMemoryRecord record = new PageFrameMemoryRecord();
                record.of(frameCursor);
                PageFrame frame;
                int frameIndex = 0;
                while ((frame = frameCursor.next()) != null) {
                    addressCache.add(frameIndex, frame);
                    final PageFrameMemory memory = memoryPool.navigateTo(frameIndex);
                    record.init(memory);
                    final long rowCount = frame.getPartitionHi() - frame.getPartitionLo();
                    sink.put("frame ").put(frameIndex).put(" rows=").put(rowCount)
                            .put(memory.getFrameFormat() == PartitionFormat.PARQUET ? " parquet" : " native").put('\n');
                    final long dataAddress = memory.getPageAddress(vIndex);
                    final long auxAddress = memory.getAuxPageAddress(vIndex);
                    for (long r = 0; r < rowCount; r++) {
                        record.setRowIndex(r);
                        sink.put(record.getVarcharA(kIndex)).put('\t');
                        if (dataAddress == 0 && auxAddress == 0) {
                            sink.put("top");
                        } else if (driver instanceof ColumnTypeDriver && memory.getFrameFormat() == PartitionFormat.PARQUET) {
                            // decoded var-size vectors hold addresses and padding: print the value
                            sink.put("value=");
                            CursorPrinter.printColumn(record, metadata, vIndex, sink);
                        } else if (driver instanceof ColumnTypeDriver varDriver) {
                            final long auxLo = varDriver.getAuxVectorOffset(r);
                            final long auxHi = varDriver.getAuxVectorOffset(r + 1);
                            sink.put("aux=");
                            appendHex(sink, auxAddress + auxLo, auxHi - auxLo);
                            // the row's data slice ends where the data vector up to this row ends
                            final long dataLo = varDriver.getDataVectorOffset(auxAddress, r);
                            final long dataHi = varDriver.getDataVectorSizeAt(auxAddress, r);
                            sink.put(" data=");
                            if (dataHi > memory.getPageSize(vIndex) || dataLo > dataHi) {
                                sink.put("out of page: ").put(dataLo).put("..").put(dataHi);
                            } else {
                                appendHex(sink, dataAddress + dataLo, dataHi - dataLo);
                            }
                        } else {
                            final int width = driver.getMovement().size();
                            appendHex(sink, dataAddress + r * width, width);
                        }
                        sink.put('\n');
                    }
                    frameIndex++;
                }
            }
        } catch (Throwable e) {
            return "error: " + e.getMessage() + '\n';
        }
        return TypeConformanceRecording.escape(sink);
    }

    private void insertRows(String table, String mode, String prefix, long base, boolean withValue, StringSink steps) {
        insertRowsRange(table, prefix, base, 0, rows.size(), withValue, steps);
        if (isWal(mode)) {
            drainWalQueue();
        }
    }

    /**
     * Inserts rows {@code lo..hi} in one INSERT, at timestamps {@code base + i} seconds; with
     * {@code withValue} false only k and ts are written (the table has no v yet).
     */
    private void insertRowsRange(String table, String prefix, long base, int lo, int hi, boolean withValue, StringSink steps) {
        insertRowsWithStep(table, prefix, base, lo, hi, 1, withValue, steps);
    }

    private void insertRowsStep(String table, String mode, String prefix, long base, int first, int step, StringSink steps) {
        insertRowsWithStep(table, prefix, base, first, rows.size(), step, true, steps);
        if (isWal(mode)) {
            drainWalQueue();
        }
    }

    private void insertRowsWithStep(String table, String prefix, long base, int lo, int hi, int step, boolean withValue, StringSink steps) {
        TypeConformanceValues.writeRows(engine, sqlExecutionContext, table, rows, prefix, base, lo, hi, step, withValue, steps);
    }

    // whether two value rows are one dedup key (checkLaterDedup)
    private boolean isOneKey(TypeConformanceValues.Row a, TypeConformanceValues.Row b) {
        if (a.isNull() && b.isNull()) {
            return true;
        }
        if (a.isNull() || b.isNull()) {
            final TypeConformanceValues.Row value = a.isNull() ? b : a;
            return switch (TypeConformanceInvariants.policyOf(type)) {
                case TypeConformanceInvariants.POLICY_SENTINEL -> "sentinel".equals(value.label);
                case TypeConformanceInvariants.POLICY_NONE -> Arrays.equals(new long[4], value.bits);
                default -> false;
            };
        }
        return Arrays.equals(a.bits, b.bits);
    }

    /**
     * Prints a query through a record cursor, escaped; an error prints as {@code error: ...}. A
     * query that runs is also asserted with the full {@code assertQuery(sql).returns(...)} battery,
     * which reads the cursor twice and checks its size, under the random access, size and
     * designated timestamp its factory declares, as the SQL kit's {@code assertReturns} does.
     */
    private String query(String sql) throws Exception {
        final StringSink sink = new StringSink();
        final boolean isRandomAccess;
        final boolean isSizeKnown;
        try {
            printSql(sql, sink);
            try (
                    RecordCursorFactory factory = select(sql);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                isRandomAccess = factory.recordCursorSupportsRandomAccess();
                while (cursor.hasNext()) {
                    // the size is known, if at all, once the cursor has been read
                }
                cursor.toTop();
                isSizeKnown = cursor.size() != -1;
            }
        } catch (Throwable e) {
            return "error: " + e.getMessage() + '\n';
        }
        assertQuery(sql)
                .noLeakCheck()
                .supportsRandomAccess(isRandomAccess)
                .expectSize(isSizeKnown)
                .inferTimestamp()
                .returns(sink);
        return TypeConformanceRecording.escape(sink);
    }

    private void readLater(String table, Map<String, String> texts, Map<String, long[]> bits) throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT k, v FROM " + table);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bits.put(record.getVarcharA(0).toString(), TypeConformanceValues.readValue(record, 1, type));
            }
        }
        final StringSink sink = new StringSink();
        printSql("SELECT k, v FROM " + table, sink);
        final String[] lines = sink.toString().split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int tab = lines[i].indexOf('\t');
            texts.put(lines[i].substring(0, tab), lines[i].substring(tab + 1));
        }
    }

    /**
     * Runs one DDL or DML step; for a WAL table in {@code mode} it also applies the WAL. An
     * error is appended to {@code steps} as a line of the recording.
     */
    // a later type's step also records a WAL apply that suspended the table, so a refusal raised
    // during apply reads as the step's error; existing types' recordings show the apply by the rows
    private void step(String table, String name, String sql, String mode, StringSink steps) throws Exception {
        stepNoDrain(name, sql, steps);
        if (isWal(mode)) {
            drainWalQueue();
            if (type.isLater()) {
                final Map<String, String> status = texts("SELECT coalesce(errorMessage, '') k, 'x' v FROM wal_tables() WHERE name = '" + table + "' AND suspended");
                for (String error : status.keySet()) {
                    steps.put("error: ").put(name).put(": the WAL apply suspended the table: ").put(error).put('\n');
                }
            }
        }
    }

    private void stepNoDrain(String name, String sql, StringSink steps) {
        try {
            execute(sql);
        } catch (Throwable e) {
            steps.put("error: ").put(name).put(": ").put(e.getMessage()).put('\n');
        }
    }

    // label -> column 1 printed
    private Map<String, String> texts(String sql) throws Exception {
        final Map<String, String> texts = new HashMap<>();
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        final String[] lines = sink.toString().split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int tab = lines[i].indexOf('\t');
            texts.put(lines[i].substring(0, tab), lines[i].substring(tab + 1));
        }
        return texts;
    }

    private String valuesLine(String sql) throws Exception {
        final String printed = query(sql);
        if (printed.startsWith("error: ")) {
            return oneLine(printed);
        }
        final String[] lines = printed.split("\n");
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i < lines.length; i++) {
            if (i > 1) {
                sb.append('|');
            }
            final int tab = lines[i].indexOf('\t');
            sb.append(lines[i], tab + 1, lines[i].length());
        }
        return sb.toString();
    }

    /**
     * Writes value rows of a type registered later: the NULL row as a literal, the others as
     * raw bits by width ({@link TypeConformanceValues#writeRows}).
     */
    private void writeLaterRows(String table, String prefix, long base, int lo, int hi, int step, StringSink steps) {
        TypeConformanceValues.writeRows(engine, sqlExecutionContext, table, rows, prefix, base, lo, hi, step, true, steps);
    }

    // recordings: start
    static {
        rec("BOOLEAN", """
                ## tops
                k\tv
                d0:min\tfalse
                d0:max\tfalse
                d0:null\tfalse
                d1:min\tfalse
                d1:max\ttrue
                d1:null\tfalse
                d2:min\tfalse
                d2:max\ttrue
                d2:null\tfalse
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t01
                d1:null\t00
                frame 3 rows=3 native
                d2:min\t00
                d2:max\t01
                d2:null\t00
                ## tops-o3
                k\tv
                d0:min\tfalse
                o3d0:min\tfalse
                d0:max\tfalse
                o3d0:max\ttrue
                d0:null\tfalse
                o3d0:null\tfalse
                d1:min\tfalse
                o3d1:min\tfalse
                d1:max\ttrue
                o3d1:max\ttrue
                d1:null\tfalse
                o3d1:null\tfalse
                d2:min\tfalse
                d2:max\ttrue
                d2:null\tfalse
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t00
                o3d0:min\t00
                d0:max\t00
                o3d0:max\t01
                d0:null\t00
                o3d0:null\t00
                frame 1 rows=6 native
                d1:min\t00
                o3d1:min\t00
                d1:max\t01
                o3d1:max\t01
                d1:null\t00
                o3d1:null\t00
                frame 2 rows=3 native
                d2:min\t00
                d2:max\t01
                d2:null\t00
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\tfalse
                ## single_row-frames
                frame 0 rows=1 native
                min\t00
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## frames
                frame 0 rows=3 native
                min\t00
                max\t01
                null\t00
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\tfalse
                null\tfalse
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00
                null\t00
                ## frames-o3
                frame 0 rows=3 native
                min\t00
                max\t01
                null\t00
                ## parquet
                k\tv
                d0:min\tfalse
                d0:max\ttrue
                d0:null\tfalse
                d1:min\tfalse
                d1:max\ttrue
                d1:null\tfalse
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t00
                d0:max\t01
                d0:null\t00
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t01
                d1:null\t00
                ## parquet-native
                k\tv
                d0:min\tfalse
                d0:max\ttrue
                d0:null\tfalse
                d1:min\tfalse
                d1:max\ttrue
                d1:null\tfalse
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t00
                d0:max\t01
                d0:null\t00
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t01
                d1:null\t00
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] column 'v' type is already 'BOOLEAN'
                BYTE\t0|0|0|0|1|0
                SHORT\t0|0|0|0|1|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|0|1|0
                LONG\tnull|null|null|0|1|0
                DATE\t|||1970-01-01T00:00:00.000Z|1970-01-01T00:00:00.001Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t|||1970-01-01T00:00:00.000000Z|1970-01-01T00:00:00.000001Z|1970-01-01T00:00:00.000000Z
                FLOAT\tnull|null|null|0.0|1.0|0.0
                DOUBLE\tnull|null|null|0.0|1.0|0.0
                STRING\t|||false|true|false
                SYMBOL\t|||false|true|false
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t|||false|true|false
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=BOOLEAN, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t|||1970-01-01T00:00:00.000000000Z|1970-01-01T00:00:00.000000001Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=BOOLEAN, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## dedup
                k\tv
                dup:min\tfalse
                shift:min\ttrue
                dup:max\ttrue
                shift:max\tfalse
                shift:null\tfalse
                ## dedup-frames
                frame 0 rows=5 native
                dup:min\t00
                shift:min\t01
                dup:max\t01
                shift:max\t00
                shift:null\t00
                ## latest_by_key
                k\tv
                b:max\ttrue
                b:null\tfalse
                ## parquet_convert
                parquet
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                native
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                """);
        rec("BYTE", """
                ## dedup
                k\tv
                dup:min\t-128
                shift:min\t127
                dup:max\t127
                shift:max\t-1
                dup:other_null\t-1
                shift:other_null\t0
                dup:null\t0
                shift:null\t-128
                ## dedup-frames
                frame 0 rows=8 native
                dup:min\t80
                shift:min\t7f
                dup:max\t7f
                shift:max\tff
                dup:other_null\tff
                shift:other_null\t00
                dup:null\t00
                shift:null\t80
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-128
                ## single_row-frames
                frame 0 rows=1 native
                min\t80
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t0
                d0:max\t0
                d0:other_null\t0
                d0:null\t0
                d1:min\t0
                d1:max\t0
                d1:other_null\t-1
                d1:null\t0
                d2:min\t-128
                d2:max\t127
                d2:other_null\t-1
                d2:null\t0
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:other_null\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:other_null\tff
                d1:null\t00
                frame 3 rows=4 native
                d2:min\t80
                d2:max\t7f
                d2:other_null\tff
                d2:null\t00
                ## tops-o3
                k\tv
                d0:min\t0
                o3d0:min\t-128
                d0:max\t0
                o3d0:max\t127
                d0:other_null\t0
                o3d0:other_null\t-1
                d0:null\t0
                o3d0:null\t0
                d1:min\t0
                o3d1:min\t-128
                d1:max\t0
                o3d1:max\t127
                d1:other_null\t-1
                o3d1:other_null\t-1
                d1:null\t0
                o3d1:null\t0
                d2:min\t-128
                d2:max\t127
                d2:other_null\t-1
                d2:null\t0
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t00
                o3d0:min\t80
                d0:max\t00
                o3d0:max\t7f
                d0:other_null\t00
                o3d0:other_null\tff
                d0:null\t00
                o3d0:null\t00
                frame 1 rows=8 native
                d1:min\t00
                o3d1:min\t80
                d1:max\t00
                o3d1:max\t7f
                d1:other_null\tff
                o3d1:other_null\tff
                d1:null\t00
                o3d1:null\t00
                frame 2 rows=4 native
                d2:min\t80
                d2:max\t7f
                d2:other_null\tff
                d2:null\t00
                ## parquet
                k\tv
                d0:min\t-128
                d0:max\t127
                d0:other_null\t-1
                d0:null\t0
                d1:min\t-128
                d1:max\t127
                d1:other_null\t-1
                d1:null\t0
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t80
                d0:max\t7f
                d0:other_null\tff
                d0:null\t00
                frame 1 rows=4 native
                d1:min\t80
                d1:max\t7f
                d1:other_null\tff
                d1:null\t00
                ## parquet-native
                k\tv
                d0:min\t-128
                d0:max\t127
                d0:other_null\t-1
                d0:null\t0
                d1:min\t-128
                d1:max\t127
                d1:other_null\t-1
                d1:null\t0
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t80
                d0:max\t7f
                d0:other_null\tff
                d0:null\t00
                frame 1 rows=4 native
                d1:min\t80
                d1:max\t7f
                d1:other_null\tff
                d1:null\t00
                ## insert
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## frames
                frame 0 rows=4 native
                min\t80
                max\t7f
                other_null\tff
                null\t00
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-128
                other_null\t-1
                ## frames-o3-none
                frame 0 rows=2 native
                min\t80
                other_null\tff
                ## frames-o3
                frame 0 rows=4 native
                min\t80
                max\t7f
                other_null\tff
                null\t00
                ## alter
                target\td0:min|d0:max|d0:other_null|d0:null|d1:min|d1:max|d1:other_null|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|true|false
                BYTE\terror: alter: [41] column 'v' type is already 'BYTE'
                SHORT\t0|0|0|0|-128|127|-1|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|-128|127|-1|0
                LONG\tnull|null|null|null|-128|127|-1|0
                DATE\t||||1969-12-31T23:59:59.872Z|1970-01-01T00:00:00.127Z|1969-12-31T23:59:59.999Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t||||1969-12-31T23:59:59.999872Z|1970-01-01T00:00:00.000127Z|1969-12-31T23:59:59.999999Z|1970-01-01T00:00:00.000000Z
                FLOAT\tnull|null|null|null|-128.0|127.0|-1.0|0.0
                DOUBLE\tnull|null|null|null|-128.0|127.0|-1.0|0.0
                STRING\t||||-128|127|-1|0
                SYMBOL\t||||-128|127|-1|0
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-128|127|-1|0
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=BYTE, new=DOUBLE[]]
                DECIMAL8\t||||||-1.0|0.0
                DECIMAL16\t||||||-1.00|0.00
                DECIMAL32\t||||-128|127|-1|0
                DECIMAL64\t||||-128.0000|127.0000|-1.0000|0.0000
                DECIMAL128\t||||-128.0000000000|127.0000000000|-1.0000000000|0.0000000000
                DECIMAL256\t||||-128.00000000000000000000|127.00000000000000000000|-1.00000000000000000000|0.00000000000000000000
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1969-12-31T23:59:59.999999872Z|1970-01-01T00:00:00.000000127Z|1969-12-31T23:59:59.999999999Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t||||-128.00|127.00|-1.00|0.00
                DECIMAL(18,3)\t||||-128.000|127.000|-1.000|0.000
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=BYTE, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\t127
                b:min\t-128
                b:null\t0
                b:other_null\t-1
                ## parquet_convert
                parquet
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                native
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                """);
        rec("SHORT", """
                ## parquet
                k\tv
                d0:min\t-32768
                d0:max\t32767
                d0:other_null\t-1
                d0:null\t0
                d1:min\t-32768
                d1:max\t32767
                d1:other_null\t-1
                d1:null\t0
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0080
                d0:max\tff7f
                d0:other_null\tffff
                d0:null\t0000
                frame 1 rows=4 native
                d1:min\t0080
                d1:max\tff7f
                d1:other_null\tffff
                d1:null\t0000
                ## parquet-native
                k\tv
                d0:min\t-32768
                d0:max\t32767
                d0:other_null\t-1
                d0:null\t0
                d1:min\t-32768
                d1:max\t32767
                d1:other_null\t-1
                d1:null\t0
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0080
                d0:max\tff7f
                d0:other_null\tffff
                d0:null\t0000
                frame 1 rows=4 native
                d1:min\t0080
                d1:max\tff7f
                d1:other_null\tffff
                d1:null\t0000
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-32768
                ## single_row-frames
                frame 0 rows=1 native
                min\t0080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## frames
                frame 0 rows=4 native
                min\t0080
                max\tff7f
                other_null\tffff
                null\t0000
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-32768
                other_null\t-1
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0080
                other_null\tffff
                ## frames-o3
                frame 0 rows=4 native
                min\t0080
                max\tff7f
                other_null\tffff
                null\t0000
                ## dedup
                k\tv
                dup:min\t-32768
                shift:min\t32767
                dup:max\t32767
                shift:max\t-1
                dup:other_null\t-1
                shift:other_null\t0
                dup:null\t0
                shift:null\t-32768
                ## dedup-frames
                frame 0 rows=8 native
                dup:min\t0080
                shift:min\tff7f
                dup:max\tff7f
                shift:max\tffff
                dup:other_null\tffff
                shift:other_null\t0000
                dup:null\t0000
                shift:null\t0080
                ## tops
                k\tv
                d0:min\t0
                d0:max\t0
                d0:other_null\t0
                d0:null\t0
                d1:min\t0
                d1:max\t0
                d1:other_null\t-1
                d1:null\t0
                d2:min\t-32768
                d2:max\t32767
                d2:other_null\t-1
                d2:null\t0
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:other_null\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:other_null\tffff
                d1:null\t0000
                frame 3 rows=4 native
                d2:min\t0080
                d2:max\tff7f
                d2:other_null\tffff
                d2:null\t0000
                ## tops-o3
                k\tv
                d0:min\t0
                o3d0:min\t-32768
                d0:max\t0
                o3d0:max\t32767
                d0:other_null\t0
                o3d0:other_null\t-1
                d0:null\t0
                o3d0:null\t0
                d1:min\t0
                o3d1:min\t-32768
                d1:max\t0
                o3d1:max\t32767
                d1:other_null\t-1
                o3d1:other_null\t-1
                d1:null\t0
                o3d1:null\t0
                d2:min\t-32768
                d2:max\t32767
                d2:other_null\t-1
                d2:null\t0
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000
                o3d0:min\t0080
                d0:max\t0000
                o3d0:max\tff7f
                d0:other_null\t0000
                o3d0:other_null\tffff
                d0:null\t0000
                o3d0:null\t0000
                frame 1 rows=8 native
                d1:min\t0000
                o3d1:min\t0080
                d1:max\t0000
                o3d1:max\tff7f
                d1:other_null\tffff
                o3d1:other_null\tffff
                d1:null\t0000
                o3d1:null\t0000
                frame 2 rows=4 native
                d2:min\t0080
                d2:max\tff7f
                d2:other_null\tffff
                d2:null\t0000
                ## alter
                target\td0:min|d0:max|d0:other_null|d0:null|d1:min|d1:max|d1:other_null|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|true|false
                BYTE\t0|0|0|0|0|-1|-1|0
                SHORT\terror: alter: [41] column 'v' type is already 'SHORT'
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|-32768|32767|-1|0
                LONG\tnull|null|null|null|-32768|32767|-1|0
                DATE\t||||1969-12-31T23:59:27.232Z|1970-01-01T00:00:32.767Z|1969-12-31T23:59:59.999Z|1970-01-01T00:00:00.000Z
                TIMESTAMP\t||||1969-12-31T23:59:59.967232Z|1970-01-01T00:00:00.032767Z|1969-12-31T23:59:59.999999Z|1970-01-01T00:00:00.000000Z
                FLOAT\tnull|null|null|null|-32768.0|32767.0|-1.0|0.0
                DOUBLE\tnull|null|null|null|-32768.0|32767.0|-1.0|0.0
                STRING\t||||-32768|32767|-1|0
                SYMBOL\t||||-32768|32767|-1|0
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-32768|32767|-1|0
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=SHORT, new=DOUBLE[]]
                DECIMAL8\t||||||-1.0|0.0
                DECIMAL16\t||||||-1.00|0.00
                DECIMAL32\t||||-32768|32767|-1|0
                DECIMAL64\t||||-32768.0000|32767.0000|-1.0000|0.0000
                DECIMAL128\t||||-32768.0000000000|32767.0000000000|-1.0000000000|0.0000000000
                DECIMAL256\t||||-32768.00000000000000000000|32767.00000000000000000000|-1.00000000000000000000|0.00000000000000000000
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1969-12-31T23:59:59.999967232Z|1970-01-01T00:00:00.000032767Z|1969-12-31T23:59:59.999999999Z|1970-01-01T00:00:00.000000000Z
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t||||||-1.00|0.00
                DECIMAL(18,3)\t||||-32768.000|32767.000|-1.000|0.000
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=SHORT, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\t32767
                b:min\t-32768
                b:null\t0
                b:other_null\t-1
                ## parquet_convert
                parquet
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                native
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                """);
        rec("CHAR", """
                ## alter
                target\td0:min|d0:max|d0:other_null|d0:null|d1:min|d1:max|d1:other_null|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\terror: alter: [41] column 'v' type is already 'CHAR'
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\t|||||\\uffff|\\uffff|
                SYMBOL\t|||||\\uffff|\\uffff|
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t|||||\\uffff|\\uffff|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=CHAR, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=CHAR, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:other_null\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:other_null\t\\uffff
                d1:null\t
                d2:min\t
                d2:max\t\\uffff
                d2:other_null\t\\uffff
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:other_null\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:other_null\tffff
                d1:null\t0000
                frame 3 rows=4 native
                d2:min\t0000
                d2:max\tffff
                d2:other_null\tffff
                d2:null\t0000
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t
                d0:max\t
                o3d0:max\t\\uffff
                d0:other_null\t
                o3d0:other_null\t\\uffff
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t
                d1:max\t
                o3d1:max\t\\uffff
                d1:other_null\t\\uffff
                o3d1:other_null\t\\uffff
                d1:null\t
                o3d1:null\t
                d2:min\t
                d2:max\t\\uffff
                d2:other_null\t\\uffff
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000
                o3d0:min\t0000
                d0:max\t0000
                o3d0:max\tffff
                d0:other_null\t0000
                o3d0:other_null\tffff
                d0:null\t0000
                o3d0:null\t0000
                frame 1 rows=8 native
                d1:min\t0000
                o3d1:min\t0000
                d1:max\t0000
                o3d1:max\tffff
                d1:other_null\tffff
                o3d1:other_null\tffff
                d1:null\t0000
                o3d1:null\t0000
                frame 2 rows=4 native
                d2:min\t0000
                d2:max\tffff
                d2:other_null\tffff
                d2:null\t0000
                ## dedup
                k\tv
                dup:min\t
                shift:min\t\\uffff
                shift:max\t\\uffff
                dup:other_null\t\\uffff
                shift:other_null\t
                shift:null\t
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t0000
                shift:min\tffff
                shift:max\tffff
                dup:other_null\tffff
                shift:other_null\t0000
                shift:null\t0000
                ## insert
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## frames
                frame 0 rows=4 native
                min\t0000
                max\tffff
                other_null\tffff
                null\t0000
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t
                other_null\t\\uffff
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000
                other_null\tffff
                ## frames-o3
                frame 0 rows=4 native
                min\t0000
                max\tffff
                other_null\tffff
                null\t0000
                ## parquet
                k\tv
                d0:min\t
                d0:max\t\\uffff
                d0:other_null\t\\uffff
                d0:null\t
                d1:min\t
                d1:max\t\\uffff
                d1:other_null\t\\uffff
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0000
                d0:max\tffff
                d0:other_null\tffff
                d0:null\t0000
                frame 1 rows=4 native
                d1:min\t0000
                d1:max\tffff
                d1:other_null\tffff
                d1:null\t0000
                ## parquet-native
                k\tv
                d0:min\t
                d0:max\t\\uffff
                d0:other_null\t\\uffff
                d0:null\t
                d1:min\t
                d1:max\t\\uffff
                d1:other_null\t\\uffff
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0000
                d0:max\tffff
                d0:other_null\tffff
                d0:null\t0000
                frame 1 rows=4 native
                d1:min\t0000
                d1:max\tffff
                d1:other_null\tffff
                d1:null\t0000
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                k\tv
                b:null\t
                b:other_null\t\\uffff
                ## parquet_convert
                parquet
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                native
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                """);
        rec("INT", """
                ## dedup
                k\tv
                dup:min\t-2147483647
                shift:min\t2147483647
                dup:max\t2147483647
                shift:max\tnull
                shift:sentinel\tnull
                dup:null\tnull
                shift:null\t-2147483647
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t01000080
                shift:min\tffffff7f
                dup:max\tffffff7f
                shift:max\t00000080
                shift:sentinel\t00000080
                dup:null\t00000080
                shift:null\t01000080
                ## insert
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## frames
                frame 0 rows=4 native
                min\t01000080
                max\tffffff7f
                sentinel\t00000080
                null\t00000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-2147483647
                sentinel\tnull
                ## frames-o3-none
                frame 0 rows=2 native
                min\t01000080
                sentinel\t00000080
                ## frames-o3
                frame 0 rows=4 native
                min\t01000080
                max\tffffff7f
                sentinel\t00000080
                null\t00000080
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\tnull
                d1:max\tnull
                d1:sentinel\tnull
                d1:null\tnull
                d2:min\t-2147483647
                d2:max\t2147483647
                d2:sentinel\tnull
                d2:null\tnull
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t00000080
                d1:null\t00000080
                frame 3 rows=4 native
                d2:min\t01000080
                d2:max\tffffff7f
                d2:sentinel\t00000080
                d2:null\t00000080
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t-2147483647
                d0:max\tnull
                o3d0:max\t2147483647
                d0:sentinel\tnull
                o3d0:sentinel\tnull
                d0:null\tnull
                o3d0:null\tnull
                d1:min\tnull
                o3d1:min\t-2147483647
                d1:max\tnull
                o3d1:max\t2147483647
                d1:sentinel\tnull
                o3d1:sentinel\tnull
                d1:null\tnull
                o3d1:null\tnull
                d2:min\t-2147483647
                d2:max\t2147483647
                d2:sentinel\tnull
                d2:null\tnull
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t00000080
                o3d0:min\t01000080
                d0:max\t00000080
                o3d0:max\tffffff7f
                d0:sentinel\t00000080
                o3d0:sentinel\t00000080
                d0:null\t00000080
                o3d0:null\t00000080
                frame 1 rows=8 native
                d1:min\t00000080
                o3d1:min\t01000080
                d1:max\t00000080
                o3d1:max\tffffff7f
                d1:sentinel\t00000080
                o3d1:sentinel\t00000080
                d1:null\t00000080
                o3d1:null\t00000080
                frame 2 rows=4 native
                d2:min\t01000080
                d2:max\tffffff7f
                d2:sentinel\t00000080
                d2:null\t00000080
                ## parquet
                k\tv
                d0:min\t-2147483647
                d0:max\t2147483647
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\t-2147483647
                d1:max\t2147483647
                d1:sentinel\tnull
                d1:null\tnull
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t01000080
                d0:max\tffffff7f
                d0:sentinel\t00000080
                d0:null\t00000080
                frame 1 rows=4 native
                d1:min\t01000080
                d1:max\tffffff7f
                d1:sentinel\t00000080
                d1:null\t00000080
                ## parquet-native
                k\tv
                d0:min\t-2147483647
                d0:max\t2147483647
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\t-2147483647
                d1:max\t2147483647
                d1:sentinel\tnull
                d1:null\tnull
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t01000080
                d0:max\tffffff7f
                d0:sentinel\t00000080
                d0:null\t00000080
                frame 1 rows=4 native
                d1:min\t01000080
                d1:max\tffffff7f
                d1:sentinel\t00000080
                d1:null\t00000080
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|false|false
                BYTE\t0|0|0|0|1|-1|0|0
                SHORT\t0|0|0|0|1|-1|0|0
                CHAR\tincompatible [41]
                INT\terror: alter: [41] column 'v' type is already 'INT'
                LONG\tnull|null|null|null|-2147483647|2147483647|null|null
                DATE\t||||1969-12-07T03:28:36.353Z|1970-01-25T20:31:23.647Z||
                TIMESTAMP\t||||1969-12-31T23:24:12.516353Z|1970-01-01T00:35:47.483647Z||
                FLOAT\tnull|null|null|null|-2.1474836E9|2.1474836E9|null|null
                DOUBLE\tnull|null|null|null|-2.147483647E9|2.147483647E9|null|null
                STRING\t||||-2147483647|2147483647||
                SYMBOL\t||||-2147483647|2147483647||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-2147483647|2147483647||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=INT, new=DOUBLE[]]
                DECIMAL8\t|||||||
                DECIMAL16\t|||||||
                DECIMAL32\t|||||||
                DECIMAL64\t||||-2147483647.0000|2147483647.0000||
                DECIMAL128\t||||-2147483647.0000000000|2147483647.0000000000||
                DECIMAL256\t||||-2147483647.00000000000000000000|2147483647.00000000000000000000||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1969-12-31T23:59:57.852516353Z|1970-01-01T00:00:02.147483647Z||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t|||||||
                DECIMAL(18,3)\t||||-2147483647.000|2147483647.000||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=INT, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-2147483647
                ## single_row-frames
                frame 0 rows=1 native
                min\t01000080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                k\tv
                b:max\t2147483647
                b:min\t-2147483647
                b:null\tnull
                ## parquet_convert
                parquet
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                native
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                """);
        rec("LONG", """
                ## parquet
                k\tv
                d0:min\t-9223372036854775807
                d0:max\t9223372036854775807
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\t-9223372036854775807
                d1:max\t9223372036854775807
                d1:sentinel\tnull
                d1:null\tnull
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t-9223372036854775807
                d0:max\t9223372036854775807
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\t-9223372036854775807
                d1:max\t9223372036854775807
                d1:sentinel\tnull
                d1:null\tnull
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## insert
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## frames
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-9223372036854775807
                sentinel\tnull
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0100000000000080
                sentinel\t0000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|false|false
                BYTE\t0|0|0|0|1|-1|0|0
                SHORT\t0|0|0|0|1|-1|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|1|-1|null|null
                LONG\terror: alter: [41] column 'v' type is already 'LONG'
                DATE\t||||-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                TIMESTAMP\t||||-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                FLOAT\tnull|null|null|null|-9.223372E18|9.223372E18|null|null
                DOUBLE\tnull|null|null|null|-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t||||-9223372036854775807|9223372036854775807||
                SYMBOL\t||||-9223372036854775807|9223372036854775807||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-9223372036854775807|9223372036854775807||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=LONG, new=DOUBLE[]]
                DECIMAL8\t|||||||
                DECIMAL16\t|||||||
                DECIMAL32\t|||||||
                DECIMAL64\t|||||||
                DECIMAL128\t||||-9223372036854775807.0000000000|9223372036854775807.0000000000||
                DECIMAL256\t||||-9223372036854775807.00000000000000000000|9223372036854775807.00000000000000000000||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t|||||||
                DECIMAL(18,3)\t|||||||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=LONG, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-9223372036854775807
                ## single_row-frames
                frame 0 rows=1 native
                min\t0100000000000080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:min\t-9223372036854775807
                shift:min\t9223372036854775807
                dup:max\t9223372036854775807
                shift:max\tnull
                shift:sentinel\tnull
                dup:null\tnull
                shift:null\t-9223372036854775807
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t0100000000000080
                shift:min\tffffffffffffff7f
                dup:max\tffffffffffffff7f
                shift:max\t0000000000000080
                shift:sentinel\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t0100000000000080
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:sentinel\tnull
                d0:null\tnull
                d1:min\tnull
                d1:max\tnull
                d1:sentinel\tnull
                d1:null\tnull
                d2:min\t-9223372036854775807
                d2:max\t9223372036854775807
                d2:sentinel\tnull
                d2:null\tnull
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                frame 3 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t-9223372036854775807
                d0:max\tnull
                o3d0:max\t9223372036854775807
                d0:sentinel\tnull
                o3d0:sentinel\tnull
                d0:null\tnull
                o3d0:null\tnull
                d1:min\tnull
                o3d1:min\t-9223372036854775807
                d1:max\tnull
                o3d1:max\t9223372036854775807
                d1:sentinel\tnull
                o3d1:sentinel\tnull
                d1:null\tnull
                o3d1:null\tnull
                d2:min\t-9223372036854775807
                d2:max\t9223372036854775807
                d2:sentinel\tnull
                d2:null\tnull
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000000000000080
                o3d0:min\t0100000000000080
                d0:max\t0000000000000080
                o3d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                o3d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=8 native
                d1:min\t0000000000000080
                o3d1:min\t0100000000000080
                d1:max\t0000000000000080
                o3d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                o3d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## latest_by_key
                k\tv
                b:max\t9223372036854775807
                b:min\t-9223372036854775807
                b:null\tnull
                ## parquet_convert
                parquet
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                native
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                """);
        rec("DATE", """
                ## insert
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0100000000000080
                sentinel\t0000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                ## single_row-frames
                frame 0 rows=1 native
                min\t0100000000000080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|false|false
                BYTE\t0|0|0|0|1|-1|0|0
                SHORT\t0|0|0|0|1|-1|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|1|-1|null|null
                LONG\tnull|null|null|null|-9223372036854775807|9223372036854775807|null|null
                DATE\terror: alter: [41] column 'v' type is already 'DATE'
                TIMESTAMP\t||||1970-01-01T00:00:00.001000Z|1969-12-31T23:59:59.999000Z||
                FLOAT\tnull|null|null|null|-9.223372E18|9.223372E18|null|null
                DOUBLE\tnull|null|null|null|-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t||||-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                SYMBOL\t||||-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-292275055-05-16T16:47:04.193Z|292278994-08-17T07:12:55.807Z||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DATE, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1970-01-01T00:00:00.001000000Z|1969-12-31T23:59:59.999000000Z||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DATE, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t-292275055-05-16T16:47:04.193Z
                d2:max\t292278994-08-17T07:12:55.807Z
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                frame 3 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-292275055-05-16T16:47:04.193Z
                d0:max\t
                o3d0:max\t292278994-08-17T07:12:55.807Z
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-292275055-05-16T16:47:04.193Z
                d1:max\t
                o3d1:max\t292278994-08-17T07:12:55.807Z
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t-292275055-05-16T16:47:04.193Z
                d2:max\t292278994-08-17T07:12:55.807Z
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000000000000080
                o3d0:min\t0100000000000080
                d0:max\t0000000000000080
                o3d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                o3d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=8 native
                d1:min\t0000000000000080
                o3d1:min\t0100000000000080
                d1:max\t0000000000000080
                o3d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                o3d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## dedup
                k\tv
                dup:min\t-292275055-05-16T16:47:04.193Z
                shift:min\t292278994-08-17T07:12:55.807Z
                dup:max\t292278994-08-17T07:12:55.807Z
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t-292275055-05-16T16:47:04.193Z
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t0100000000000080
                shift:min\tffffffffffffff7f
                dup:max\tffffffffffffff7f
                shift:max\t0000000000000080
                shift:sentinel\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t0100000000000080
                ## parquet
                k\tv
                d0:min\t-292275055-05-16T16:47:04.193Z
                d0:max\t292278994-08-17T07:12:55.807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t-292275055-05-16T16:47:04.193Z
                d1:max\t292278994-08-17T07:12:55.807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t-292275055-05-16T16:47:04.193Z
                d0:max\t292278994-08-17T07:12:55.807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t-292275055-05-16T16:47:04.193Z
                d1:max\t292278994-08-17T07:12:55.807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## latest_by_key
                k\tv
                b:max\t292278994-08-17T07:12:55.807Z
                b:min\t-292275055-05-16T16:47:04.193Z
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                native
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                """);
        rec("TIMESTAMP", """
                ## dedup
                k\tv
                dup:min\t-290308-01-01T19:59:05.224193Z
                shift:min\t294247-01-10T04:00:54.775807Z
                dup:max\t294247-01-10T04:00:54.775807Z
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t-290308-01-01T19:59:05.224193Z
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t0100000000000080
                shift:min\tffffffffffffff7f
                dup:max\tffffffffffffff7f
                shift:max\t0000000000000080
                shift:sentinel\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t0100000000000080
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                ## single_row-frames
                frame 0 rows=1 native
                min\t0100000000000080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t-290308-01-01T19:59:05.224193Z
                d2:max\t294247-01-10T04:00:54.775807Z
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                frame 3 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-290308-01-01T19:59:05.224193Z
                d0:max\t
                o3d0:max\t294247-01-10T04:00:54.775807Z
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-290308-01-01T19:59:05.224193Z
                d1:max\t
                o3d1:max\t294247-01-10T04:00:54.775807Z
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t-290308-01-01T19:59:05.224193Z
                d2:max\t294247-01-10T04:00:54.775807Z
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000000000000080
                o3d0:min\t0100000000000080
                d0:max\t0000000000000080
                o3d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                o3d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=8 native
                d1:min\t0000000000000080
                o3d1:min\t0100000000000080
                d1:max\t0000000000000080
                o3d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                o3d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|false|false
                BYTE\t0|0|0|0|1|-1|0|0
                SHORT\t0|0|0|0|1|-1|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|1|-1|null|null
                LONG\tnull|null|null|null|-9223372036854775807|9223372036854775807|null|null
                DATE\t||||-290308-12-21T19:59:05.225Z|294247-01-10T04:00:54.775Z||
                TIMESTAMP\terror: alter: [41] column 'v' type is already 'TIMESTAMP'
                FLOAT\tnull|null|null|null|-9.223372E18|9.223372E18|null|null
                DOUBLE\tnull|null|null|null|-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t||||-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                SYMBOL\t||||-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||-290308-01-01T19:59:05.224193Z|294247-01-10T04:00:54.775807Z||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=TIMESTAMP, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||1970-01-01T00:00:00.000001000Z|1969-12-31T23:59:59.999999000Z||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=TIMESTAMP, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t-290308-01-01T19:59:05.224193Z
                d0:max\t294247-01-10T04:00:54.775807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t-290308-01-01T19:59:05.224193Z
                d1:max\t294247-01-10T04:00:54.775807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t-290308-01-01T19:59:05.224193Z
                d0:max\t294247-01-10T04:00:54.775807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t-290308-01-01T19:59:05.224193Z
                d1:max\t294247-01-10T04:00:54.775807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## insert
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0100000000000080
                sentinel\t0000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## latest_by_key
                k\tv
                b:max\t294247-01-10T04:00:54.775807Z
                b:min\t-290308-01-01T19:59:05.224193Z
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                native
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                """);
        rec("FLOAT", """
                ## dedup
                k\tv
                dup:min\t-3.4028235E38
                shift:min\t3.4028235E38
                dup:max\t3.4028235E38
                shift:max\tnull
                shift:nan\tnull
                dup:literal_inf\tnull
                shift:literal_inf\t-0.0
                dup:negzero\t-0.0
                shift:negzero\tnull
                dup:null\tnull
                shift:null\t-3.4028235E38
                dup:inf\tnull
                dup:ninf\tnull
                ## dedup-frames
                frame 0 rows=13 native
                dup:min\tffff7fff
                shift:min\tffff7f7f
                dup:max\tffff7f7f
                shift:max\t0000c07f
                shift:nan\t0000c07f
                dup:literal_inf\t0000c07f
                shift:literal_inf\t00000080
                dup:negzero\t00000080
                shift:negzero\t0000c07f
                dup:null\t0000c07f
                shift:null\tffff7fff
                dup:inf\t0000807f
                dup:ninf\t000080ff
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\tnull
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\tnull
                d1:max\tnull
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                d2:min\t-3.4028235E38
                d2:max\t3.4028235E38
                d2:nan\tnull
                d2:literal_inf\tnull
                d2:negzero\t-0.0
                d2:null\tnull
                d2:inf\tnull
                d2:ninf\tnull
                ## tops-frames
                frame 0 rows=8 native
                d0:min\ttop
                d0:max\ttop
                d0:nan\ttop
                d0:literal_inf\ttop
                d0:negzero\ttop
                d0:null\ttop
                d0:inf\ttop
                d0:ninf\ttop
                frame 1 rows=4 native
                d1:min\ttop
                d1:max\ttop
                d1:nan\ttop
                d1:literal_inf\ttop
                frame 2 rows=4 native
                d1:negzero\t00000080
                d1:null\t0000c07f
                d1:inf\t0000807f
                d1:ninf\t000080ff
                frame 3 rows=8 native
                d2:min\tffff7fff
                d2:max\tffff7f7f
                d2:nan\t0000c07f
                d2:literal_inf\t0000c07f
                d2:negzero\t00000080
                d2:null\t0000c07f
                d2:inf\t0000807f
                d2:ninf\t000080ff
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t-3.4028235E38
                d0:max\tnull
                o3d0:max\t3.4028235E38
                d0:nan\tnull
                o3d0:nan\tnull
                d0:literal_inf\tnull
                o3d0:literal_inf\tnull
                d0:negzero\tnull
                o3d0:negzero\t-0.0
                d0:null\tnull
                o3d0:null\tnull
                d0:inf\tnull
                o3d0:inf\tnull
                d0:ninf\tnull
                o3d0:ninf\tnull
                d1:min\tnull
                o3d1:min\t-3.4028235E38
                d1:max\tnull
                o3d1:max\t3.4028235E38
                d1:nan\tnull
                o3d1:nan\tnull
                d1:literal_inf\tnull
                o3d1:literal_inf\tnull
                d1:negzero\t-0.0
                o3d1:negzero\t-0.0
                d1:null\tnull
                o3d1:null\tnull
                d1:inf\tnull
                o3d1:inf\tnull
                d1:ninf\tnull
                o3d1:ninf\tnull
                d2:min\t-3.4028235E38
                d2:max\t3.4028235E38
                d2:nan\tnull
                d2:literal_inf\tnull
                d2:negzero\t-0.0
                d2:null\tnull
                d2:inf\tnull
                d2:ninf\tnull
                ## tops-o3-frames
                frame 0 rows=16 native
                d0:min\t0000c07f
                o3d0:min\tffff7fff
                d0:max\t0000c07f
                o3d0:max\tffff7f7f
                d0:nan\t0000c07f
                o3d0:nan\t0000c07f
                d0:literal_inf\t0000c07f
                o3d0:literal_inf\t0000c07f
                d0:negzero\t0000c07f
                o3d0:negzero\t00000080
                d0:null\t0000c07f
                o3d0:null\t0000c07f
                d0:inf\t0000c07f
                o3d0:inf\t0000807f
                d0:ninf\t0000c07f
                o3d0:ninf\t000080ff
                frame 1 rows=16 native
                d1:min\t0000c07f
                o3d1:min\tffff7fff
                d1:max\t0000c07f
                o3d1:max\tffff7f7f
                d1:nan\t0000c07f
                o3d1:nan\t0000c07f
                d1:literal_inf\t0000c07f
                o3d1:literal_inf\t0000c07f
                d1:negzero\t00000080
                o3d1:negzero\t00000080
                d1:null\t0000c07f
                o3d1:null\t0000c07f
                d1:inf\t0000807f
                o3d1:inf\t0000807f
                d1:ninf\t000080ff
                o3d1:ninf\t000080ff
                frame 2 rows=8 native
                d2:min\tffff7fff
                d2:max\tffff7f7f
                d2:nan\t0000c07f
                d2:literal_inf\t0000c07f
                d2:negzero\t00000080
                d2:null\t0000c07f
                d2:inf\t0000807f
                d2:ninf\t000080ff
                ## parquet
                k\tv
                d0:min\t-3.4028235E38
                d0:max\t3.4028235E38
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\t-0.0
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\t-3.4028235E38
                d1:max\t3.4028235E38
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                ## parquet-frames
                frame 0 rows=8 parquet
                d0:min\tffff7fff
                d0:max\tffff7f7f
                d0:nan\t0000c07f
                d0:literal_inf\t0000c07f
                d0:negzero\t00000080
                d0:null\t0000c07f
                d0:inf\t0000807f
                d0:ninf\t000080ff
                frame 1 rows=8 native
                d1:min\tffff7fff
                d1:max\tffff7f7f
                d1:nan\t0000c07f
                d1:literal_inf\t0000c07f
                d1:negzero\t00000080
                d1:null\t0000c07f
                d1:inf\t0000807f
                d1:ninf\t000080ff
                ## parquet-native
                k\tv
                d0:min\t-3.4028235E38
                d0:max\t3.4028235E38
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\t-0.0
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\t-3.4028235E38
                d1:max\t3.4028235E38
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                ## parquet-native-frames
                frame 0 rows=8 native
                d0:min\tffff7fff
                d0:max\tffff7f7f
                d0:nan\t0000c07f
                d0:literal_inf\t0000c07f
                d0:negzero\t00000080
                d0:null\t0000c07f
                d0:inf\t0000807f
                d0:ninf\t000080ff
                frame 1 rows=8 native
                d1:min\tffff7fff
                d1:max\tffff7f7f
                d1:nan\t0000c07f
                d1:literal_inf\t0000c07f
                d1:negzero\t00000080
                d1:null\t0000c07f
                d1:inf\t0000807f
                d1:ninf\t000080ff
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-3.4028235E38
                ## single_row-frames
                frame 0 rows=1 native
                min\tffff7fff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## frames
                frame 0 rows=8 native
                min\tffff7fff
                max\tffff7f7f
                nan\t0000c07f
                literal_inf\t0000c07f
                negzero\t00000080
                null\t0000c07f
                inf\t0000807f
                ninf\t000080ff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-3.4028235E38
                nan\tnull
                negzero\t-0.0
                inf\tnull
                ninf\tnull
                ## frames-o3-none
                frame 0 rows=5 native
                min\tffff7fff
                nan\t0000c07f
                negzero\t00000080
                inf\t0000807f
                ninf\t000080ff
                ## frames-o3
                frame 0 rows=8 native
                min\tffff7fff
                max\tffff7f7f
                nan\t0000c07f
                literal_inf\t0000c07f
                negzero\t00000080
                null\t0000c07f
                inf\t0000807f
                ninf\t000080ff
                ## alter
                target\td0:min|d0:max|d0:nan|d0:literal_inf|d0:negzero|d0:null|d0:inf|d0:ninf|d1:min|d1:max|d1:nan|d1:literal_inf|d1:negzero|d1:null|d1:inf|d1:ninf
                BOOLEAN\tfalse|false|false|false|false|false|false|false|true|true|false|false|false|false|true|true
                BYTE\t0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|null|null|null|null|null|null|null|null|0|null|null|null
                LONG\tnull|null|null|null|null|null|null|null|null|null|null|null|0|null|null|null
                DATE\t||||||||||||1970-01-01T00:00:00.000Z|||
                TIMESTAMP\t||||||||||||1970-01-01T00:00:00.000000Z|||
                FLOAT\terror: alter: [41] column 'v' type is already 'FLOAT'
                DOUBLE\tnull|null|null|null|null|null|null|null|-3.4028234663852886E38|3.4028234663852886E38|null|null|-0.0|null|null|null
                STRING\t||||||||-3.4028235E38|3.4028235E38|||-0.0|||
                SYMBOL\t||||||||-3.4028235E38|3.4028235E38|||-0.0|||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||||||-3.4028235E38|3.4028235E38|||-0.0|||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=FLOAT, new=DOUBLE[]]
                DECIMAL8\t||||||||||||0.0|||
                DECIMAL16\t||||||||||||0.00|||
                DECIMAL32\t||||||||||||0|||
                DECIMAL64\t||||||||||||0.0000|||
                DECIMAL128\t||||||||||||0.0000000000|||
                DECIMAL256\t||||||||-340282350000000000000000000000000000000.00000000000000000000|340282350000000000000000000000000000000.00000000000000000000|||0.00000000000000000000|||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||||||||||1970-01-01T00:00:00.000000000Z|||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t||||||||||||0.00|||
                DECIMAL(18,3)\t||||||||||||0.000|||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=FLOAT, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:inf\tnull
                b:max\t3.4028235E38
                b:min\t-3.4028235E38
                b:negzero\t-0.0
                b:ninf\tnull
                b:null\tnull
                ## parquet_convert
                parquet
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                native
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                """);
        rec("DOUBLE", """
                ## alter
                target\td0:min|d0:max|d0:nan|d0:literal_inf|d0:negzero|d0:null|d0:inf|d0:ninf|d1:min|d1:max|d1:nan|d1:literal_inf|d1:negzero|d1:null|d1:inf|d1:ninf
                BOOLEAN\tfalse|false|false|false|false|false|false|false|true|true|false|false|false|false|true|true
                BYTE\t0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|null|null|null|null|null|null|null|null|0|null|null|null
                LONG\tnull|null|null|null|null|null|null|null|null|null|null|null|0|null|null|null
                DATE\t||||||||||||1970-01-01T00:00:00.000Z|||
                TIMESTAMP\t||||||||||||1970-01-01T00:00:00.000000Z|||
                FLOAT\tnull|null|null|null|null|null|null|null|null|null|null|null|-0.0|null|null|null
                DOUBLE\terror: alter: [41] column 'v' type is already 'DOUBLE'
                STRING\t||||||||-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                SYMBOL\t||||||||-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||||||-1.7976931348623157E308|1.7976931348623157E308|||-0.0|||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DOUBLE, new=DOUBLE[]]
                DECIMAL8\t||||||||||||0.0|||
                DECIMAL16\t||||||||||||0.00|||
                DECIMAL32\t||||||||||||0|||
                DECIMAL64\t||||||||||||0.0000|||
                DECIMAL128\t||||||||||||0.0000000000|||
                DECIMAL256\t||||||||||||0.00000000000000000000|||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t||||||||||||1970-01-01T00:00:00.000000000Z|||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t||||||||||||0.00|||
                DECIMAL(18,3)\t||||||||||||0.000|||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DOUBLE, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\tnull
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\tnull
                d1:max\tnull
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                d2:min\t-1.7976931348623157E308
                d2:max\t1.7976931348623157E308
                d2:nan\tnull
                d2:literal_inf\tnull
                d2:negzero\t-0.0
                d2:null\tnull
                d2:inf\tnull
                d2:ninf\tnull
                ## tops-frames
                frame 0 rows=8 native
                d0:min\ttop
                d0:max\ttop
                d0:nan\ttop
                d0:literal_inf\ttop
                d0:negzero\ttop
                d0:null\ttop
                d0:inf\ttop
                d0:ninf\ttop
                frame 1 rows=4 native
                d1:min\ttop
                d1:max\ttop
                d1:nan\ttop
                d1:literal_inf\ttop
                frame 2 rows=4 native
                d1:negzero\t0000000000000080
                d1:null\t000000000000f87f
                d1:inf\t000000000000f07f
                d1:ninf\t000000000000f0ff
                frame 3 rows=8 native
                d2:min\tffffffffffffefff
                d2:max\tffffffffffffef7f
                d2:nan\t000000000000f87f
                d2:literal_inf\t000000000000f87f
                d2:negzero\t0000000000000080
                d2:null\t000000000000f87f
                d2:inf\t000000000000f07f
                d2:ninf\t000000000000f0ff
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t-1.7976931348623157E308
                d0:max\tnull
                o3d0:max\t1.7976931348623157E308
                d0:nan\tnull
                o3d0:nan\tnull
                d0:literal_inf\tnull
                o3d0:literal_inf\tnull
                d0:negzero\tnull
                o3d0:negzero\t-0.0
                d0:null\tnull
                o3d0:null\tnull
                d0:inf\tnull
                o3d0:inf\tnull
                d0:ninf\tnull
                o3d0:ninf\tnull
                d1:min\tnull
                o3d1:min\t-1.7976931348623157E308
                d1:max\tnull
                o3d1:max\t1.7976931348623157E308
                d1:nan\tnull
                o3d1:nan\tnull
                d1:literal_inf\tnull
                o3d1:literal_inf\tnull
                d1:negzero\t-0.0
                o3d1:negzero\t-0.0
                d1:null\tnull
                o3d1:null\tnull
                d1:inf\tnull
                o3d1:inf\tnull
                d1:ninf\tnull
                o3d1:ninf\tnull
                d2:min\t-1.7976931348623157E308
                d2:max\t1.7976931348623157E308
                d2:nan\tnull
                d2:literal_inf\tnull
                d2:negzero\t-0.0
                d2:null\tnull
                d2:inf\tnull
                d2:ninf\tnull
                ## tops-o3-frames
                frame 0 rows=16 native
                d0:min\t000000000000f87f
                o3d0:min\tffffffffffffefff
                d0:max\t000000000000f87f
                o3d0:max\tffffffffffffef7f
                d0:nan\t000000000000f87f
                o3d0:nan\t000000000000f87f
                d0:literal_inf\t000000000000f87f
                o3d0:literal_inf\t000000000000f87f
                d0:negzero\t000000000000f87f
                o3d0:negzero\t0000000000000080
                d0:null\t000000000000f87f
                o3d0:null\t000000000000f87f
                d0:inf\t000000000000f87f
                o3d0:inf\t000000000000f07f
                d0:ninf\t000000000000f87f
                o3d0:ninf\t000000000000f0ff
                frame 1 rows=16 native
                d1:min\t000000000000f87f
                o3d1:min\tffffffffffffefff
                d1:max\t000000000000f87f
                o3d1:max\tffffffffffffef7f
                d1:nan\t000000000000f87f
                o3d1:nan\t000000000000f87f
                d1:literal_inf\t000000000000f87f
                o3d1:literal_inf\t000000000000f87f
                d1:negzero\t0000000000000080
                o3d1:negzero\t0000000000000080
                d1:null\t000000000000f87f
                o3d1:null\t000000000000f87f
                d1:inf\t000000000000f07f
                o3d1:inf\t000000000000f07f
                d1:ninf\t000000000000f0ff
                o3d1:ninf\t000000000000f0ff
                frame 2 rows=8 native
                d2:min\tffffffffffffefff
                d2:max\tffffffffffffef7f
                d2:nan\t000000000000f87f
                d2:literal_inf\t000000000000f87f
                d2:negzero\t0000000000000080
                d2:null\t000000000000f87f
                d2:inf\t000000000000f07f
                d2:ninf\t000000000000f0ff
                ## parquet
                k\tv
                d0:min\t-1.7976931348623157E308
                d0:max\t1.7976931348623157E308
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\t-0.0
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\t-1.7976931348623157E308
                d1:max\t1.7976931348623157E308
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                ## parquet-frames
                frame 0 rows=8 parquet
                d0:min\tffffffffffffefff
                d0:max\tffffffffffffef7f
                d0:nan\t000000000000f87f
                d0:literal_inf\t000000000000f87f
                d0:negzero\t0000000000000080
                d0:null\t000000000000f87f
                d0:inf\t000000000000f07f
                d0:ninf\t000000000000f0ff
                frame 1 rows=8 native
                d1:min\tffffffffffffefff
                d1:max\tffffffffffffef7f
                d1:nan\t000000000000f87f
                d1:literal_inf\t000000000000f87f
                d1:negzero\t0000000000000080
                d1:null\t000000000000f87f
                d1:inf\t000000000000f07f
                d1:ninf\t000000000000f0ff
                ## parquet-native
                k\tv
                d0:min\t-1.7976931348623157E308
                d0:max\t1.7976931348623157E308
                d0:nan\tnull
                d0:literal_inf\tnull
                d0:negzero\t-0.0
                d0:null\tnull
                d0:inf\tnull
                d0:ninf\tnull
                d1:min\t-1.7976931348623157E308
                d1:max\t1.7976931348623157E308
                d1:nan\tnull
                d1:literal_inf\tnull
                d1:negzero\t-0.0
                d1:null\tnull
                d1:inf\tnull
                d1:ninf\tnull
                ## parquet-native-frames
                frame 0 rows=8 native
                d0:min\tffffffffffffefff
                d0:max\tffffffffffffef7f
                d0:nan\t000000000000f87f
                d0:literal_inf\t000000000000f87f
                d0:negzero\t0000000000000080
                d0:null\t000000000000f87f
                d0:inf\t000000000000f07f
                d0:ninf\t000000000000f0ff
                frame 1 rows=8 native
                d1:min\tffffffffffffefff
                d1:max\tffffffffffffef7f
                d1:nan\t000000000000f87f
                d1:literal_inf\t000000000000f87f
                d1:negzero\t0000000000000080
                d1:null\t000000000000f87f
                d1:inf\t000000000000f07f
                d1:ninf\t000000000000f0ff
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-1.7976931348623157E308
                ## single_row-frames
                frame 0 rows=1 native
                min\tffffffffffffefff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:min\t-1.7976931348623157E308
                shift:min\t1.7976931348623157E308
                dup:max\t1.7976931348623157E308
                shift:max\tnull
                shift:nan\tnull
                dup:literal_inf\tnull
                shift:literal_inf\t-0.0
                dup:negzero\t-0.0
                shift:negzero\tnull
                dup:null\tnull
                shift:null\t-1.7976931348623157E308
                dup:inf\tnull
                dup:ninf\tnull
                ## dedup-frames
                frame 0 rows=13 native
                dup:min\tffffffffffffefff
                shift:min\tffffffffffffef7f
                dup:max\tffffffffffffef7f
                shift:max\t000000000000f87f
                shift:nan\t000000000000f87f
                dup:literal_inf\t000000000000f87f
                shift:literal_inf\t0000000000000080
                dup:negzero\t0000000000000080
                shift:negzero\t000000000000f87f
                dup:null\t000000000000f87f
                shift:null\tffffffffffffefff
                dup:inf\t000000000000f07f
                dup:ninf\t000000000000f0ff
                ## insert
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## frames
                frame 0 rows=8 native
                min\tffffffffffffefff
                max\tffffffffffffef7f
                nan\t000000000000f87f
                literal_inf\t000000000000f87f
                negzero\t0000000000000080
                null\t000000000000f87f
                inf\t000000000000f07f
                ninf\t000000000000f0ff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-1.7976931348623157E308
                nan\tnull
                negzero\t-0.0
                inf\tnull
                ninf\tnull
                ## frames-o3-none
                frame 0 rows=5 native
                min\tffffffffffffefff
                nan\t000000000000f87f
                negzero\t0000000000000080
                inf\t000000000000f07f
                ninf\t000000000000f0ff
                ## frames-o3
                frame 0 rows=8 native
                min\tffffffffffffefff
                max\tffffffffffffef7f
                nan\t000000000000f87f
                literal_inf\t000000000000f87f
                negzero\t0000000000000080
                null\t000000000000f87f
                inf\t000000000000f07f
                ninf\t000000000000f0ff
                ## latest_by_key
                k\tv
                b:inf\tnull
                b:max\t1.7976931348623157E308
                b:min\t-1.7976931348623157E308
                b:negzero\t-0.0
                b:ninf\tnull
                b:null\tnull
                ## parquet_convert
                parquet
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                native
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                """);
        rec("STRING", """
                ## parquet
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-frames
                frame 0 rows=5 parquet
                d0:empty\tvalue=
                d0:min\tvalue=\s
                d0:max\tvalue=ü€😀�
                d0:escape\tvalue=a"b,c\\d'e
                d0:null\tvalue=
                frame 1 rows=5 native
                d1:empty\taux=0000000000000000 data=00000000
                d1:min\taux=0400000000000000 data=010000002000
                d1:max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                d1:escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                d1:null\taux=2e00000000000000 data=ffffffff
                ## parquet-native
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=5 native
                d0:empty\taux=0000000000000000 data=00000000
                d0:min\taux=0400000000000000 data=010000002000
                d0:max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                d0:escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                d0:null\taux=2e00000000000000 data=ffffffff
                frame 1 rows=5 native
                d1:empty\taux=0000000000000000 data=00000000
                d1:min\taux=0400000000000000 data=010000002000
                d1:max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                d1:escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                d1:null\taux=2e00000000000000 data=ffffffff
                ## tops
                k\tv
                d0:empty\t
                d0:min\t
                d0:max\t
                d0:escape\t
                d0:null\t
                d1:empty\t
                d1:min\t
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-frames
                frame 0 rows=5 native
                d0:empty\ttop
                d0:min\ttop
                d0:max\ttop
                d0:escape\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:empty\ttop
                d1:min\ttop
                frame 2 rows=3 native
                d1:max\taux=0000000000000000 data=05000000fc00ac203dd800defdff
                d1:escape\taux=0e00000000000000 data=090000006100220062002c0063005c00640027006500
                d1:null\taux=2400000000000000 data=ffffffff
                frame 3 rows=5 native
                d2:empty\taux=0000000000000000 data=00000000
                d2:min\taux=0400000000000000 data=010000002000
                d2:max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                d2:escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                d2:null\taux=2e00000000000000 data=ffffffff
                ## tops-o3
                k\tv
                d0:empty\t
                o3d0:empty\t
                d0:min\t
                o3d0:min\t\s
                d0:max\t
                o3d0:max\tü€😀�
                d0:escape\t
                o3d0:escape\ta"b,c\\d'e
                d0:null\t
                o3d0:null\t
                d1:empty\t
                o3d1:empty\t
                d1:min\t
                o3d1:min\t\s
                d1:max\tü€😀�
                o3d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                o3d1:escape\ta"b,c\\d'e
                d1:null\t
                o3d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=10 native
                d0:empty\taux=0000000000000000 data=ffffffff
                o3d0:empty\taux=0400000000000000 data=00000000
                d0:min\taux=0800000000000000 data=ffffffff
                o3d0:min\taux=0c00000000000000 data=010000002000
                d0:max\taux=1200000000000000 data=ffffffff
                o3d0:max\taux=1600000000000000 data=05000000fc00ac203dd800defdff
                d0:escape\taux=2400000000000000 data=ffffffff
                o3d0:escape\taux=2800000000000000 data=090000006100220062002c0063005c00640027006500
                d0:null\taux=3e00000000000000 data=ffffffff
                o3d0:null\taux=4200000000000000 data=ffffffff
                frame 1 rows=10 native
                d1:empty\taux=0000000000000000 data=ffffffff
                o3d1:empty\taux=0400000000000000 data=00000000
                d1:min\taux=0800000000000000 data=ffffffff
                o3d1:min\taux=0c00000000000000 data=010000002000
                d1:max\taux=1200000000000000 data=05000000fc00ac203dd800defdff
                o3d1:max\taux=2000000000000000 data=05000000fc00ac203dd800defdff
                d1:escape\taux=2e00000000000000 data=090000006100220062002c0063005c00640027006500
                o3d1:escape\taux=4400000000000000 data=090000006100220062002c0063005c00640027006500
                d1:null\taux=5a00000000000000 data=ffffffff
                o3d1:null\taux=5e00000000000000 data=ffffffff
                frame 2 rows=5 native
                d2:empty\taux=0000000000000000 data=00000000
                d2:min\taux=0400000000000000 data=010000002000
                d2:max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                d2:escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                d2:null\taux=2e00000000000000 data=ffffffff
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                empty\t
                ## single_row-frames
                frame 0 rows=1 native
                empty\taux=0000000000000000 data=00000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:empty\t
                shift:empty\t\s
                dup:min\t\s
                shift:min\tü€😀�
                dup:max\tü€😀�
                shift:max\ta"b,c\\d'e
                dup:escape\ta"b,c\\d'e
                shift:escape\t
                dup:null\t
                shift:null\t
                ## dedup-frames
                frame 0 rows=10 native
                dup:empty\taux=0000000000000000 data=00000000
                shift:empty\taux=0400000000000000 data=010000002000
                dup:min\taux=0a00000000000000 data=010000002000
                shift:min\taux=1000000000000000 data=05000000fc00ac203dd800defdff
                dup:max\taux=1e00000000000000 data=05000000fc00ac203dd800defdff
                shift:max\taux=2c00000000000000 data=090000006100220062002c0063005c00640027006500
                dup:escape\taux=4200000000000000 data=090000006100220062002c0063005c00640027006500
                shift:escape\taux=5800000000000000 data=ffffffff
                dup:null\taux=5c00000000000000 data=ffffffff
                shift:null\taux=6000000000000000 data=00000000
                ## alter
                target\td0:empty|d0:min|d0:max|d0:escape|d0:null|d1:empty|d1:min|d1:max|d1:escape|d1:null
                BOOLEAN\tfalse|false|false|false|false|false|false|false|false|false
                BYTE\t0|0|0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0|0|0
                CHAR\t|||||| |ü|a|
                INT\tnull|null|null|null|null|null|null|null|null|null
                LONG\tnull|null|null|null|null|null|null|null|null|null
                DATE\t|||||||||
                TIMESTAMP\t|||||||||
                FLOAT\tnull|null|null|null|null|null|null|null|null|null
                DOUBLE\tnull|null|null|null|null|null|null|null|null|null
                STRING\terror: alter: [41] column 'v' type is already 'STRING'
                SYMBOL\t|||||| |ü€😀�|a"b,c\\d'e|
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\t|||||||||
                LONG128\tincompatible [41]
                IPv4\t|||||||||
                VARCHAR\t|||||| |ü€😀�|a"b,c\\d'e|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=STRING, new=DOUBLE[]]
                DECIMAL8\t|||||||||
                DECIMAL16\t|||||||||
                DECIMAL32\t|||||||||
                DECIMAL64\t|||||||||
                DECIMAL128\t|||||||||
                DECIMAL256\t|||||||||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t|||||||||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t|||||||||
                DECIMAL(18,3)\t|||||||||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=STRING, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## insert
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## frames
                frame 0 rows=5 native
                empty\taux=0000000000000000 data=00000000
                min\taux=0400000000000000 data=010000002000
                max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                null\taux=2e00000000000000 data=ffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                empty\t
                max\tü€😀�
                null\t
                ## frames-o3-none
                frame 0 rows=3 native
                empty\taux=0000000000000000 data=00000000
                max\taux=0400000000000000 data=05000000fc00ac203dd800defdff
                null\taux=1200000000000000 data=ffffffff
                ## frames-o3
                frame 0 rows=5 native
                empty\taux=0000000000000000 data=00000000
                min\taux=0400000000000000 data=010000002000
                max\taux=0a00000000000000 data=05000000fc00ac203dd800defdff
                escape\taux=1800000000000000 data=090000006100220062002c0063005c00640027006500
                null\taux=2e00000000000000 data=ffffffff
                ## latest_by_key
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                native
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("SYMBOL", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                empty\t
                ## single_row-frames
                frame 0 rows=1 native
                empty\t00000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## parquet
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-frames
                frame 0 rows=5 parquet
                d0:empty\t00000000
                d0:min\t01000000
                d0:max\t02000000
                d0:escape\t03000000
                d0:null\t00000080
                frame 1 rows=5 native
                d1:empty\t00000000
                d1:min\t01000000
                d1:max\t02000000
                d1:escape\t03000000
                d1:null\t00000080
                ## parquet-native
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=5 native
                d0:empty\t00000000
                d0:min\t01000000
                d0:max\t02000000
                d0:escape\t03000000
                d0:null\t00000080
                frame 1 rows=5 native
                d1:empty\t00000000
                d1:min\t01000000
                d1:max\t02000000
                d1:escape\t03000000
                d1:null\t00000080
                ## dedup
                k\tv
                dup:empty\t
                shift:empty\t\s
                dup:min\t\s
                shift:min\tü€😀�
                dup:max\tü€😀�
                shift:max\ta"b,c\\d'e
                dup:escape\ta"b,c\\d'e
                shift:escape\t
                dup:null\t
                shift:null\t
                ## dedup-frames
                frame 0 rows=10 native
                dup:empty\t00000000
                shift:empty\t01000000
                dup:min\t01000000
                shift:min\t02000000
                dup:max\t02000000
                shift:max\t03000000
                dup:escape\t03000000
                shift:escape\t00000080
                dup:null\t00000080
                shift:null\t00000000
                ## insert
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## frames
                frame 0 rows=5 native
                empty\t00000000
                min\t01000000
                max\t02000000
                escape\t03000000
                null\t00000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                empty\t
                max\tü€😀�
                null\t
                ## frames-o3-none
                frame 0 rows=3 native
                empty\t00000000
                max\t01000000
                null\t00000080
                ## frames-o3
                frame 0 rows=5 native
                empty\t00000000
                min\t02000000
                max\t01000000
                escape\t03000000
                null\t00000080
                ## tops
                k\tv
                d0:empty\t
                d0:min\t
                d0:max\t
                d0:escape\t
                d0:null\t
                d1:empty\t
                d1:min\t
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-frames
                frame 0 rows=5 native
                d0:empty\ttop
                d0:min\ttop
                d0:max\ttop
                d0:escape\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:empty\ttop
                d1:min\ttop
                frame 2 rows=3 native
                d1:max\t00000000
                d1:escape\t01000000
                d1:null\t00000080
                frame 3 rows=5 native
                d2:empty\t02000000
                d2:min\t03000000
                d2:max\t00000000
                d2:escape\t01000000
                d2:null\t00000080
                ## tops-o3
                k\tv
                d0:empty\t
                o3d0:empty\t
                d0:min\t
                o3d0:min\t\s
                d0:max\t
                o3d0:max\tü€😀�
                d0:escape\t
                o3d0:escape\ta"b,c\\d'e
                d0:null\t
                o3d0:null\t
                d1:empty\t
                o3d1:empty\t
                d1:min\t
                o3d1:min\t\s
                d1:max\tü€😀�
                o3d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                o3d1:escape\ta"b,c\\d'e
                d1:null\t
                o3d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=10 native
                d0:empty\t00000080
                o3d0:empty\t02000000
                d0:min\t00000080
                o3d0:min\t03000000
                d0:max\t00000080
                o3d0:max\t00000000
                d0:escape\t00000080
                o3d0:escape\t01000000
                d0:null\t00000080
                o3d0:null\t00000080
                frame 1 rows=10 native
                d1:empty\t00000080
                o3d1:empty\t02000000
                d1:min\t00000080
                o3d1:min\t03000000
                d1:max\t00000000
                o3d1:max\t00000000
                d1:escape\t01000000
                o3d1:escape\t01000000
                d1:null\t00000080
                o3d1:null\t00000080
                frame 2 rows=5 native
                d2:empty\t02000000
                d2:min\t03000000
                d2:max\t00000000
                d2:escape\t01000000
                d2:null\t00000080
                ## symbols
                symbol_count\t4
                0\t
                1\t\s
                2\tü€😀�
                3\ta"b,c\\d'e
                null key\t
                count_distinct
                4
                ## alter
                target\td0:empty|d0:min|d0:max|d0:escape|d0:null|d1:empty|d1:min|d1:max|d1:escape|d1:null
                BOOLEAN\tfalse|false|false|false|false|false|false|false|false|false
                BYTE\t0|0|0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0|0|0
                CHAR\t|||||| |ü|a|
                INT\tnull|null|null|null|null|null|null|null|null|null
                LONG\tnull|null|null|null|null|null|null|null|null|null
                DATE\t|||||||||
                TIMESTAMP\t|||||||||
                FLOAT\tnull|null|null|null|null|null|null|null|null|null
                DOUBLE\tnull|null|null|null|null|null|null|null|null|null
                STRING\t|||||| |ü€😀�|a"b,c\\d'e|
                SYMBOL\terror: alter: [41] column 'v' type is already 'SYMBOL'
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\t|||||||||
                LONG128\tincompatible [41]
                IPv4\t|||||||||
                VARCHAR\t|||||| |ü€😀�|a"b,c\\d'e|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=SYMBOL, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t|||||||||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=SYMBOL, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                native
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("LONG256", """
                ## dedup
                k\tv
                dup:min\t0x00
                shift:min\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                dup:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t0x00
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t0000000000000000000000000000000000000000000000000000000000000000
                shift:min\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                dup:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                shift:max\t0000000000000080000000000000008000000000000000800000000000000080
                shift:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                dup:null\t0000000000000080000000000000008000000000000000800000000000000080
                shift:null\t0000000000000000000000000000000000000000000000000000000000000000
                ## insert
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t0000000000000000000000000000000000000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                null\t0000000000000080000000000000008000000000000000800000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t0x00
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000000000000000000000000000000000000000000000000000000000000000
                sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t0000000000000000000000000000000000000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                null\t0000000000000080000000000000008000000000000000800000000000000080
                ## parquet
                k\tv
                d0:min\t0x00
                d0:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t0x00
                d1:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0000000000000000000000000000000000000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d0:null\t0000000000000080000000000000008000000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t0000000000000000000000000000000000000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d1:null\t0000000000000080000000000000008000000000000000800000000000000080
                ## parquet-native
                k\tv
                d0:min\t0x00
                d0:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t0x00
                d1:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0000000000000000000000000000000000000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d0:null\t0000000000000080000000000000008000000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t0000000000000000000000000000000000000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d1:null\t0000000000000080000000000000008000000000000000800000000000000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t0x00
                d2:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d1:null\t0000000000000080000000000000008000000000000000800000000000000080
                frame 3 rows=4 native
                d2:min\t0000000000000000000000000000000000000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d2:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d2:null\t0000000000000080000000000000008000000000000000800000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t0x00
                d0:max\t
                o3d0:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t0x00
                d1:max\t
                o3d1:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t0x00
                d2:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000000000000080000000000000008000000000000000800000000000000080
                o3d0:min\t0000000000000000000000000000000000000000000000000000000000000000
                d0:max\t0000000000000080000000000000008000000000000000800000000000000080
                o3d0:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d0:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                o3d0:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d0:null\t0000000000000080000000000000008000000000000000800000000000000080
                o3d0:null\t0000000000000080000000000000008000000000000000800000000000000080
                frame 1 rows=8 native
                d1:min\t0000000000000080000000000000008000000000000000800000000000000080
                o3d1:min\t0000000000000000000000000000000000000000000000000000000000000000
                d1:max\t0000000000000080000000000000008000000000000000800000000000000080
                o3d1:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d1:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                o3d1:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d1:null\t0000000000000080000000000000008000000000000000800000000000000080
                o3d1:null\t0000000000000080000000000000008000000000000000800000000000000080
                frame 2 rows=4 native
                d2:min\t0000000000000000000000000000000000000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                d2:sentinel\t0000000000000080000000000000008000000000000000800000000000000080
                d2:null\t0000000000000080000000000000008000000000000000800000000000000080
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t0x00
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000000000000000000000000000000000000000000000000000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\terror: alter: [41] column 'v' type is already 'LONG256'
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=LONG256, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=LONG256, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                b:min\t0x00
                b:null\t
                ## parquet_convert
                error: alter: [39] incompatible column type change [existing=VARCHAR, new=LONG256]
                parquet
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                native
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                """);
        rec("GEOBYTE", """
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\terror: alter: [51] column 'v' type is already 'GEOHASH(7b)'
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(7b), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(7b), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t0000000
                d0:max\t1111111
                d0:null\t
                d1:min\t0000000
                d1:max\t1111111
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t00
                d0:max\t7f
                d0:null\tff
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t7f
                d1:null\tff
                ## parquet-native
                k\tv
                d0:min\t0000000
                d0:max\t1111111
                d0:null\t
                d1:min\t0000000
                d1:max\t1111111
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t00
                d0:max\t7f
                d0:null\tff
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t7f
                d1:null\tff
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t0000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t00
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:min\t0000000
                shift:min\t1111111
                dup:max\t1111111
                shift:max\t
                dup:null\t
                shift:null\t0000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t00
                shift:min\t7f
                dup:max\t7f
                shift:max\tff
                dup:null\tff
                shift:null\t00
                ## insert
                k\tv
                min\t0000000
                max\t1111111
                null\t
                ## frames
                frame 0 rows=3 native
                min\t00
                max\t7f
                null\tff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t0000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00
                null\tff
                ## frames-o3
                frame 0 rows=3 native
                min\t00
                max\t7f
                null\tff
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t1111111
                d1:null\t
                d2:min\t0000000
                d2:max\t1111111
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t7f
                d1:null\tff
                frame 3 rows=3 native
                d2:min\t00
                d2:max\t7f
                d2:null\tff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t0000000
                d0:max\t
                o3d0:max\t1111111
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t0000000
                d1:max\t1111111
                o3d1:max\t1111111
                d1:null\t
                o3d1:null\t
                d2:min\t0000000
                d2:max\t1111111
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tff
                o3d0:min\t00
                d0:max\tff
                o3d0:max\t7f
                d0:null\tff
                o3d0:null\tff
                frame 1 rows=6 native
                d1:min\tff
                o3d1:min\t00
                d1:max\t7f
                o3d1:max\t7f
                d1:null\tff
                o3d1:null\tff
                frame 2 rows=3 native
                d2:min\t00
                d2:max\t7f
                d2:null\tff
                ## latest_by_key
                k\tv
                b:max\t1111111
                b:min\t0000000
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(7b)]
                parquet
                k\tv
                min\t0000000
                max\t1111111
                null\t
                native
                k\tv
                min\t0000000
                max\t1111111
                null\t
                """);
        rec("GEOSHORT", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t000
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\tzzz
                d1:null\t
                d2:min\t000
                d2:max\tzzz
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tff7f
                d1:null\tffff
                frame 3 rows=3 native
                d2:min\t0000
                d2:max\tff7f
                d2:null\tffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t000
                d0:max\t
                o3d0:max\tzzz
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t000
                d1:max\tzzz
                o3d1:max\tzzz
                d1:null\t
                o3d1:null\t
                d2:min\t000
                d2:max\tzzz
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffff
                o3d0:min\t0000
                d0:max\tffff
                o3d0:max\tff7f
                d0:null\tffff
                o3d0:null\tffff
                frame 1 rows=6 native
                d1:min\tffff
                o3d1:min\t0000
                d1:max\tff7f
                o3d1:max\tff7f
                d1:null\tffff
                o3d1:null\tffff
                frame 2 rows=3 native
                d2:min\t0000
                d2:max\tff7f
                d2:null\tffff
                ## parquet
                k\tv
                d0:min\t000
                d0:max\tzzz
                d0:null\t
                d1:min\t000
                d1:max\tzzz
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t0000
                d0:max\tff7f
                d0:null\tffff
                frame 1 rows=3 native
                d1:min\t0000
                d1:max\tff7f
                d1:null\tffff
                ## parquet-native
                k\tv
                d0:min\t000
                d0:max\tzzz
                d0:null\t
                d1:min\t000
                d1:max\tzzz
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t0000
                d0:max\tff7f
                d0:null\tffff
                frame 1 rows=3 native
                d1:min\t0000
                d1:max\tff7f
                d1:null\tffff
                ## insert
                k\tv
                min\t000
                max\tzzz
                null\t
                ## frames
                frame 0 rows=3 native
                min\t0000
                max\tff7f
                null\tffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000
                null\tffff
                ## frames-o3
                frame 0 rows=3 native
                min\t0000
                max\tff7f
                null\tffff
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\terror: alter: [51] column 'v' type is already 'GEOHASH(3c)'
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(3c), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(3c), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## dedup
                k\tv
                dup:min\t000
                shift:min\tzzz
                dup:max\tzzz
                shift:max\t
                dup:null\t
                shift:null\t000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t0000
                shift:min\tff7f
                dup:max\tff7f
                shift:max\tffff
                dup:null\tffff
                shift:null\t0000
                ## latest_by_key
                k\tv
                b:max\tzzz
                b:min\t000
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(3c)]
                parquet
                k\tv
                min\t000
                max\tzzz
                null\t
                native
                k\tv
                min\t000
                max\tzzz
                null\t
                """);
        rec("GEOINT", """
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\tzzzzzz
                d1:null\t
                d2:min\t000000
                d2:max\tzzzzzz
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffffff3f
                d1:null\tffffffff
                frame 3 rows=3 native
                d2:min\t00000000
                d2:max\tffffff3f
                d2:null\tffffffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t000000
                d0:max\t
                o3d0:max\tzzzzzz
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t000000
                d1:max\tzzzzzz
                o3d1:max\tzzzzzz
                d1:null\t
                o3d1:null\t
                d2:min\t000000
                d2:max\tzzzzzz
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffffffff
                o3d0:min\t00000000
                d0:max\tffffffff
                o3d0:max\tffffff3f
                d0:null\tffffffff
                o3d0:null\tffffffff
                frame 1 rows=6 native
                d1:min\tffffffff
                o3d1:min\t00000000
                d1:max\tffffff3f
                o3d1:max\tffffff3f
                d1:null\tffffffff
                o3d1:null\tffffffff
                frame 2 rows=3 native
                d2:min\t00000000
                d2:max\tffffff3f
                d2:null\tffffffff
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\terror: alter: [51] column 'v' type is already 'GEOHASH(6c)'
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(6c), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(6c), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t00000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## frames
                frame 0 rows=3 native
                min\t00000000
                max\tffffff3f
                null\tffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00000000
                null\tffffffff
                ## frames-o3
                frame 0 rows=3 native
                min\t00000000
                max\tffffff3f
                null\tffffffff
                ## parquet
                k\tv
                d0:min\t000000
                d0:max\tzzzzzz
                d0:null\t
                d1:min\t000000
                d1:max\tzzzzzz
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t00000000
                d0:max\tffffff3f
                d0:null\tffffffff
                frame 1 rows=3 native
                d1:min\t00000000
                d1:max\tffffff3f
                d1:null\tffffffff
                ## parquet-native
                k\tv
                d0:min\t000000
                d0:max\tzzzzzz
                d0:null\t
                d1:min\t000000
                d1:max\tzzzzzz
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t00000000
                d0:max\tffffff3f
                d0:null\tffffffff
                frame 1 rows=3 native
                d1:min\t00000000
                d1:max\tffffff3f
                d1:null\tffffffff
                ## dedup
                k\tv
                dup:min\t000000
                shift:min\tzzzzzz
                dup:max\tzzzzzz
                shift:max\t
                dup:null\t
                shift:null\t000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t00000000
                shift:min\tffffff3f
                dup:max\tffffff3f
                shift:max\tffffffff
                dup:null\tffffffff
                shift:null\t00000000
                ## latest_by_key
                k\tv
                b:max\tzzzzzz
                b:min\t000000
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(6c)]
                parquet
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                native
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                """);
        rec("GEOLONG", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t00000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\tzzzzzzzz
                d1:null\t
                d2:min\t00000000
                d2:max\tzzzzzzzz
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffffffffff000000
                d1:null\tffffffffffffffff
                frame 3 rows=3 native
                d2:min\t0000000000000000
                d2:max\tffffffffff000000
                d2:null\tffffffffffffffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t00000000
                d0:max\t
                o3d0:max\tzzzzzzzz
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t00000000
                d1:max\tzzzzzzzz
                o3d1:max\tzzzzzzzz
                d1:null\t
                o3d1:null\t
                d2:min\t00000000
                d2:max\tzzzzzzzz
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffffffffffffffff
                o3d0:min\t0000000000000000
                d0:max\tffffffffffffffff
                o3d0:max\tffffffffff000000
                d0:null\tffffffffffffffff
                o3d0:null\tffffffffffffffff
                frame 1 rows=6 native
                d1:min\tffffffffffffffff
                o3d1:min\t0000000000000000
                d1:max\tffffffffff000000
                o3d1:max\tffffffffff000000
                d1:null\tffffffffffffffff
                o3d1:null\tffffffffffffffff
                frame 2 rows=3 native
                d2:min\t0000000000000000
                d2:max\tffffffffff000000
                d2:null\tffffffffffffffff
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\terror: alter: [51] column 'v' type is already 'GEOHASH(8c)'
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(8c), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(8c), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## insert
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## frames
                frame 0 rows=3 native
                min\t0000000000000000
                max\tffffffffff000000
                null\tffffffffffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t00000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000000000000000
                null\tffffffffffffffff
                ## frames-o3
                frame 0 rows=3 native
                min\t0000000000000000
                max\tffffffffff000000
                null\tffffffffffffffff
                ## parquet
                k\tv
                d0:min\t00000000
                d0:max\tzzzzzzzz
                d0:null\t
                d1:min\t00000000
                d1:max\tzzzzzzzz
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t0000000000000000
                d0:max\tffffffffff000000
                d0:null\tffffffffffffffff
                frame 1 rows=3 native
                d1:min\t0000000000000000
                d1:max\tffffffffff000000
                d1:null\tffffffffffffffff
                ## parquet-native
                k\tv
                d0:min\t00000000
                d0:max\tzzzzzzzz
                d0:null\t
                d1:min\t00000000
                d1:max\tzzzzzzzz
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t0000000000000000
                d0:max\tffffffffff000000
                d0:null\tffffffffffffffff
                frame 1 rows=3 native
                d1:min\t0000000000000000
                d1:max\tffffffffff000000
                d1:null\tffffffffffffffff
                ## dedup
                k\tv
                dup:min\t00000000
                shift:min\tzzzzzzzz
                dup:max\tzzzzzzzz
                shift:max\t
                dup:null\t
                shift:null\t00000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t0000000000000000
                shift:min\tffffffffff000000
                dup:max\tffffffffff000000
                shift:max\tffffffffffffffff
                dup:null\tffffffffffffffff
                shift:null\t0000000000000000
                ## latest_by_key
                k\tv
                b:max\tzzzzzzzz
                b:min\t00000000
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(8c)]
                parquet
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                native
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                """);
        rec("BINARY", """
                ## dedup
                k\tv
                dup:empty\t
                shift:empty\t00000000 00 01 02 fd fe ff
                dup:max\t00000000 00 01 02 fd fe ff
                shift:max\t
                dup:null\t
                shift:null\t
                ## dedup-frames
                frame 0 rows=6 native
                dup:empty\taux=0000000000000000 data=0000000000000000
                shift:empty\taux=0800000000000000 data=0600000000000000000102fdfeff
                dup:max\taux=1600000000000000 data=0600000000000000000102fdfeff
                shift:max\taux=2400000000000000 data=ffffffffffffffff
                dup:null\taux=2c00000000000000 data=ffffffffffffffff
                shift:null\taux=3400000000000000 data=0000000000000000
                ## parquet
                k\tv
                d0:empty\t
                d0:max\t00000000 00 01 02 fd fe ff
                d0:null\t
                d1:empty\t
                d1:max\t00000000 00 01 02 fd fe ff
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:empty\tvalue=
                d0:max\tvalue=00000000 00 01 02 fd fe ff
                d0:null\tvalue=
                frame 1 rows=3 native
                d1:empty\taux=0000000000000000 data=0000000000000000
                d1:max\taux=0800000000000000 data=0600000000000000000102fdfeff
                d1:null\taux=1600000000000000 data=ffffffffffffffff
                ## parquet-native
                k\tv
                d0:empty\t
                d0:max\t00000000 00 01 02 fd fe ff
                d0:null\t
                d1:empty\t
                d1:max\t00000000 00 01 02 fd fe ff
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:empty\taux=0000000000000000 data=0000000000000000
                d0:max\taux=0800000000000000 data=0600000000000000000102fdfeff
                d0:null\taux=1600000000000000 data=ffffffffffffffff
                frame 1 rows=3 native
                d1:empty\taux=0000000000000000 data=0000000000000000
                d1:max\taux=0800000000000000 data=0600000000000000000102fdfeff
                d1:null\taux=1600000000000000 data=ffffffffffffffff
                ## insert
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## frames
                frame 0 rows=3 native
                empty\taux=0000000000000000 data=0000000000000000
                max\taux=0800000000000000 data=0600000000000000000102fdfeff
                null\taux=1600000000000000 data=ffffffffffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                empty\t
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                empty\taux=0000000000000000 data=0000000000000000
                null\taux=0800000000000000 data=ffffffffffffffff
                ## frames-o3
                frame 0 rows=3 native
                empty\taux=0000000000000000 data=0000000000000000
                max\taux=0800000000000000 data=0600000000000000000102fdfeff
                null\taux=1600000000000000 data=ffffffffffffffff
                ## tops
                k\tv
                d0:empty\t
                d0:max\t
                d0:null\t
                d1:empty\t
                d1:max\t00000000 00 01 02 fd fe ff
                d1:null\t
                d2:empty\t
                d2:max\t00000000 00 01 02 fd fe ff
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:empty\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:empty\ttop
                frame 2 rows=2 native
                d1:max\taux=0000000000000000 data=0600000000000000000102fdfeff
                d1:null\taux=0e00000000000000 data=ffffffffffffffff
                frame 3 rows=3 native
                d2:empty\taux=0000000000000000 data=0000000000000000
                d2:max\taux=0800000000000000 data=0600000000000000000102fdfeff
                d2:null\taux=1600000000000000 data=ffffffffffffffff
                ## tops-o3
                k\tv
                d0:empty\t
                o3d0:empty\t
                d0:max\t
                o3d0:max\t00000000 00 01 02 fd fe ff
                d0:null\t
                o3d0:null\t
                d1:empty\t
                o3d1:empty\t
                d1:max\t00000000 00 01 02 fd fe ff
                o3d1:max\t00000000 00 01 02 fd fe ff
                d1:null\t
                o3d1:null\t
                d2:empty\t
                d2:max\t00000000 00 01 02 fd fe ff
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:empty\taux=0000000000000000 data=ffffffffffffffff
                o3d0:empty\taux=0800000000000000 data=0000000000000000
                d0:max\taux=1000000000000000 data=ffffffffffffffff
                o3d0:max\taux=1800000000000000 data=0600000000000000000102fdfeff
                d0:null\taux=2600000000000000 data=ffffffffffffffff
                o3d0:null\taux=2e00000000000000 data=ffffffffffffffff
                frame 1 rows=6 native
                d1:empty\taux=0000000000000000 data=ffffffffffffffff
                o3d1:empty\taux=0800000000000000 data=0000000000000000
                d1:max\taux=1000000000000000 data=0600000000000000000102fdfeff
                o3d1:max\taux=1e00000000000000 data=0600000000000000000102fdfeff
                d1:null\taux=2c00000000000000 data=ffffffffffffffff
                o3d1:null\taux=3400000000000000 data=ffffffffffffffff
                frame 2 rows=3 native
                d2:empty\taux=0000000000000000 data=0000000000000000
                d2:max\taux=0800000000000000 data=0600000000000000000102fdfeff
                d2:null\taux=1600000000000000 data=ffffffffffffffff
                ## alter
                target\td0:empty|d0:max|d0:null|d1:empty|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\terror: alter: [41] column 'v' type is already 'BINARY'
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=BINARY, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=BINARY, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                empty\t
                ## single_row-frames
                frame 0 rows=1 native
                empty\taux=0000000000000000 data=0000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                error: [51] v (BINARY): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                error: alter: [39] incompatible column type change [existing=VARCHAR, new=BINARY]
                parquet
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                native
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                """);
        rec("UUID", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t00000000000000000000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## parquet
                k\tv
                d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t00000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t00000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                ## parquet-native
                k\tv
                d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t00000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t00000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t00000000-0000-0000-0000-000000000000
                d2:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                frame 3 rows=4 native
                d2:min\t00000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffff
                d2:sentinel\t00000000000000800000000000000080
                d2:null\t00000000000000800000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\t
                o3d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\t
                o3d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t00000000-0000-0000-0000-000000000000
                d2:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t00000000000000800000000000000080
                o3d0:min\t00000000000000000000000000000000
                d0:max\t00000000000000800000000000000080
                o3d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                o3d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                o3d0:null\t00000000000000800000000000000080
                frame 1 rows=8 native
                d1:min\t00000000000000800000000000000080
                o3d1:min\t00000000000000000000000000000000
                d1:max\t00000000000000800000000000000080
                o3d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                o3d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                o3d1:null\t00000000000000800000000000000080
                frame 2 rows=4 native
                d2:min\t00000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffff
                d2:sentinel\t00000000000000800000000000000080
                d2:null\t00000000000000800000000000000080
                ## dedup
                k\tv
                dup:min\t00000000-0000-0000-0000-000000000000
                shift:min\tffffffff-ffff-ffff-ffff-ffffffffffff
                dup:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t00000000-0000-0000-0000-000000000000
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t00000000000000000000000000000000
                shift:min\tffffffffffffffffffffffffffffffff
                dup:max\tffffffffffffffffffffffffffffffff
                shift:max\t00000000000000800000000000000080
                shift:sentinel\t00000000000000800000000000000080
                dup:null\t00000000000000800000000000000080
                shift:null\t00000000000000000000000000000000
                ## insert
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t00000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffff
                sentinel\t00000000000000800000000000000080
                null\t00000000000000800000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00000000000000000000000000000000
                sentinel\t00000000000000800000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t00000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffff
                sentinel\t00000000000000800000000000000080
                null\t00000000000000800000000000000080
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\t||||00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                SYMBOL\t||||00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\terror: alter: [41] column 'v' type is already 'UUID'
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=UUID, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=UUID, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                b:min\t00000000-0000-0000-0000-000000000000
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                native
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                """);
        rec("LONG128", """
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t00000000-0000-0000-0000-000000000000
                d2:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                frame 3 rows=4 native
                d2:min\t00000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffff
                d2:sentinel\t00000000000000800000000000000080
                d2:null\t00000000000000800000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\t
                o3d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\t
                o3d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t00000000-0000-0000-0000-000000000000
                d2:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t00000000000000800000000000000080
                o3d0:min\t00000000000000000000000000000000
                d0:max\t00000000000000800000000000000080
                o3d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                o3d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                o3d0:null\t00000000000000800000000000000080
                frame 1 rows=8 native
                d1:min\t00000000000000800000000000000080
                o3d1:min\t00000000000000000000000000000000
                d1:max\t00000000000000800000000000000080
                o3d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                o3d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                o3d1:null\t00000000000000800000000000000080
                frame 2 rows=4 native
                d2:min\t00000000000000000000000000000000
                d2:max\tffffffffffffffffffffffffffffffff
                d2:sentinel\t00000000000000800000000000000080
                d2:null\t00000000000000800000000000000080
                ## parquet
                k\tv
                d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t00000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t00000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                ## parquet-native
                k\tv
                d0:min\t00000000-0000-0000-0000-000000000000
                d0:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d0:sentinel\t
                d0:null\t
                d1:min\t00000000-0000-0000-0000-000000000000
                d1:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t00000000000000000000000000000000
                d0:max\tffffffffffffffffffffffffffffffff
                d0:sentinel\t00000000000000800000000000000080
                d0:null\t00000000000000800000000000000080
                frame 1 rows=4 native
                d1:min\t00000000000000000000000000000000
                d1:max\tffffffffffffffffffffffffffffffff
                d1:sentinel\t00000000000000800000000000000080
                d1:null\t00000000000000800000000000000080
                ## dedup
                k\tv
                dup:min\t00000000-0000-0000-0000-000000000000
                shift:min\tffffffff-ffff-ffff-ffff-ffffffffffff
                dup:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t00000000-0000-0000-0000-000000000000
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t00000000000000000000000000000000
                shift:min\tffffffffffffffffffffffffffffffff
                dup:max\tffffffffffffffffffffffffffffffff
                shift:max\t00000000000000800000000000000080
                shift:sentinel\t00000000000000800000000000000080
                dup:null\t00000000000000800000000000000080
                shift:null\t00000000000000000000000000000000
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\terror: alter: [41] column 'v' type is already 'LONG128'
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=LONG128, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=LONG128, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t00000000000000000000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t00000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffff
                sentinel\t00000000000000800000000000000080
                null\t00000000000000800000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00000000000000000000000000000000
                sentinel\t00000000000000800000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t00000000000000000000000000000000
                max\tffffffffffffffffffffffffffffffff
                sentinel\t00000000000000800000000000000080
                null\t00000000000000800000000000000080
                ## latest_by_key
                k\tv
                b:max\tffffffff-ffff-ffff-ffff-ffffffffffff
                b:min\t00000000-0000-0000-0000-000000000000
                b:null\t
                ## parquet_convert
                error: alter: [39] incompatible column type change [existing=VARCHAR, new=LONG128]
                parquet
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                native
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                """);
        rec("IPv4", """
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t0.0.0.1
                d2:max\t255.255.255.255
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t00000000
                d1:null\t00000000
                frame 3 rows=4 native
                d2:min\t01000000
                d2:max\tffffffff
                d2:sentinel\t00000000
                d2:null\t00000000
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t0.0.0.1
                d0:max\t
                o3d0:max\t255.255.255.255
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t0.0.0.1
                d1:max\t
                o3d1:max\t255.255.255.255
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t0.0.0.1
                d2:max\t255.255.255.255
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t00000000
                o3d0:min\t01000000
                d0:max\t00000000
                o3d0:max\tffffffff
                d0:sentinel\t00000000
                o3d0:sentinel\t00000000
                d0:null\t00000000
                o3d0:null\t00000000
                frame 1 rows=8 native
                d1:min\t00000000
                o3d1:min\t01000000
                d1:max\t00000000
                o3d1:max\tffffffff
                d1:sentinel\t00000000
                o3d1:sentinel\t00000000
                d1:null\t00000000
                o3d1:null\t00000000
                frame 2 rows=4 native
                d2:min\t01000000
                d2:max\tffffffff
                d2:sentinel\t00000000
                d2:null\t00000000
                ## dedup
                k\tv
                dup:min\t0.0.0.1
                shift:min\t255.255.255.255
                dup:max\t255.255.255.255
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t0.0.0.1
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t01000000
                shift:min\tffffffff
                dup:max\tffffffff
                shift:max\t00000000
                shift:sentinel\t00000000
                dup:null\t00000000
                shift:null\t01000000
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\t||||0.0.0.1|255.255.255.255||
                SYMBOL\t||||0.0.0.1|255.255.255.255||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\terror: alter: [41] column 'v' type is already 'IPv4'
                VARCHAR\t||||0.0.0.1|255.255.255.255||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=IPv4, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=IPv4, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t0.0.0.1
                d0:max\t255.255.255.255
                d0:sentinel\t
                d0:null\t
                d1:min\t0.0.0.1
                d1:max\t255.255.255.255
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t01000000
                d0:max\tffffffff
                d0:sentinel\t00000000
                d0:null\t00000000
                frame 1 rows=4 native
                d1:min\t01000000
                d1:max\tffffffff
                d1:sentinel\t00000000
                d1:null\t00000000
                ## parquet-native
                k\tv
                d0:min\t0.0.0.1
                d0:max\t255.255.255.255
                d0:sentinel\t
                d0:null\t
                d1:min\t0.0.0.1
                d1:max\t255.255.255.255
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t01000000
                d0:max\tffffffff
                d0:sentinel\t00000000
                d0:null\t00000000
                frame 1 rows=4 native
                d1:min\t01000000
                d1:max\tffffffff
                d1:sentinel\t00000000
                d1:null\t00000000
                ## insert
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t01000000
                max\tffffffff
                sentinel\t00000000
                null\t00000000
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t0.0.0.1
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t01000000
                sentinel\t00000000
                ## frames-o3
                frame 0 rows=4 native
                min\t01000000
                max\tffffffff
                sentinel\t00000000
                null\t00000000
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t0.0.0.1
                ## single_row-frames
                frame 0 rows=1 native
                min\t01000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                k\tv
                b:max\t255.255.255.255
                b:min\t0.0.0.1
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                native
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                """);
        rec("VARCHAR", """
                ## insert
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## frames
                frame 0 rows=5 native
                empty\taux=03000000000000000000000000000000 data=
                min\taux=13200000000000000000000000000000 data=
                max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                escape\taux=936122622c635c6427650c0000000000 data=
                null\taux=040000000000000000000c0000000000 data=
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                empty\t
                max\tü€😀�
                null\t
                ## frames-o3-none
                frame 0 rows=3 native
                empty\taux=03000000000000000000000000000000 data=
                max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                null\taux=040000000000000000000c0000000000 data=
                ## frames-o3
                frame 0 rows=5 native
                empty\taux=03000000000000000000000000000000 data=
                min\taux=13200000000000000000000000000000 data=
                max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                escape\taux=936122622c635c6427650c0000000000 data=
                null\taux=040000000000000000000c0000000000 data=
                ## alter
                target\td0:empty|d0:min|d0:max|d0:escape|d0:null|d1:empty|d1:min|d1:max|d1:escape|d1:null
                BOOLEAN\tfalse|false|false|false|false|false|false|false|false|false
                BYTE\t0|0|0|0|0|0|0|0|0|0
                SHORT\t0|0|0|0|0|0|0|0|0|0
                CHAR\t|||||| |ü|a|
                INT\tnull|null|null|null|null|null|null|null|null|null
                LONG\tnull|null|null|null|null|null|null|null|null|null
                DATE\t|||||||||
                TIMESTAMP\t|||||||||
                FLOAT\tnull|null|null|null|null|null|null|null|null|null
                DOUBLE\tnull|null|null|null|null|null|null|null|null|null
                STRING\t|||||| |ü€😀�|a"b,c\\d'e|
                SYMBOL\t|||||| |ü€😀�|a"b,c\\d'e|
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\t|||||||||
                LONG128\tincompatible [41]
                IPv4\t|||||||||
                VARCHAR\terror: alter: [41] column 'v' type is already 'VARCHAR'
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=VARCHAR, new=DOUBLE[]]
                DECIMAL8\t|||||||||
                DECIMAL16\t|||||||||
                DECIMAL32\t|||||||||
                DECIMAL64\t|||||||||
                DECIMAL128\t|||||||||
                DECIMAL256\t|||||||||
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\t|||||||||
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\t|||||||||
                DECIMAL(18,3)\t|||||||||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=VARCHAR, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                empty\t
                ## single_row-frames
                frame 0 rows=1 native
                empty\taux=03000000000000000000000000000000 data=
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## parquet
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-frames
                frame 0 rows=5 parquet
                d0:empty\tvalue=
                d0:min\tvalue=\s
                d0:max\tvalue=ü€😀�
                d0:escape\tvalue=a"b,c\\d'e
                d0:null\tvalue=
                frame 1 rows=5 native
                d1:empty\taux=03000000000000000000000000000000 data=
                d1:min\taux=13200000000000000000000000000000 data=
                d1:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d1:escape\taux=936122622c635c6427650c0000000000 data=
                d1:null\taux=040000000000000000000c0000000000 data=
                ## parquet-native
                k\tv
                d0:empty\t
                d0:min\t\s
                d0:max\tü€😀�
                d0:escape\ta"b,c\\d'e
                d0:null\t
                d1:empty\t
                d1:min\t\s
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=5 native
                d0:empty\taux=03000000000000000000000000000000 data=
                d0:min\taux=13200000000000000000000000000000 data=
                d0:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d0:escape\taux=936122622c635c6427650c0000000000 data=
                d0:null\taux=040000000000000000000c0000000000 data=
                frame 1 rows=5 native
                d1:empty\taux=03000000000000000000000000000000 data=
                d1:min\taux=13200000000000000000000000000000 data=
                d1:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d1:escape\taux=936122622c635c6427650c0000000000 data=
                d1:null\taux=040000000000000000000c0000000000 data=
                ## dedup
                k\tv
                dup:empty\t
                shift:empty\t\s
                dup:min\t\s
                shift:min\tü€😀�
                dup:max\tü€😀�
                shift:max\ta"b,c\\d'e
                dup:escape\ta"b,c\\d'e
                shift:escape\t
                dup:null\t
                shift:null\t
                ## dedup-frames
                frame 0 rows=10 native
                dup:empty\taux=03000000000000000000000000000000 data=
                shift:empty\taux=13200000000000000000000000000000 data=
                dup:min\taux=13200000000000000000000000000000 data=
                shift:min\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                dup:max\taux=c0000000c3bce282acf00c0000000000 data=c3bce282acf09f9880efbfbd
                shift:max\taux=936122622c635c642765180000000000 data=
                dup:escape\taux=936122622c635c642765180000000000 data=
                shift:escape\taux=04000000000000000000180000000000 data=
                dup:null\taux=04000000000000000000180000000000 data=
                shift:null\taux=03000000000000000000180000000000 data=
                ## tops
                k\tv
                d0:empty\t
                d0:min\t
                d0:max\t
                d0:escape\t
                d0:null\t
                d1:empty\t
                d1:min\t
                d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-frames
                frame 0 rows=5 native
                d0:empty\ttop
                d0:min\ttop
                d0:max\ttop
                d0:escape\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:empty\ttop
                d1:min\ttop
                frame 2 rows=3 native
                d1:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d1:escape\taux=936122622c635c6427650c0000000000 data=
                d1:null\taux=040000000000000000000c0000000000 data=
                frame 3 rows=5 native
                d2:empty\taux=03000000000000000000000000000000 data=
                d2:min\taux=13200000000000000000000000000000 data=
                d2:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d2:escape\taux=936122622c635c6427650c0000000000 data=
                d2:null\taux=040000000000000000000c0000000000 data=
                ## tops-o3
                k\tv
                d0:empty\t
                o3d0:empty\t
                d0:min\t
                o3d0:min\t\s
                d0:max\t
                o3d0:max\tü€😀�
                d0:escape\t
                o3d0:escape\ta"b,c\\d'e
                d0:null\t
                o3d0:null\t
                d1:empty\t
                o3d1:empty\t
                d1:min\t
                o3d1:min\t\s
                d1:max\tü€😀�
                o3d1:max\tü€😀�
                d1:escape\ta"b,c\\d'e
                o3d1:escape\ta"b,c\\d'e
                d1:null\t
                o3d1:null\t
                d2:empty\t
                d2:min\t\s
                d2:max\tü€😀�
                d2:escape\ta"b,c\\d'e
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=10 native
                d0:empty\taux=04000000000000000000000000000000 data=
                o3d0:empty\taux=03000000000000000000000000000000 data=
                d0:min\taux=04000000000000000000000000000000 data=
                o3d0:min\taux=13200000000000000000000000000000 data=
                d0:max\taux=04000000000000000000000000000000 data=
                o3d0:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d0:escape\taux=040000000000000000000c0000000000 data=
                o3d0:escape\taux=936122622c635c6427650c0000000000 data=
                d0:null\taux=040000000000000000000c0000000000 data=
                o3d0:null\taux=040000000000000000000c0000000000 data=
                frame 1 rows=10 native
                d1:empty\taux=04000000000000000000000000000000 data=
                o3d1:empty\taux=03000000000000000000000000000000 data=
                d1:min\taux=04000000000000000000000000000000 data=
                o3d1:min\taux=13200000000000000000000000000000 data=
                d1:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                o3d1:max\taux=c0000000c3bce282acf00c0000000000 data=c3bce282acf09f9880efbfbd
                d1:escape\taux=936122622c635c642765180000000000 data=
                o3d1:escape\taux=936122622c635c642765180000000000 data=
                d1:null\taux=04000000000000000000180000000000 data=
                o3d1:null\taux=04000000000000000000180000000000 data=
                frame 2 rows=5 native
                d2:empty\taux=03000000000000000000000000000000 data=
                d2:min\taux=13200000000000000000000000000000 data=
                d2:max\taux=c0000000c3bce282acf0000000000000 data=c3bce282acf09f9880efbfbd
                d2:escape\taux=936122622c635c6427650c0000000000 data=
                d2:null\taux=040000000000000000000c0000000000 data=
                ## latest_by_key
                k\tv
                b:empty\t
                b:escape\ta"b,c\\d'e
                b:max\tü€😀�
                b:min\t\s
                b:null\t
                ## parquet_convert
                error: alter: [39] column 'v' type is already 'VARCHAR'
                parquet
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                native
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("DOUBLE[]", """
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:empty\tnull
                d0:specials\tnull
                d0:null\tnull
                d1:min\tnull
                d1:max\tnull
                d1:empty\t[]
                d1:specials\t[null,null,null,-0.0]
                d1:null\tnull
                d2:min\t[-1.7976931348623157E308]
                d2:max\t[1.7976931348623157E308]
                d2:empty\t[]
                d2:specials\t[null,null,null,-0.0]
                d2:null\tnull
                ## tops-frames
                frame 0 rows=5 native
                d0:min\ttop
                d0:max\ttop
                d0:empty\ttop
                d0:specials\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=3 native
                d1:empty\taux=00000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=08000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=30000000000000000000000000000000 data=
                frame 3 rows=5 native
                d2:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d2:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d2:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d2:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d2:null\taux=50000000000000000000000000000000 data=
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t[-1.7976931348623157E308]
                d0:max\tnull
                o3d0:max\t[1.7976931348623157E308]
                d0:empty\tnull
                o3d0:empty\t[]
                d0:specials\tnull
                o3d0:specials\t[null,null,null,-0.0]
                d0:null\tnull
                o3d0:null\tnull
                d1:min\tnull
                o3d1:min\t[-1.7976931348623157E308]
                d1:max\tnull
                o3d1:max\t[1.7976931348623157E308]
                d1:empty\t[]
                o3d1:empty\t[]
                d1:specials\t[null,null,null,-0.0]
                o3d1:specials\t[null,null,null,-0.0]
                d1:null\tnull
                o3d1:null\tnull
                d2:min\t[-1.7976931348623157E308]
                d2:max\t[1.7976931348623157E308]
                d2:empty\t[]
                d2:specials\t[null,null,null,-0.0]
                d2:null\tnull
                ## tops-o3-frames
                frame 0 rows=10 native
                d0:min\taux=00000000000000000000000000000000 data=
                o3d0:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d0:max\taux=10000000000000000000000000000000 data=
                o3d0:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d0:empty\taux=20000000000000000000000000000000 data=
                o3d0:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d0:specials\taux=28000000000000000000000000000000 data=
                o3d0:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d0:null\taux=50000000000000000000000000000000 data=
                o3d0:null\taux=50000000000000000000000000000000 data=
                frame 1 rows=10 native
                d1:min\taux=00000000000000000000000000000000 data=
                o3d1:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d1:max\taux=10000000000000000000000000000000 data=
                o3d1:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                o3d1:empty\taux=28000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=30000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                o3d1:specials\taux=58000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=80000000000000000000000000000000 data=
                o3d1:null\taux=80000000000000000000000000000000 data=
                frame 2 rows=5 native
                d2:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d2:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d2:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d2:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d2:null\taux=50000000000000000000000000000000 data=
                ## insert
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## frames
                frame 0 rows=5 native
                min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                empty\taux=20000000000000000800000000000000 data=0000000000000000
                specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                null\taux=50000000000000000000000000000000 data=
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t[-1.7976931348623157E308]
                empty\t[]
                null\tnull
                ## frames-o3-none
                frame 0 rows=3 native
                min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                empty\taux=10000000000000000800000000000000 data=0000000000000000
                null\taux=18000000000000000000000000000000 data=
                ## frames-o3
                frame 0 rows=5 native
                min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                empty\taux=20000000000000000800000000000000 data=0000000000000000
                specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                null\taux=50000000000000000000000000000000 data=
                ## parquet
                k\tv
                d0:min\t[-1.7976931348623157E308]
                d0:max\t[1.7976931348623157E308]
                d0:empty\t[]
                d0:specials\t[null,null,null,-0.0]
                d0:null\tnull
                d1:min\t[-1.7976931348623157E308]
                d1:max\t[1.7976931348623157E308]
                d1:empty\t[]
                d1:specials\t[null,null,null,-0.0]
                d1:null\tnull
                ## parquet-frames
                frame 0 rows=5 parquet
                d0:min\tvalue=[-1.7976931348623157E308]
                d0:max\tvalue=[1.7976931348623157E308]
                d0:empty\tvalue=[]
                d0:specials\tvalue=[null,null,null,-0.0]
                d0:null\tvalue=null
                frame 1 rows=5 native
                d1:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d1:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=50000000000000000000000000000000 data=
                ## parquet-native
                k\tv
                d0:min\t[-1.7976931348623157E308]
                d0:max\t[1.7976931348623157E308]
                d0:empty\t[]
                d0:specials\t[null,null,null,-0.0]
                d0:null\tnull
                d1:min\t[-1.7976931348623157E308]
                d1:max\t[1.7976931348623157E308]
                d1:empty\t[]
                d1:specials\t[null,null,null,-0.0]
                d1:null\tnull
                ## parquet-native-frames
                frame 0 rows=5 native
                d0:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d0:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d0:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d0:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d0:null\taux=50000000000000000000000000000000 data=
                frame 1 rows=5 native
                d1:min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                d1:max\taux=10000000000000001000000000000000 data=0100000000000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=28000000000000002800000000000000 data=0400000000000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=50000000000000000000000000000000 data=
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t[-1.7976931348623157E308]
                ## single_row-frames
                frame 0 rows=1 native
                min\taux=00000000000000001000000000000000 data=0100000000000000ffffffffffffefff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                error: create: [116] dedup key columns cannot include ARRAY [column=v, type=DOUBLE[]]
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## alter
                target\td0:min|d0:max|d0:empty|d0:specials|d0:null|d1:min|d1:max|d1:empty|d1:specials|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=TIMESTAMP]
                FLOAT\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=FLOAT]
                DOUBLE\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=DOUBLE]
                STRING\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=STRING]
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=IPv4]
                VARCHAR\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=VARCHAR]
                DOUBLE[]\terror: alter: [48] column 'v' type is already 'DOUBLE[]'
                DECIMAL8\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=DECIMAL(2,1)]
                DECIMAL16\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=DECIMAL(4,2)]
                DECIMAL32\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=DECIMAL(9,0)]
                DECIMAL64\terror: alter: [53] incompatible column type change [existing=DOUBLE[], new=DECIMAL(16,4)]
                DECIMAL128\terror: alter: [54] incompatible column type change [existing=DOUBLE[], new=DECIMAL(38,10)]
                DECIMAL256\terror: alter: [54] incompatible column type change [existing=DOUBLE[], new=DECIMAL(76,20)]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DOUBLE[], new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DOUBLE[], new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=GEOHASH(12c)]
                DECIMAL(5,2)\terror: alter: [52] incompatible column type change [existing=DOUBLE[], new=DECIMAL(5,2)]
                DECIMAL(18,3)\terror: alter: [53] incompatible column type change [existing=DOUBLE[], new=DECIMAL(18,3)]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DOUBLE[], new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                error: [51] v (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                error: alter: [46] incompatible column type change [existing=VARCHAR, new=DOUBLE[]]
                parquet
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\t
                native
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\t
                """);
        rec("DECIMAL8", """
                ## parquet
                k\tv
                d0:min\t-9.9
                d0:max\t9.9
                d0:null\t
                d1:min\t-9.9
                d1:max\t9.9
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t9d
                d0:max\t63
                d0:null\t80
                frame 1 rows=3 native
                d1:min\t9d
                d1:max\t63
                d1:null\t80
                ## parquet-native
                k\tv
                d0:min\t-9.9
                d0:max\t9.9
                d0:null\t
                d1:min\t-9.9
                d1:max\t9.9
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t9d
                d0:max\t63
                d0:null\t80
                frame 1 rows=3 native
                d1:min\t9d
                d1:max\t63
                d1:null\t80
                ## dedup
                k\tv
                dup:min\t-9.9
                shift:min\t9.9
                dup:max\t9.9
                shift:max\t
                dup:null\t
                shift:null\t-9.9
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t9d
                shift:min\t63
                dup:max\t63
                shift:max\t80
                dup:null\t80
                shift:null\t9d
                ## insert
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## frames
                frame 0 rows=3 native
                min\t9d
                max\t63
                null\t80
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-9.9
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t9d
                null\t80
                ## frames-o3
                frame 0 rows=3 native
                min\t9d
                max\t63
                null\t80
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=TIMESTAMP]
                FLOAT\tnull|null|null|-9.9|9.9|null
                DOUBLE\tnull|null|null|-9.9|9.9|null
                STRING\t|||-9.9|9.9|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=IPv4]
                VARCHAR\t|||-9.9|9.9|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(2,1), new=DOUBLE[]]
                DECIMAL8\terror: alter: [52] column 'v' type is already 'DECIMAL(2,1)'
                DECIMAL16\t|||-9.90|9.90|
                DECIMAL32\t|||-10|10|
                DECIMAL64\t|||-9.9000|9.9000|
                DECIMAL128\t|||-9.9000000000|9.9000000000|
                DECIMAL256\t|||-9.90000000000000000000|9.90000000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(2,1), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(2,1), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||-9.90|9.90|
                DECIMAL(18,3)\t|||-9.900|9.900|
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(2,1), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-9.9
                ## single_row-frames
                frame 0 rows=1 native
                min\t9d
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t9.9
                d1:null\t
                d2:min\t-9.9
                d2:max\t9.9
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t63
                d1:null\t80
                frame 3 rows=3 native
                d2:min\t9d
                d2:max\t63
                d2:null\t80
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-9.9
                d0:max\t
                o3d0:max\t9.9
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-9.9
                d1:max\t9.9
                o3d1:max\t9.9
                d1:null\t
                o3d1:null\t
                d2:min\t-9.9
                d2:max\t9.9
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t80
                o3d0:min\t9d
                d0:max\t80
                o3d0:max\t63
                d0:null\t80
                o3d0:null\t80
                frame 1 rows=6 native
                d1:min\t80
                o3d1:min\t9d
                d1:max\t63
                o3d1:max\t63
                d1:null\t80
                o3d1:null\t80
                frame 2 rows=3 native
                d2:min\t9d
                d2:max\t63
                d2:null\t80
                ## latest_by_key
                error: [51] v (DECIMAL(2,1)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                native
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                """);
        rec("DECIMAL16", """
                ## dedup
                k\tv
                dup:min\t-99.99
                shift:min\t99.99
                dup:max\t99.99
                shift:max\t
                dup:null\t
                shift:null\t-99.99
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\tf1d8
                shift:min\t0f27
                dup:max\t0f27
                shift:max\t0080
                dup:null\t0080
                shift:null\tf1d8
                ## insert
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## frames
                frame 0 rows=3 native
                min\tf1d8
                max\t0f27
                null\t0080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-99.99
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\tf1d8
                null\t0080
                ## frames-o3
                frame 0 rows=3 native
                min\tf1d8
                max\t0f27
                null\t0080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t99.99
                d1:null\t
                d2:min\t-99.99
                d2:max\t99.99
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t0f27
                d1:null\t0080
                frame 3 rows=3 native
                d2:min\tf1d8
                d2:max\t0f27
                d2:null\t0080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-99.99
                d0:max\t
                o3d0:max\t99.99
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-99.99
                d1:max\t99.99
                o3d1:max\t99.99
                d1:null\t
                o3d1:null\t
                d2:min\t-99.99
                d2:max\t99.99
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t0080
                o3d0:min\tf1d8
                d0:max\t0080
                o3d0:max\t0f27
                d0:null\t0080
                o3d0:null\t0080
                frame 1 rows=6 native
                d1:min\t0080
                o3d1:min\tf1d8
                d1:max\t0f27
                o3d1:max\t0f27
                d1:null\t0080
                o3d1:null\t0080
                frame 2 rows=3 native
                d2:min\tf1d8
                d2:max\t0f27
                d2:null\t0080
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=TIMESTAMP]
                FLOAT\tnull|null|null|-99.99|99.99|null
                DOUBLE\tnull|null|null|-99.99|99.99|null
                STRING\t|||-99.99|99.99|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=IPv4]
                VARCHAR\t|||-99.99|99.99|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(4,2), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\terror: alter: [52] column 'v' type is already 'DECIMAL(4,2)'
                DECIMAL32\t|||-100|100|
                DECIMAL64\t|||-99.9900|99.9900|
                DECIMAL128\t|||-99.9900000000|99.9900000000|
                DECIMAL256\t|||-99.99000000000000000000|99.99000000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(4,2), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(4,2), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||-99.99|99.99|
                DECIMAL(18,3)\t|||-99.990|99.990|
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(4,2), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t-99.99
                d0:max\t99.99
                d0:null\t
                d1:min\t-99.99
                d1:max\t99.99
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\tf1d8
                d0:max\t0f27
                d0:null\t0080
                frame 1 rows=3 native
                d1:min\tf1d8
                d1:max\t0f27
                d1:null\t0080
                ## parquet-native
                k\tv
                d0:min\t-99.99
                d0:max\t99.99
                d0:null\t
                d1:min\t-99.99
                d1:max\t99.99
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\tf1d8
                d0:max\t0f27
                d0:null\t0080
                frame 1 rows=3 native
                d1:min\tf1d8
                d1:max\t0f27
                d1:null\t0080
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-99.99
                ## single_row-frames
                frame 0 rows=1 native
                min\tf1d8
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                error: [51] v (DECIMAL(4,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                native
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                """);
        rec("DECIMAL32", """
                ## dedup
                k\tv
                dup:min\t-999999999
                shift:min\t999999999
                dup:max\t999999999
                shift:max\t
                dup:null\t
                shift:null\t-999999999
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t013665c4
                shift:min\tffc99a3b
                dup:max\tffc99a3b
                shift:max\t00000080
                dup:null\t00000080
                shift:null\t013665c4
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=TIMESTAMP]
                FLOAT\tnull|null|null|-1.0E9|1.0E9|null
                DOUBLE\tnull|null|null|-9.99999999E8|9.99999999E8|null
                STRING\t|||-999999999|999999999|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=IPv4]
                VARCHAR\t|||-999999999|999999999|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(9,0), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\terror: alter: [52] column 'v' type is already 'DECIMAL(9,0)'
                DECIMAL64\t|||-999999999.0000|999999999.0000|
                DECIMAL128\t|||-999999999.0000000000|999999999.0000000000|
                DECIMAL256\t|||-999999999.00000000000000000000|999999999.00000000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(9,0), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(9,0), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||||
                DECIMAL(18,3)\t|||-999999999.000|999999999.000|
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(9,0), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-999999999
                ## single_row-frames
                frame 0 rows=1 native
                min\t013665c4
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## frames
                frame 0 rows=3 native
                min\t013665c4
                max\tffc99a3b
                null\t00000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-999999999
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t013665c4
                null\t00000080
                ## frames-o3
                frame 0 rows=3 native
                min\t013665c4
                max\tffc99a3b
                null\t00000080
                ## parquet
                k\tv
                d0:min\t-999999999
                d0:max\t999999999
                d0:null\t
                d1:min\t-999999999
                d1:max\t999999999
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t013665c4
                d0:max\tffc99a3b
                d0:null\t00000080
                frame 1 rows=3 native
                d1:min\t013665c4
                d1:max\tffc99a3b
                d1:null\t00000080
                ## parquet-native
                k\tv
                d0:min\t-999999999
                d0:max\t999999999
                d0:null\t
                d1:min\t-999999999
                d1:max\t999999999
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t013665c4
                d0:max\tffc99a3b
                d0:null\t00000080
                frame 1 rows=3 native
                d1:min\t013665c4
                d1:max\tffc99a3b
                d1:null\t00000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t999999999
                d1:null\t
                d2:min\t-999999999
                d2:max\t999999999
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffc99a3b
                d1:null\t00000080
                frame 3 rows=3 native
                d2:min\t013665c4
                d2:max\tffc99a3b
                d2:null\t00000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-999999999
                d0:max\t
                o3d0:max\t999999999
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-999999999
                d1:max\t999999999
                o3d1:max\t999999999
                d1:null\t
                o3d1:null\t
                d2:min\t-999999999
                d2:max\t999999999
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t00000080
                o3d0:min\t013665c4
                d0:max\t00000080
                o3d0:max\tffc99a3b
                d0:null\t00000080
                o3d0:null\t00000080
                frame 1 rows=6 native
                d1:min\t00000080
                o3d1:min\t013665c4
                d1:max\tffc99a3b
                o3d1:max\tffc99a3b
                d1:null\t00000080
                o3d1:null\t00000080
                frame 2 rows=3 native
                d2:min\t013665c4
                d2:max\tffc99a3b
                d2:null\t00000080
                ## latest_by_key
                error: [51] v (DECIMAL(9,0)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                native
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                """);
        rec("DECIMAL64", """
                ## parquet
                k\tv
                d0:min\t-999999999999.9999
                d0:max\t999999999999.9999
                d0:null\t
                d1:min\t-999999999999.9999
                d1:max\t999999999999.9999
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t01003f900d79dcff
                d0:max\tffffc06ff2862300
                d0:null\t0000000000000080
                frame 1 rows=3 native
                d1:min\t01003f900d79dcff
                d1:max\tffffc06ff2862300
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t-999999999999.9999
                d0:max\t999999999999.9999
                d0:null\t
                d1:min\t-999999999999.9999
                d1:max\t999999999999.9999
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t01003f900d79dcff
                d0:max\tffffc06ff2862300
                d0:null\t0000000000000080
                frame 1 rows=3 native
                d1:min\t01003f900d79dcff
                d1:max\tffffc06ff2862300
                d1:null\t0000000000000080
                ## insert
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## frames
                frame 0 rows=3 native
                min\t01003f900d79dcff
                max\tffffc06ff2862300
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-999999999999.9999
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t01003f900d79dcff
                null\t0000000000000080
                ## frames-o3
                frame 0 rows=3 native
                min\t01003f900d79dcff
                max\tffffc06ff2862300
                null\t0000000000000080
                ## dedup
                k\tv
                dup:min\t-999999999999.9999
                shift:min\t999999999999.9999
                dup:max\t999999999999.9999
                shift:max\t
                dup:null\t
                shift:null\t-999999999999.9999
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t01003f900d79dcff
                shift:min\tffffc06ff2862300
                dup:max\tffffc06ff2862300
                shift:max\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t01003f900d79dcff
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t999999999999.9999
                d1:null\t
                d2:min\t-999999999999.9999
                d2:max\t999999999999.9999
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffffc06ff2862300
                d1:null\t0000000000000080
                frame 3 rows=3 native
                d2:min\t01003f900d79dcff
                d2:max\tffffc06ff2862300
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-999999999999.9999
                d0:max\t
                o3d0:max\t999999999999.9999
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-999999999999.9999
                d1:max\t999999999999.9999
                o3d1:max\t999999999999.9999
                d1:null\t
                o3d1:null\t
                d2:min\t-999999999999.9999
                d2:max\t999999999999.9999
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t0000000000000080
                o3d0:min\t01003f900d79dcff
                d0:max\t0000000000000080
                o3d0:max\tffffc06ff2862300
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=6 native
                d1:min\t0000000000000080
                o3d1:min\t01003f900d79dcff
                d1:max\tffffc06ff2862300
                o3d1:max\tffffc06ff2862300
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=3 native
                d2:min\t01003f900d79dcff
                d2:max\tffffc06ff2862300
                d2:null\t0000000000000080
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-999999999999.9999
                ## single_row-frames
                frame 0 rows=1 native
                min\t01003f900d79dcff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=TIMESTAMP]
                FLOAT\tnull|null|null|-1.0E12|1.0E12|null
                DOUBLE\tnull|null|null|-9.999999999999999E11|9.999999999999999E11|null
                STRING\t|||-999999999999.9999|999999999999.9999|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=IPv4]
                VARCHAR\t|||-999999999999.9999|999999999999.9999|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(16,4), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\t|||||
                DECIMAL64\terror: alter: [53] column 'v' type is already 'DECIMAL(16,4)'
                DECIMAL128\t|||-999999999999.9999000000|999999999999.9999000000|
                DECIMAL256\t|||-999999999999.99990000000000000000|999999999999.99990000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(16,4), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(16,4), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||||
                DECIMAL(18,3)\t|||-1000000000000.000|1000000000000.000|
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(16,4), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                error: [51] v (DECIMAL(16,4)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                native
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                """);
        rec("DECIMAL128", """
                ## dedup
                k\tv
                dup:min\t-9999999999999999999999999999.9999999999
                shift:min\t9999999999999999999999999999.9999999999
                dup:max\t9999999999999999999999999999.9999999999
                shift:max\t
                dup:null\t
                shift:null\t-9999999999999999999999999999.9999999999
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t853b79a557b3c4b401000000c0dd75f6
                shift:min\t7ac4865aa84c3b4bffffffff3f228a09
                dup:max\t7ac4865aa84c3b4bffffffff3f228a09
                shift:max\t00000000000000800000000000000000
                dup:null\t00000000000000800000000000000000
                shift:null\t853b79a557b3c4b401000000c0dd75f6
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-9999999999999999999999999999.9999999999
                ## single_row-frames
                frame 0 rows=1 native
                min\t853b79a557b3c4b401000000c0dd75f6
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=TIMESTAMP]
                FLOAT\tnull|null|null|-1.0E28|1.0E28|null
                DOUBLE\tnull|null|null|-1.0E28|1.0E28|null
                STRING\t|||-9999999999999999999999999999.9999999999|9999999999999999999999999999.9999999999|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=IPv4]
                VARCHAR\t|||-9999999999999999999999999999.9999999999|9999999999999999999999999999.9999999999|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(38,10), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\t|||||
                DECIMAL64\t|||||
                DECIMAL128\terror: alter: [54] column 'v' type is already 'DECIMAL(38,10)'
                DECIMAL256\t|||-9999999999999999999999999999.99999999990000000000|9999999999999999999999999999.99999999990000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(38,10), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(38,10), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||||
                DECIMAL(18,3)\t|||||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(38,10), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## insert
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## frames
                frame 0 rows=3 native
                min\t853b79a557b3c4b401000000c0dd75f6
                max\t7ac4865aa84c3b4bffffffff3f228a09
                null\t00000000000000800000000000000000
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-9999999999999999999999999999.9999999999
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t853b79a557b3c4b401000000c0dd75f6
                null\t00000000000000800000000000000000
                ## frames-o3
                frame 0 rows=3 native
                min\t853b79a557b3c4b401000000c0dd75f6
                max\t7ac4865aa84c3b4bffffffff3f228a09
                null\t00000000000000800000000000000000
                ## parquet
                k\tv
                d0:min\t-9999999999999999999999999999.9999999999
                d0:max\t9999999999999999999999999999.9999999999
                d0:null\t
                d1:min\t-9999999999999999999999999999.9999999999
                d1:max\t9999999999999999999999999999.9999999999
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t853b79a557b3c4b401000000c0dd75f6
                d0:max\t7ac4865aa84c3b4bffffffff3f228a09
                d0:null\t00000000000000800000000000000000
                frame 1 rows=3 native
                d1:min\t853b79a557b3c4b401000000c0dd75f6
                d1:max\t7ac4865aa84c3b4bffffffff3f228a09
                d1:null\t00000000000000800000000000000000
                ## parquet-native
                k\tv
                d0:min\t-9999999999999999999999999999.9999999999
                d0:max\t9999999999999999999999999999.9999999999
                d0:null\t
                d1:min\t-9999999999999999999999999999.9999999999
                d1:max\t9999999999999999999999999999.9999999999
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t853b79a557b3c4b401000000c0dd75f6
                d0:max\t7ac4865aa84c3b4bffffffff3f228a09
                d0:null\t00000000000000800000000000000000
                frame 1 rows=3 native
                d1:min\t853b79a557b3c4b401000000c0dd75f6
                d1:max\t7ac4865aa84c3b4bffffffff3f228a09
                d1:null\t00000000000000800000000000000000
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t9999999999999999999999999999.9999999999
                d1:null\t
                d2:min\t-9999999999999999999999999999.9999999999
                d2:max\t9999999999999999999999999999.9999999999
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t7ac4865aa84c3b4bffffffff3f228a09
                d1:null\t00000000000000800000000000000000
                frame 3 rows=3 native
                d2:min\t853b79a557b3c4b401000000c0dd75f6
                d2:max\t7ac4865aa84c3b4bffffffff3f228a09
                d2:null\t00000000000000800000000000000000
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-9999999999999999999999999999.9999999999
                d0:max\t
                o3d0:max\t9999999999999999999999999999.9999999999
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-9999999999999999999999999999.9999999999
                d1:max\t9999999999999999999999999999.9999999999
                o3d1:max\t9999999999999999999999999999.9999999999
                d1:null\t
                o3d1:null\t
                d2:min\t-9999999999999999999999999999.9999999999
                d2:max\t9999999999999999999999999999.9999999999
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t00000000000000800000000000000000
                o3d0:min\t853b79a557b3c4b401000000c0dd75f6
                d0:max\t00000000000000800000000000000000
                o3d0:max\t7ac4865aa84c3b4bffffffff3f228a09
                d0:null\t00000000000000800000000000000000
                o3d0:null\t00000000000000800000000000000000
                frame 1 rows=6 native
                d1:min\t00000000000000800000000000000000
                o3d1:min\t853b79a557b3c4b401000000c0dd75f6
                d1:max\t7ac4865aa84c3b4bffffffff3f228a09
                o3d1:max\t7ac4865aa84c3b4bffffffff3f228a09
                d1:null\t00000000000000800000000000000000
                o3d1:null\t00000000000000800000000000000000
                frame 2 rows=3 native
                d2:min\t853b79a557b3c4b401000000c0dd75f6
                d2:max\t7ac4865aa84c3b4bffffffff3f228a09
                d2:null\t00000000000000800000000000000000
                ## latest_by_key
                error: [51] v (DECIMAL(38,10)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                native
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                """);
        rec("DECIMAL256", """
                ## insert
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## frames
                frame 0 rows=3 native
                min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                null\t0000000000000080000000000000000000000000000000000000000000000000
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                null\t0000000000000080000000000000000000000000000000000000000000000000
                ## frames-o3
                frame 0 rows=3 native
                min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                null\t0000000000000080000000000000000000000000000000000000000000000000
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:null\t
                d2:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d2:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d1:null\t0000000000000080000000000000000000000000000000000000000000000000
                frame 3 rows=3 native
                d2:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d2:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d2:null\t0000000000000080000000000000000000000000000000000000000000000000
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:max\t
                o3d0:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                o3d1:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:null\t
                o3d1:null\t
                d2:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d2:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t0000000000000080000000000000000000000000000000000000000000000000
                o3d0:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d0:max\t0000000000000080000000000000000000000000000000000000000000000000
                o3d0:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d0:null\t0000000000000080000000000000000000000000000000000000000000000000
                o3d0:null\t0000000000000080000000000000000000000000000000000000000000000000
                frame 1 rows=6 native
                d1:min\t0000000000000080000000000000000000000000000000000000000000000000
                o3d1:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d1:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                o3d1:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d1:null\t0000000000000080000000000000000000000000000000000000000000000000
                o3d1:null\t0000000000000080000000000000000000000000000000000000000000000000
                frame 2 rows=3 native
                d2:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d2:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d2:null\t0000000000000080000000000000000000000000000000000000000000000000
                ## dedup
                k\tv
                dup:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                shift:min\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                dup:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                shift:max\t
                dup:null\t
                shift:null\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                shift:min\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                dup:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                shift:max\t0000000000000080000000000000000000000000000000000000000000000000
                dup:null\t0000000000000080000000000000000000000000000000000000000000000000
                shift:null\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=TIMESTAMP]
                FLOAT\tnull|null|null|null|null|null
                DOUBLE\tnull|null|null|-1.0E56|1.0E56|null
                STRING\t|||-99999999999999999999999999999999999999999999999999999999.99999999999999999999|99999999999999999999999999999999999999999999999999999999.99999999999999999999|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=IPv4]
                VARCHAR\t|||-99999999999999999999999999999999999999999999999999999999.99999999999999999999|99999999999999999999999999999999999999999999999999999999.99999999999999999999|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(76,20), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\t|||||
                DECIMAL64\t|||||
                DECIMAL128\t|||||
                DECIMAL256\terror: alter: [54] column 'v' type is already 'DECIMAL(76,20)'
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(76,20), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(76,20), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||||
                DECIMAL(18,3)\t|||||
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(76,20), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:null\t
                d1:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d0:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d0:null\t0000000000000080000000000000000000000000000000000000000000000000
                frame 1 rows=3 native
                d1:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d1:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d1:null\t0000000000000080000000000000000000000000000000000000000000000000
                ## parquet-native
                k\tv
                d0:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d0:null\t
                d1:min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d0:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d0:null\t0000000000000080000000000000000000000000000000000000000000000000
                frame 1 rows=3 native
                d1:min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                d1:max\tb5159911a7cc1b16792965e8abb46407ff0f9571f1a57577ffffffffffffffff
                d1:null\t0000000000000080000000000000000000000000000000000000000000000000
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                ## single_row-frames
                frame 0 rows=1 native
                min\t4aea66ee5833e4e986d69a17544b9bf800f06a8e0e5a8a880100000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## latest_by_key
                error: [51] v (DECIMAL(76,20)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                native
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                """);
        rec("INTERVAL", """
                ## empty_table
                error: create: [37] non-persisted type: INTERVAL
                ## empty_table-frames
                error: create: [37] non-persisted type: INTERVAL
                ## single_row
                error: create: [37] non-persisted type: INTERVAL
                ## single_row-frames
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition-frames
                error: create: [37] non-persisted type: INTERVAL
                ## alter
                target\td0:max|d0:null|d1:max|d1:null
                *\terror: add column: [34] non-persisted type: INTERVAL error: insert d1:: [25] Invalid column: v
                ## dedup
                error: create: [35] non-persisted type: INTERVAL
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## tops
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## insert
                error: create: [35] non-persisted type: INTERVAL
                ## frames
                error: create: [35] non-persisted type: INTERVAL
                ## insert-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3
                error: create: [35] non-persisted type: INTERVAL
                ## parquet
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-frames
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native-frames
                error: create: [34] non-persisted type: INTERVAL
                ## latest_by_key
                error: create: [34] non-persisted type: INTERVAL
                ## parquet_convert
                error: create: [35] non-persisted type: INTERVAL
                """);
        rec("VARCHAR_SLICE", """
                ## dedup
                error: create: [35] unsupported column type: VARCHAR_SLICE
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## tops
                error: add column: [34] unsupported column type: VARCHAR_SLICE
                ## tops-frames
                error: add column: [34] unsupported column type: VARCHAR_SLICE
                ## tops-o3
                error: add column: [34] unsupported column type: VARCHAR_SLICE
                ## tops-o3-frames
                error: add column: [34] unsupported column type: VARCHAR_SLICE
                ## empty_table
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## empty_table-frames
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## single_row
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## single_row-frames
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## empty_partition
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## empty_partition-frames
                error: create: [37] unsupported column type: VARCHAR_SLICE
                ## parquet
                error: create: [34] unsupported column type: VARCHAR_SLICE
                ## parquet-frames
                error: create: [34] unsupported column type: VARCHAR_SLICE
                ## parquet-native
                error: create: [34] unsupported column type: VARCHAR_SLICE
                ## parquet-native-frames
                error: create: [34] unsupported column type: VARCHAR_SLICE
                ## alter
                target\td0:empty|d0:min|d0:max|d0:escape|d0:null|d1:empty|d1:min|d1:max|d1:escape|d1:null
                *\terror: add column: [34] unsupported column type: VARCHAR_SLICE error: insert d1:: [25] Invalid column: v
                ## insert
                error: create: [35] unsupported column type: VARCHAR_SLICE
                ## frames
                error: create: [35] unsupported column type: VARCHAR_SLICE
                ## insert-o3-none
                error: create: [35] unsupported column type: VARCHAR_SLICE
                ## frames-o3-none
                error: create: [35] unsupported column type: VARCHAR_SLICE
                ## frames-o3
                error: create: [35] unsupported column type: VARCHAR_SLICE
                ## latest_by_key
                error: create: [34] unsupported column type: VARCHAR_SLICE
                ## parquet_convert
                error: create: [35] unsupported column type: VARCHAR_SLICE
                """);
        rec("TIMESTAMP_NS", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                ## single_row-frames
                frame 0 rows=1 native
                min\t0100000000000080
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ## frames
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                sentinel\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0100000000000080
                sentinel\t0000000000000080
                ## frames-o3
                frame 0 rows=4 native
                min\t0100000000000080
                max\tffffffffffffff7f
                sentinel\t0000000000000080
                null\t0000000000000080
                ## alter
                target\td0:min|d0:max|d0:sentinel|d0:null|d1:min|d1:max|d1:sentinel|d1:null
                BOOLEAN\tfalse|false|false|false|true|true|false|false
                BYTE\t0|0|0|0|1|-1|0|0
                SHORT\t0|0|0|0|1|-1|0|0
                CHAR\tincompatible [41]
                INT\tnull|null|null|null|1|-1|null|null
                LONG\tnull|null|null|null|-9223372036854775807|9223372036854775807|null|null
                DATE\t||||1677-09-21T00:12:43.146Z|2262-04-11T23:47:16.854Z||
                TIMESTAMP\t||||1677-09-21T00:12:43.145225Z|2262-04-11T23:47:16.854775Z||
                FLOAT\tnull|null|null|null|-9.223372E18|9.223372E18|null|null
                DOUBLE\tnull|null|null|null|-9.223372036854776E18|9.223372036854776E18|null|null
                STRING\t||||1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                SYMBOL\t||||1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\t||||1677-01-01T00:12:43.145224193Z|2262-04-11T23:47:16.854775807Z||
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=TIMESTAMP_NS, new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] column 'v' type is already 'TIMESTAMP_NS'
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=TIMESTAMP_NS, new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## parquet
                k\tv
                d0:min\t1677-01-01T00:12:43.145224193Z
                d0:max\t2262-04-11T23:47:16.854775807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t1677-01-01T00:12:43.145224193Z
                d1:max\t2262-04-11T23:47:16.854775807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-frames
                frame 0 rows=4 parquet
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t1677-01-01T00:12:43.145224193Z
                d0:max\t2262-04-11T23:47:16.854775807Z
                d0:sentinel\t
                d0:null\t
                d1:min\t1677-01-01T00:12:43.145224193Z
                d1:max\t2262-04-11T23:47:16.854775807Z
                d1:sentinel\t
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=4 native
                d0:min\t0100000000000080
                d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                frame 1 rows=4 native
                d1:min\t0100000000000080
                d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                ## dedup
                k\tv
                dup:min\t1677-01-01T00:12:43.145224193Z
                shift:min\t2262-04-11T23:47:16.854775807Z
                dup:max\t2262-04-11T23:47:16.854775807Z
                shift:max\t
                shift:sentinel\t
                dup:null\t
                shift:null\t1677-01-01T00:12:43.145224193Z
                ## dedup-frames
                frame 0 rows=7 native
                dup:min\t0100000000000080
                shift:min\tffffffffffffff7f
                dup:max\tffffffffffffff7f
                shift:max\t0000000000000080
                shift:sentinel\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t0100000000000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:sentinel\t
                d0:null\t
                d1:min\t
                d1:max\t
                d1:sentinel\t
                d1:null\t
                d2:min\t1677-01-01T00:12:43.145224193Z
                d2:max\t2262-04-11T23:47:16.854775807Z
                d2:sentinel\t
                d2:null\t
                ## tops-frames
                frame 0 rows=4 native
                d0:min\ttop
                d0:max\ttop
                d0:sentinel\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=2 native
                d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                frame 3 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t1677-01-01T00:12:43.145224193Z
                d0:max\t
                o3d0:max\t2262-04-11T23:47:16.854775807Z
                d0:sentinel\t
                o3d0:sentinel\t
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t1677-01-01T00:12:43.145224193Z
                d1:max\t
                o3d1:max\t2262-04-11T23:47:16.854775807Z
                d1:sentinel\t
                o3d1:sentinel\t
                d1:null\t
                o3d1:null\t
                d2:min\t1677-01-01T00:12:43.145224193Z
                d2:max\t2262-04-11T23:47:16.854775807Z
                d2:sentinel\t
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=8 native
                d0:min\t0000000000000080
                o3d0:min\t0100000000000080
                d0:max\t0000000000000080
                o3d0:max\tffffffffffffff7f
                d0:sentinel\t0000000000000080
                o3d0:sentinel\t0000000000000080
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=8 native
                d1:min\t0000000000000080
                o3d1:min\t0100000000000080
                d1:max\t0000000000000080
                o3d1:max\tffffffffffffff7f
                d1:sentinel\t0000000000000080
                o3d1:sentinel\t0000000000000080
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=4 native
                d2:min\t0100000000000080
                d2:max\tffffffffffffff7f
                d2:sentinel\t0000000000000080
                d2:null\t0000000000000080
                ## latest_by_key
                k\tv
                b:max\t2262-04-11T23:47:16.854775807Z
                b:min\t1677-01-01T00:12:43.145224193Z
                b:null\t
                ## parquet_convert
                parquet
                k\tv
                min\t1677-01-01T00:25:26.290448385Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                native
                k\tv
                min\t1677-01-01T00:25:26.290448385Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                """);
        rec("GEOHASH(1c)", """
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\tz
                d1:null\t
                d2:min\t0
                d2:max\tz
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t1f
                d1:null\tff
                frame 3 rows=3 native
                d2:min\t00
                d2:max\t1f
                d2:null\tff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t0
                d0:max\t
                o3d0:max\tz
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t0
                d1:max\tz
                o3d1:max\tz
                d1:null\t
                o3d1:null\t
                d2:min\t0
                d2:max\tz
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tff
                o3d0:min\t00
                d0:max\tff
                o3d0:max\t1f
                d0:null\tff
                o3d0:null\tff
                frame 1 rows=6 native
                d1:min\tff
                o3d1:min\t00
                d1:max\t1f
                o3d1:max\t1f
                d1:null\tff
                o3d1:null\tff
                frame 2 rows=3 native
                d2:min\t00
                d2:max\t1f
                d2:null\tff
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t0
                ## single_row-frames
                frame 0 rows=1 native
                min\t00
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:min\t0
                shift:min\tz
                dup:max\tz
                shift:max\t
                dup:null\t
                shift:null\t0
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t00
                shift:min\t1f
                dup:max\t1f
                shift:max\tff
                dup:null\tff
                shift:null\t00
                ## insert
                k\tv
                min\t0
                max\tz
                null\t
                ## frames
                frame 0 rows=3 native
                min\t00
                max\t1f
                null\tff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t0
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00
                null\tff
                ## frames-o3
                frame 0 rows=3 native
                min\t00
                max\t1f
                null\tff
                ## parquet
                k\tv
                d0:min\t0
                d0:max\tz
                d0:null\t
                d1:min\t0
                d1:max\tz
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t00
                d0:max\t1f
                d0:null\tff
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t1f
                d1:null\tff
                ## parquet-native
                k\tv
                d0:min\t0
                d0:max\tz
                d0:null\t
                d1:min\t0
                d1:max\tz
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t00
                d0:max\t1f
                d0:null\tff
                frame 1 rows=3 native
                d1:min\t00
                d1:max\t1f
                d1:null\tff
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(1c), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\terror: alter: [51] column 'v' type is already 'GEOHASH(1c)'
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(1c), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\tz
                b:min\t0
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(1c)]
                parquet
                k\tv
                min\t0
                max\tz
                null\t
                native
                k\tv
                min\t0
                max\tz
                null\t
                """);
        rec("GEOHASH(8b)", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t00000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## parquet
                k\tv
                d0:min\t00000000
                d0:max\t11111111
                d0:null\t
                d1:min\t00000000
                d1:max\t11111111
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t0000
                d0:max\tff00
                d0:null\tffff
                frame 1 rows=3 native
                d1:min\t0000
                d1:max\tff00
                d1:null\tffff
                ## parquet-native
                k\tv
                d0:min\t00000000
                d0:max\t11111111
                d0:null\t
                d1:min\t00000000
                d1:max\t11111111
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t0000
                d0:max\tff00
                d0:null\tffff
                frame 1 rows=3 native
                d1:min\t0000
                d1:max\tff00
                d1:null\tffff
                ## dedup
                k\tv
                dup:min\t00000000
                shift:min\t11111111
                dup:max\t11111111
                shift:max\t
                dup:null\t
                shift:null\t00000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t0000
                shift:min\tff00
                dup:max\tff00
                shift:max\tffff
                dup:null\tffff
                shift:null\t0000
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(8b), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\terror: alter: [51] column 'v' type is already 'GEOHASH(8b)'
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(8b), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t11111111
                d1:null\t
                d2:min\t00000000
                d2:max\t11111111
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tff00
                d1:null\tffff
                frame 3 rows=3 native
                d2:min\t0000
                d2:max\tff00
                d2:null\tffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t00000000
                d0:max\t
                o3d0:max\t11111111
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t00000000
                d1:max\t11111111
                o3d1:max\t11111111
                d1:null\t
                o3d1:null\t
                d2:min\t00000000
                d2:max\t11111111
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffff
                o3d0:min\t0000
                d0:max\tffff
                o3d0:max\tff00
                d0:null\tffff
                o3d0:null\tffff
                frame 1 rows=6 native
                d1:min\tffff
                o3d1:min\t0000
                d1:max\tff00
                o3d1:max\tff00
                d1:null\tffff
                o3d1:null\tffff
                frame 2 rows=3 native
                d2:min\t0000
                d2:max\tff00
                d2:null\tffff
                ## insert
                k\tv
                min\t00000000
                max\t11111111
                null\t
                ## frames
                frame 0 rows=3 native
                min\t0000
                max\tff00
                null\tffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t00000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000
                null\tffff
                ## frames-o3
                frame 0 rows=3 native
                min\t0000
                max\tff00
                null\tffff
                ## latest_by_key
                k\tv
                b:max\t11111111
                b:min\t00000000
                b:null\t
                ## parquet_convert
                error: alter: [49] incompatible column type change [existing=VARCHAR, new=GEOHASH(8b)]
                parquet
                k\tv
                min\t00000000
                max\t11111111
                null\t
                native
                k\tv
                min\t00000000
                max\t11111111
                null\t
                """);
        rec("GEOHASH(31b)", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t0000000000000000000000000000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t00000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t1111111111111111111111111111111
                d1:null\t
                d2:min\t0000000000000000000000000000000
                d2:max\t1111111111111111111111111111111
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffffff7f
                d1:null\tffffffff
                frame 3 rows=3 native
                d2:min\t00000000
                d2:max\tffffff7f
                d2:null\tffffffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t0000000000000000000000000000000
                d0:max\t
                o3d0:max\t1111111111111111111111111111111
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t0000000000000000000000000000000
                d1:max\t1111111111111111111111111111111
                o3d1:max\t1111111111111111111111111111111
                d1:null\t
                o3d1:null\t
                d2:min\t0000000000000000000000000000000
                d2:max\t1111111111111111111111111111111
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffffffff
                o3d0:min\t00000000
                d0:max\tffffffff
                o3d0:max\tffffff7f
                d0:null\tffffffff
                o3d0:null\tffffffff
                frame 1 rows=6 native
                d1:min\tffffffff
                o3d1:min\t00000000
                d1:max\tffffff7f
                o3d1:max\tffffff7f
                d1:null\tffffffff
                o3d1:null\tffffffff
                frame 2 rows=3 native
                d2:min\t00000000
                d2:max\tffffff7f
                d2:null\tffffffff
                ## dedup
                k\tv
                dup:min\t0000000000000000000000000000000
                shift:min\t1111111111111111111111111111111
                dup:max\t1111111111111111111111111111111
                shift:max\t
                dup:null\t
                shift:null\t0000000000000000000000000000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t00000000
                shift:min\tffffff7f
                dup:max\tffffff7f
                shift:max\tffffffff
                dup:null\tffffffff
                shift:null\t00000000
                ## insert
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                ## frames
                frame 0 rows=3 native
                min\t00000000
                max\tffffff7f
                null\tffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t0000000000000000000000000000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t00000000
                null\tffffffff
                ## frames-o3
                frame 0 rows=3 native
                min\t00000000
                max\tffffff7f
                null\tffffffff
                ## parquet
                k\tv
                d0:min\t0000000000000000000000000000000
                d0:max\t1111111111111111111111111111111
                d0:null\t
                d1:min\t0000000000000000000000000000000
                d1:max\t1111111111111111111111111111111
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t00000000
                d0:max\tffffff7f
                d0:null\tffffffff
                frame 1 rows=3 native
                d1:min\t00000000
                d1:max\tffffff7f
                d1:null\tffffffff
                ## parquet-native
                k\tv
                d0:min\t0000000000000000000000000000000
                d0:max\t1111111111111111111111111111111
                d0:null\t
                d1:min\t0000000000000000000000000000000
                d1:max\t1111111111111111111111111111111
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t00000000
                d0:max\tffffff7f
                d0:null\tffffffff
                frame 1 rows=3 native
                d1:min\t00000000
                d1:max\tffffff7f
                d1:null\tffffffff
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(31b), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\terror: alter: [52] column 'v' type is already 'GEOHASH(31b)'
                GEOHASH(12c)\tincompatible [52]
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(31b), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## latest_by_key
                k\tv
                b:max\t1111111111111111111111111111111
                b:min\t0000000000000000000000000000000
                b:null\t
                ## parquet_convert
                error: alter: [50] incompatible column type change [existing=VARCHAR, new=GEOHASH(31b)]
                parquet
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                native
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                """);
        rec("GEOHASH(12c)", """
                ## dedup
                k\tv
                dup:min\t000000000000
                shift:min\tzzzzzzzzzzzz
                dup:max\tzzzzzzzzzzzz
                shift:max\t
                dup:null\t
                shift:null\t000000000000
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t0000000000000000
                shift:min\tffffffffffffff0f
                dup:max\tffffffffffffff0f
                shift:max\tffffffffffffffff
                dup:null\tffffffffffffffff
                shift:null\t0000000000000000
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\tincompatible [41]
                BYTE\tincompatible [41]
                SHORT\tincompatible [41]
                CHAR\tincompatible [41]
                INT\tincompatible [41]
                LONG\tincompatible [41]
                DATE\tincompatible [41]
                TIMESTAMP\tincompatible [41]
                FLOAT\tincompatible [41]
                DOUBLE\tincompatible [41]
                STRING\tincompatible [41]
                SYMBOL\tincompatible [41]
                LONG256\tincompatible [41]
                GEOBYTE\tincompatible [51]
                GEOSHORT\tincompatible [51]
                GEOINT\tincompatible [51]
                GEOLONG\tincompatible [51]
                BINARY\tincompatible [41]
                UUID\tincompatible [41]
                LONG128\tincompatible [41]
                IPv4\tincompatible [41]
                VARCHAR\tincompatible [41]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=GEOHASH(12c), new=DOUBLE[]]
                DECIMAL8\tincompatible [52]
                DECIMAL16\tincompatible [52]
                DECIMAL32\tincompatible [52]
                DECIMAL64\tincompatible [53]
                DECIMAL128\tincompatible [54]
                DECIMAL256\tincompatible [54]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\tincompatible [41]
                GEOHASH(1c)\tincompatible [51]
                GEOHASH(8b)\tincompatible [51]
                GEOHASH(31b)\tincompatible [52]
                GEOHASH(12c)\terror: alter: [52] column 'v' type is already 'GEOHASH(12c)'
                DECIMAL(5,2)\tincompatible [52]
                DECIMAL(18,3)\tincompatible [53]
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=GEOHASH(12c), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t000000000000
                ## single_row-frames
                frame 0 rows=1 native
                min\t0000000000000000
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## insert
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## frames
                frame 0 rows=3 native
                min\t0000000000000000
                max\tffffffffffffff0f
                null\tffffffffffffffff
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t000000000000
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t0000000000000000
                null\tffffffffffffffff
                ## frames-o3
                frame 0 rows=3 native
                min\t0000000000000000
                max\tffffffffffffff0f
                null\tffffffffffffffff
                ## parquet
                k\tv
                d0:min\t000000000000
                d0:max\tzzzzzzzzzzzz
                d0:null\t
                d1:min\t000000000000
                d1:max\tzzzzzzzzzzzz
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t0000000000000000
                d0:max\tffffffffffffff0f
                d0:null\tffffffffffffffff
                frame 1 rows=3 native
                d1:min\t0000000000000000
                d1:max\tffffffffffffff0f
                d1:null\tffffffffffffffff
                ## parquet-native
                k\tv
                d0:min\t000000000000
                d0:max\tzzzzzzzzzzzz
                d0:null\t
                d1:min\t000000000000
                d1:max\tzzzzzzzzzzzz
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t0000000000000000
                d0:max\tffffffffffffff0f
                d0:null\tffffffffffffffff
                frame 1 rows=3 native
                d1:min\t0000000000000000
                d1:max\tffffffffffffff0f
                d1:null\tffffffffffffffff
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\tzzzzzzzzzzzz
                d1:null\t
                d2:min\t000000000000
                d2:max\tzzzzzzzzzzzz
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffffffffffffff0f
                d1:null\tffffffffffffffff
                frame 3 rows=3 native
                d2:min\t0000000000000000
                d2:max\tffffffffffffff0f
                d2:null\tffffffffffffffff
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t000000000000
                d0:max\t
                o3d0:max\tzzzzzzzzzzzz
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t000000000000
                d1:max\tzzzzzzzzzzzz
                o3d1:max\tzzzzzzzzzzzz
                d1:null\t
                o3d1:null\t
                d2:min\t000000000000
                d2:max\tzzzzzzzzzzzz
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\tffffffffffffffff
                o3d0:min\t0000000000000000
                d0:max\tffffffffffffffff
                o3d0:max\tffffffffffffff0f
                d0:null\tffffffffffffffff
                o3d0:null\tffffffffffffffff
                frame 1 rows=6 native
                d1:min\tffffffffffffffff
                o3d1:min\t0000000000000000
                d1:max\tffffffffffffff0f
                o3d1:max\tffffffffffffff0f
                d1:null\tffffffffffffffff
                o3d1:null\tffffffffffffffff
                frame 2 rows=3 native
                d2:min\t0000000000000000
                d2:max\tffffffffffffff0f
                d2:null\tffffffffffffffff
                ## latest_by_key
                k\tv
                b:max\tzzzzzzzzzzzz
                b:min\t000000000000
                b:null\t
                ## parquet_convert
                error: alter: [50] incompatible column type change [existing=VARCHAR, new=GEOHASH(12c)]
                parquet
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                native
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                """);
        rec("DECIMAL(5,2)", """
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-999.99
                ## single_row-frames
                frame 0 rows=1 native
                min\t6179feff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=TIMESTAMP]
                FLOAT\tnull|null|null|-999.99|999.99|null
                DOUBLE\tnull|null|null|-999.99|999.99|null
                STRING\t|||-999.99|999.99|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=IPv4]
                VARCHAR\t|||-999.99|999.99|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(5,2), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\t|||-1000|1000|
                DECIMAL64\t|||-999.9900|999.9900|
                DECIMAL128\t|||-999.9900000000|999.9900000000|
                DECIMAL256\t|||-999.99000000000000000000|999.99000000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(5,2), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(5,2), new=GEOHASH(12c)]
                DECIMAL(5,2)\terror: alter: [52] column 'v' type is already 'DECIMAL(5,2)'
                DECIMAL(18,3)\t|||-999.990|999.990|
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(5,2), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## insert
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## frames
                frame 0 rows=3 native
                min\t6179feff
                max\t9f860100
                null\t00000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-999.99
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t6179feff
                null\t00000080
                ## frames-o3
                frame 0 rows=3 native
                min\t6179feff
                max\t9f860100
                null\t00000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t999.99
                d1:null\t
                d2:min\t-999.99
                d2:max\t999.99
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\t9f860100
                d1:null\t00000080
                frame 3 rows=3 native
                d2:min\t6179feff
                d2:max\t9f860100
                d2:null\t00000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-999.99
                d0:max\t
                o3d0:max\t999.99
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-999.99
                d1:max\t999.99
                o3d1:max\t999.99
                d1:null\t
                o3d1:null\t
                d2:min\t-999.99
                d2:max\t999.99
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t00000080
                o3d0:min\t6179feff
                d0:max\t00000080
                o3d0:max\t9f860100
                d0:null\t00000080
                o3d0:null\t00000080
                frame 1 rows=6 native
                d1:min\t00000080
                o3d1:min\t6179feff
                d1:max\t9f860100
                o3d1:max\t9f860100
                d1:null\t00000080
                o3d1:null\t00000080
                frame 2 rows=3 native
                d2:min\t6179feff
                d2:max\t9f860100
                d2:null\t00000080
                ## dedup
                k\tv
                dup:min\t-999.99
                shift:min\t999.99
                dup:max\t999.99
                shift:max\t
                dup:null\t
                shift:null\t-999.99
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t6179feff
                shift:min\t9f860100
                dup:max\t9f860100
                shift:max\t00000080
                dup:null\t00000080
                shift:null\t6179feff
                ## parquet
                k\tv
                d0:min\t-999.99
                d0:max\t999.99
                d0:null\t
                d1:min\t-999.99
                d1:max\t999.99
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t6179feff
                d0:max\t9f860100
                d0:null\t00000080
                frame 1 rows=3 native
                d1:min\t6179feff
                d1:max\t9f860100
                d1:null\t00000080
                ## parquet-native
                k\tv
                d0:min\t-999.99
                d0:max\t999.99
                d0:null\t
                d1:min\t-999.99
                d1:max\t999.99
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t6179feff
                d0:max\t9f860100
                d0:null\t00000080
                frame 1 rows=3 native
                d1:min\t6179feff
                d1:max\t9f860100
                d1:null\t00000080
                ## latest_by_key
                error: [51] v (DECIMAL(5,2)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                native
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                """);
        rec("DECIMAL(18,3)", """
                ## alter
                target\td0:min|d0:max|d0:null|d1:min|d1:max|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=TIMESTAMP]
                FLOAT\tnull|null|null|-1.0E15|1.0E15|null
                DOUBLE\tnull|null|null|-1.0E15|1.0E15|null
                STRING\t|||-999999999999999.999|999999999999999.999|
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=IPv4]
                VARCHAR\t|||-999999999999999.999|999999999999999.999|
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DECIMAL(18,3), new=DOUBLE[]]
                DECIMAL8\t|||||
                DECIMAL16\t|||||
                DECIMAL32\t|||||
                DECIMAL64\t|||||
                DECIMAL128\t|||-999999999999999.9990000000|999999999999999.9990000000|
                DECIMAL256\t|||-999999999999999.99900000000000000000|999999999999999.99900000000000000000|
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DECIMAL(18,3), new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DECIMAL(18,3), new=GEOHASH(12c)]
                DECIMAL(5,2)\t|||||
                DECIMAL(18,3)\terror: alter: [53] column 'v' type is already 'DECIMAL(18,3)'
                DOUBLE[][]\terror: alter: [50] incompatible column type change [existing=DECIMAL(18,3), new=DOUBLE[][]]
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t-999999999999999.999
                ## single_row-frames
                frame 0 rows=1 native
                min\t01009c584c491ff2
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## dedup
                k\tv
                dup:min\t-999999999999999.999
                shift:min\t999999999999999.999
                dup:max\t999999999999999.999
                shift:max\t
                dup:null\t
                shift:null\t-999999999999999.999
                ## dedup-frames
                frame 0 rows=6 native
                dup:min\t01009c584c491ff2
                shift:min\tffff63a7b3b6e00d
                dup:max\tffff63a7b3b6e00d
                shift:max\t0000000000000080
                dup:null\t0000000000000080
                shift:null\t01009c584c491ff2
                ## insert
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## frames
                frame 0 rows=3 native
                min\t01009c584c491ff2
                max\tffff63a7b3b6e00d
                null\t0000000000000080
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t-999999999999999.999
                null\t
                ## frames-o3-none
                frame 0 rows=2 native
                min\t01009c584c491ff2
                null\t0000000000000080
                ## frames-o3
                frame 0 rows=3 native
                min\t01009c584c491ff2
                max\tffff63a7b3b6e00d
                null\t0000000000000080
                ## parquet
                k\tv
                d0:min\t-999999999999999.999
                d0:max\t999999999999999.999
                d0:null\t
                d1:min\t-999999999999999.999
                d1:max\t999999999999999.999
                d1:null\t
                ## parquet-frames
                frame 0 rows=3 parquet
                d0:min\t01009c584c491ff2
                d0:max\tffff63a7b3b6e00d
                d0:null\t0000000000000080
                frame 1 rows=3 native
                d1:min\t01009c584c491ff2
                d1:max\tffff63a7b3b6e00d
                d1:null\t0000000000000080
                ## parquet-native
                k\tv
                d0:min\t-999999999999999.999
                d0:max\t999999999999999.999
                d0:null\t
                d1:min\t-999999999999999.999
                d1:max\t999999999999999.999
                d1:null\t
                ## parquet-native-frames
                frame 0 rows=3 native
                d0:min\t01009c584c491ff2
                d0:max\tffff63a7b3b6e00d
                d0:null\t0000000000000080
                frame 1 rows=3 native
                d1:min\t01009c584c491ff2
                d1:max\tffff63a7b3b6e00d
                d1:null\t0000000000000080
                ## tops
                k\tv
                d0:min\t
                d0:max\t
                d0:null\t
                d1:min\t
                d1:max\t999999999999999.999
                d1:null\t
                d2:min\t-999999999999999.999
                d2:max\t999999999999999.999
                d2:null\t
                ## tops-frames
                frame 0 rows=3 native
                d0:min\ttop
                d0:max\ttop
                d0:null\ttop
                frame 1 rows=1 native
                d1:min\ttop
                frame 2 rows=2 native
                d1:max\tffff63a7b3b6e00d
                d1:null\t0000000000000080
                frame 3 rows=3 native
                d2:min\t01009c584c491ff2
                d2:max\tffff63a7b3b6e00d
                d2:null\t0000000000000080
                ## tops-o3
                k\tv
                d0:min\t
                o3d0:min\t-999999999999999.999
                d0:max\t
                o3d0:max\t999999999999999.999
                d0:null\t
                o3d0:null\t
                d1:min\t
                o3d1:min\t-999999999999999.999
                d1:max\t999999999999999.999
                o3d1:max\t999999999999999.999
                d1:null\t
                o3d1:null\t
                d2:min\t-999999999999999.999
                d2:max\t999999999999999.999
                d2:null\t
                ## tops-o3-frames
                frame 0 rows=6 native
                d0:min\t0000000000000080
                o3d0:min\t01009c584c491ff2
                d0:max\t0000000000000080
                o3d0:max\tffff63a7b3b6e00d
                d0:null\t0000000000000080
                o3d0:null\t0000000000000080
                frame 1 rows=6 native
                d1:min\t0000000000000080
                o3d1:min\t01009c584c491ff2
                d1:max\tffff63a7b3b6e00d
                o3d1:max\tffff63a7b3b6e00d
                d1:null\t0000000000000080
                o3d1:null\t0000000000000080
                frame 2 rows=3 native
                d2:min\t01009c584c491ff2
                d2:max\tffff63a7b3b6e00d
                d2:null\t0000000000000080
                ## latest_by_key
                error: [51] v (DECIMAL(18,3)): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                parquet
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                native
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                """);
        rec("DOUBLE[][]", """
                ## dedup
                error: create: [118] dedup key columns cannot include ARRAY [column=v, type=DOUBLE[][]]
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## tops
                k\tv
                d0:min\tnull
                d0:max\tnull
                d0:empty\tnull
                d0:specials\tnull
                d0:null\tnull
                d1:min\tnull
                d1:max\tnull
                d1:empty\t[]
                d1:specials\t[[null,null,null,-0.0]]
                d1:null\tnull
                d2:min\t[[-1.7976931348623157E308]]
                d2:max\t[[1.7976931348623157E308]]
                d2:empty\t[]
                d2:specials\t[[null,null,null,-0.0]]
                d2:null\tnull
                ## tops-frames
                frame 0 rows=5 native
                d0:min\ttop
                d0:max\ttop
                d0:empty\ttop
                d0:specials\ttop
                d0:null\ttop
                frame 1 rows=2 native
                d1:min\ttop
                d1:max\ttop
                frame 2 rows=3 native
                d1:empty\taux=00000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=08000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=30000000000000000000000000000000 data=
                frame 3 rows=5 native
                d2:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d2:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d2:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d2:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d2:null\taux=50000000000000000000000000000000 data=
                ## tops-o3
                k\tv
                d0:min\tnull
                o3d0:min\t[[-1.7976931348623157E308]]
                d0:max\tnull
                o3d0:max\t[[1.7976931348623157E308]]
                d0:empty\tnull
                o3d0:empty\t[]
                d0:specials\tnull
                o3d0:specials\t[[null,null,null,-0.0]]
                d0:null\tnull
                o3d0:null\tnull
                d1:min\tnull
                o3d1:min\t[[-1.7976931348623157E308]]
                d1:max\tnull
                o3d1:max\t[[1.7976931348623157E308]]
                d1:empty\t[]
                o3d1:empty\t[]
                d1:specials\t[[null,null,null,-0.0]]
                o3d1:specials\t[[null,null,null,-0.0]]
                d1:null\tnull
                o3d1:null\tnull
                d2:min\t[[-1.7976931348623157E308]]
                d2:max\t[[1.7976931348623157E308]]
                d2:empty\t[]
                d2:specials\t[[null,null,null,-0.0]]
                d2:null\tnull
                ## tops-o3-frames
                frame 0 rows=10 native
                d0:min\taux=00000000000000000000000000000000 data=
                o3d0:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d0:max\taux=10000000000000000000000000000000 data=
                o3d0:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d0:empty\taux=20000000000000000000000000000000 data=
                o3d0:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d0:specials\taux=28000000000000000000000000000000 data=
                o3d0:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d0:null\taux=50000000000000000000000000000000 data=
                o3d0:null\taux=50000000000000000000000000000000 data=
                frame 1 rows=10 native
                d1:min\taux=00000000000000000000000000000000 data=
                o3d1:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d1:max\taux=10000000000000000000000000000000 data=
                o3d1:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                o3d1:empty\taux=28000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=30000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                o3d1:specials\taux=58000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=80000000000000000000000000000000 data=
                o3d1:null\taux=80000000000000000000000000000000 data=
                frame 2 rows=5 native
                d2:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d2:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d2:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d2:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d2:null\taux=50000000000000000000000000000000 data=
                ## alter
                target\td0:min|d0:max|d0:empty|d0:specials|d0:null|d1:min|d1:max|d1:empty|d1:specials|d1:null
                BOOLEAN\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=BOOLEAN]
                BYTE\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=BYTE]
                SHORT\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=SHORT]
                CHAR\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=CHAR]
                INT\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=INT]
                LONG\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=LONG]
                DATE\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=DATE]
                TIMESTAMP\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=TIMESTAMP]
                FLOAT\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=FLOAT]
                DOUBLE\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=DOUBLE]
                STRING\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=STRING]
                SYMBOL\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=SYMBOL]
                LONG256\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=LONG256]
                GEOBYTE\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(7b)]
                GEOSHORT\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(3c)]
                GEOINT\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(6c)]
                GEOLONG\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(8c)]
                BINARY\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=BINARY]
                UUID\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=UUID]
                LONG128\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=LONG128]
                IPv4\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=IPv4]
                VARCHAR\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=VARCHAR]
                DOUBLE[]\terror: alter: [48] incompatible column type change [existing=DOUBLE[][], new=DOUBLE[]]
                DECIMAL8\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(2,1)]
                DECIMAL16\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(4,2)]
                DECIMAL32\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(9,0)]
                DECIMAL64\terror: alter: [53] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(16,4)]
                DECIMAL128\terror: alter: [54] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(38,10)]
                DECIMAL256\terror: alter: [54] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(76,20)]
                INTERVAL\terror: alter: [41] non-persisted type: INTERVAL
                VARCHAR_SLICE\terror: alter: [41] unsupported column type: VARCHAR_SLICE
                TIMESTAMP_NS\terror: alter: [41] incompatible column type change [existing=DOUBLE[][], new=TIMESTAMP_NS]
                GEOHASH(1c)\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(1c)]
                GEOHASH(8b)\terror: alter: [51] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(8b)]
                GEOHASH(31b)\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(31b)]
                GEOHASH(12c)\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=GEOHASH(12c)]
                DECIMAL(5,2)\terror: alter: [52] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(5,2)]
                DECIMAL(18,3)\terror: alter: [53] incompatible column type change [existing=DOUBLE[][], new=DECIMAL(18,3)]
                DOUBLE[][]\terror: alter: [50] column 'v' type is already 'DOUBLE[][]'
                INTERVAL(us)\terror: alter: [41] non-persisted type: INTERVAL
                INTERVAL(ns)\terror: alter: [41] non-persisted type: INTERVAL
                ## insert
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## frames
                frame 0 rows=5 native
                min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                empty\taux=20000000000000000800000000000000 data=0000000000000000
                specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                null\taux=50000000000000000000000000000000 data=
                ## insert-o3-none
                error: insert : [-1] cannot insert rows out of order to non-partitioned table. Table=<dbRoot>/ins_n0o~
                k\tv
                min\t[[-1.7976931348623157E308]]
                empty\t[]
                null\tnull
                ## frames-o3-none
                frame 0 rows=3 native
                min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                empty\taux=10000000000000000800000000000000 data=0000000000000000
                null\taux=18000000000000000000000000000000 data=
                ## frames-o3
                frame 0 rows=5 native
                min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                empty\taux=20000000000000000800000000000000 data=0000000000000000
                specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                null\taux=50000000000000000000000000000000 data=
                ## empty_table
                k\tv
                ## empty_table-frames
                ## single_row
                k\tv
                min\t[[-1.7976931348623157E308]]
                ## single_row-frames
                frame 0 rows=1 native
                min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                ## empty_partition
                k\tv
                k\tv
                ## empty_partition-frames
                ## parquet
                k\tv
                d0:min\t[[-1.7976931348623157E308]]
                d0:max\t[[1.7976931348623157E308]]
                d0:empty\t[]
                d0:specials\t[[null,null,null,-0.0]]
                d0:null\tnull
                d1:min\t[[-1.7976931348623157E308]]
                d1:max\t[[1.7976931348623157E308]]
                d1:empty\t[]
                d1:specials\t[[null,null,null,-0.0]]
                d1:null\tnull
                ## parquet-frames
                frame 0 rows=5 parquet
                d0:min\tvalue=[[-1.7976931348623157E308]]
                d0:max\tvalue=[[1.7976931348623157E308]]
                d0:empty\tvalue=[]
                d0:specials\tvalue=[[null,null,null,-0.0]]
                d0:null\tvalue=null
                frame 1 rows=5 native
                d1:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d1:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=50000000000000000000000000000000 data=
                ## parquet-native
                k\tv
                d0:min\t[[-1.7976931348623157E308]]
                d0:max\t[[1.7976931348623157E308]]
                d0:empty\t[]
                d0:specials\t[[null,null,null,-0.0]]
                d0:null\tnull
                d1:min\t[[-1.7976931348623157E308]]
                d1:max\t[[1.7976931348623157E308]]
                d1:empty\t[]
                d1:specials\t[[null,null,null,-0.0]]
                d1:null\tnull
                ## parquet-native-frames
                frame 0 rows=5 native
                d0:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d0:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d0:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d0:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d0:null\taux=50000000000000000000000000000000 data=
                frame 1 rows=5 native
                d1:min\taux=00000000000000001000000000000000 data=0100000001000000ffffffffffffefff
                d1:max\taux=10000000000000001000000000000000 data=0100000001000000ffffffffffffef7f
                d1:empty\taux=20000000000000000800000000000000 data=0000000000000000
                d1:specials\taux=28000000000000002800000000000000 data=0100000004000000000000000000f87f000000000000f87f000000000000f87f0000000000000080
                d1:null\taux=50000000000000000000000000000000 data=
                ## latest_by_key
                error: [51] v (DOUBLE[][]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON
                ## parquet_convert
                error: alter: [48] incompatible column type change [existing=VARCHAR, new=DOUBLE[][]]
                parquet
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\t
                native
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\t
                """);
        rec("INTERVAL(us)", """
                ## empty_table
                error: create: [37] non-persisted type: INTERVAL
                ## empty_table-frames
                error: create: [37] non-persisted type: INTERVAL
                ## single_row
                error: create: [37] non-persisted type: INTERVAL
                ## single_row-frames
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition-frames
                error: create: [37] non-persisted type: INTERVAL
                ## parquet
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-frames
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native-frames
                error: create: [34] non-persisted type: INTERVAL
                ## alter
                target\td0:max|d0:null|d1:max|d1:null
                *\terror: add column: [34] non-persisted type: INTERVAL error: insert d1:: [25] Invalid column: v
                ## tops
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## dedup
                error: create: [35] non-persisted type: INTERVAL
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## insert
                error: create: [35] non-persisted type: INTERVAL
                ## frames
                error: create: [35] non-persisted type: INTERVAL
                ## insert-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3
                error: create: [35] non-persisted type: INTERVAL
                ## latest_by_key
                error: create: [34] non-persisted type: INTERVAL
                ## parquet_convert
                error: create: [35] non-persisted type: INTERVAL
                """);
        rec("INTERVAL(ns)", """
                ## empty_table
                error: create: [37] non-persisted type: INTERVAL
                ## empty_table-frames
                error: create: [37] non-persisted type: INTERVAL
                ## single_row
                error: create: [37] non-persisted type: INTERVAL
                ## single_row-frames
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition
                error: create: [37] non-persisted type: INTERVAL
                ## empty_partition-frames
                error: create: [37] non-persisted type: INTERVAL
                ## insert
                error: create: [35] non-persisted type: INTERVAL
                ## frames
                error: create: [35] non-persisted type: INTERVAL
                ## insert-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3-none
                error: create: [35] non-persisted type: INTERVAL
                ## frames-o3
                error: create: [35] non-persisted type: INTERVAL
                ## dedup
                error: create: [35] non-persisted type: INTERVAL
                error: [17] table does not exist [table=dedup_t]
                ## dedup-frames
                error: [17] table does not exist [table=dedup_t]
                ## parquet
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-frames
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native
                error: create: [34] non-persisted type: INTERVAL
                ## parquet-native-frames
                error: create: [34] non-persisted type: INTERVAL
                ## tops
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3
                error: add column: [34] non-persisted type: INTERVAL
                ## tops-o3-frames
                error: add column: [34] non-persisted type: INTERVAL
                ## alter
                target\td0:max|d0:null|d1:max|d1:null
                *\terror: add column: [34] non-persisted type: INTERVAL error: insert d1:: [25] Invalid column: v
                ## latest_by_key
                error: create: [34] non-persisted type: INTERVAL
                ## parquet_convert
                error: create: [35] non-persisted type: INTERVAL
                """);
    }
    // recordings: end

    private static void rec(String label, String recording) {
        RECORDINGS.put(label, recording);
    }
}
