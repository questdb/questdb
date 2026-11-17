/*+*****************************************************************************
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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.MultipleFailureException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class ParquetDedupColumnTopTest extends AbstractCairoTest {
    private final KeyType keyType;

    public ParquetDedupColumnTopTest(KeyType keyType) {
        this.keyType = keyType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<KeyType> parameters() {
        return List.of(KeyType.BOOLEAN, KeyType.BYTE, KeyType.SHORT, KeyType.CHAR);
    }

    public static void runReplayScenario(KeyType type, int groupSize, int groups, Shape shape, int version,
                                         boolean hasExtraKeys, int replayKind, boolean isSplitDelivery) throws Exception {
        configure(groupSize, version, 1_024, true);
        try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, type, groupSize, groups, shape, hasExtraKeys, false)) {
            scenario.prepare(type.firstKey(), null);
            scenario.replay(replayKind, isSplitDelivery);
            scenario.replay(replayKind, false);
            scenario.insert(2, 800_000, type.firstKey(), 0);
            scenario.assertRows();
        }
    }

    @Test
    public void testPreparedConvertedKeyControl() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            if (keyType == KeyType.BYTE || keyType == KeyType.CHAR) {
                String source = keyType == KeyType.BYTE ? "INT" : "VARCHAR";
                try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.NO_TOP, false, false)) {
                    scenario.prepare(keyType.firstKey(), null);
                    // Convert back to native to give the file a sentinel/variable-width source type.
                    scenario.executeBoth("ALTER TABLE %s ALTER COLUMN b TYPE " + source);
                    scenario.execute("ALTER TABLE p CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    scenario.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    scenario.executeBoth("ALTER TABLE %s ALTER COLUMN b TYPE " + keyType);
                    scenario.replay(1, false);
                }
            }
            runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 1, false);
        });
    }

    @Test
    public void testReplayAcrossEncodings() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<String> encodings = new ObjList<>();
            encodings.add("plain");
            if (keyType != KeyType.BOOLEAN) {
                encodings.add("rle_dictionary");
                encodings.add("delta_binary_packed");
            }
            List<Throwable> failures = new ArrayList<>();
            for (int version = 1; version <= 2; version++) {
                for (int i = 0; i < encodings.size(); i++) {
                    for (int kind : new int[]{0, 2}) {
                        configure(4, version, 1_024, false);
                        try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                            scenario.prepare(keyType.firstKey(), encodings.getQuick(i));
                            scenario.replay(kind, false);
                            Assert.assertEquals(kind == 0 ? 15 : 16, scenario.rows.size());
                        } catch (AssertionError e) {
                            failures.add(new AssertionError("version=" + version + ", encoding=" + encodings.getQuick(i) + ", kind=" + kind, e));
                        }
                    }
                }
            }
            assertNoFailures(failures);
        });
    }

    @Test
    public void testReplayAcrossMultiPageColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            for (int version = 1; version <= 2; version++) {
                for (int kind : new int[]{0, 2}) {
                    // PLAIN with 1 KiB pages splits even BOOLEAN keys across multiple pages.
                    configure(4_096, version, 1_024, false);
                    try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4_096, 3, Shape.TOP, false, false)) {
                        scenario.prepare(keyType.firstKey(), "plain");
                        scenario.replay(kind, false);
                        Assert.assertEquals(kind == 0 ? 12_291 : 12_292, scenario.rows.size());
                    }
                }
            }
        });
    }

    @Test
    public void testReplayAcrossNonPrefixColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            for (int groupSize : new int[]{4, 5}) {
                for (int version = 1; version <= 2; version++) {
                    for (int kind : new int[]{0, 1, 2, 3}) {
                        runReplayScenario(keyType, groupSize, 3, Shape.TOP, version, false, kind, false);
                    }
                }
            }
        });
    }

    @Test
    public void testReplayAfterKeyColumnAddedToParquet() throws Exception {
        assertMemoryLeak(() -> {
            assertMissingOrAllNull(true);
            runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 0, false);
        });
    }

    @Test
    public void testReplayAfterNoSentinelKeyTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            List<KeyType> targets = switch (keyType) {
                case BOOLEAN -> List.of(KeyType.BYTE, KeyType.SHORT);
                case BYTE -> List.of(KeyType.SHORT);
                case SHORT -> List.of(KeyType.BYTE);
                case CHAR -> List.of(KeyType.CHAR);
            };
            for (KeyType target : targets) {
                try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                    scenario.prepare(keyType == KeyType.SHORT ? 256 : keyType.firstKey(), null);
                    if (target != keyType) {
                        scenario.changeType(target);
                    }
                    scenario.replay(1, false);
                }
            }
            runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 1, false);
        });
    }

    @Test
    public void testReplayAfterReopen() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                scenario.replay(0, false);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                scenario.assertRows();
                scenario.replay(0, false);
                scenario.insert(2, 800_000, keyType.firstKey(), 0);
                scenario.assertRows();
            }
        });
    }

    @Test
    public void testReplayAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            DefaultCairoConfiguration config = new DefaultCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                @Override
                public int getPartitionEncoderParquetRowGroupSize() {
                    return 4;
                }
            };
            ReplayScenario scenario;
            try (CairoEngine db = new CairoEngine(config); SqlExecutionContext context = TestUtils.createSqlExecutionCtx(db)) {
                db.load();
                scenario = new ReplayScenario(db, context, keyType, 4, 3, Shape.TOP, false, false);
                scenario.prepare(keyType.firstKey(), null);
                scenario.replay(0, false);
            }
            try (CairoEngine db = new CairoEngine(config); SqlExecutionContext context = TestUtils.createSqlExecutionCtx(db)) {
                db.load();
                scenario.db = db;
                scenario.context = context;
                try (scenario) {
                    scenario.assertRows();
                    scenario.replay(0, false);
                }
            }
        });
    }

    @Test
    public void testReplayAfterTruncateAndRecreate() throws Exception {
        assertMemoryLeak(() -> {
            runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 0, false);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                scenario.executeBoth("TRUNCATE TABLE %s");
                scenario.rows.clear();
                scenario.assertRows();
                scenario.executeBoth("ALTER TABLE %s DEDUP DISABLE");
                scenario.executeBoth("ALTER TABLE %s RENAME COLUMN b TO old_b");
                scenario.executeBoth("ALTER TABLE %s DROP COLUMN old_b");
                scenario.populate(keyType.firstKey(), null);
                scenario.replay(0, false);
            }
        });
    }

    @Test
    public void testReplayAllNullChunk() throws Exception {
        assertMemoryLeak(() -> {
            assertMissingOrAllNull(false);
            runReplayScenario(keyType, 4, 3, Shape.TOP, 2, false, 0, false);
        });
    }

    @Test
    public void testReplayAllNullRange() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 2, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.executeBoth("CREATE TABLE %s (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                scenario.populateRange(0, 12, false);
                scenario.executeBoth("ALTER TABLE %s ADD COLUMN b " + keyType);
                scenario.executeBoth("ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts,b)");
                scenario.insert(1440, 900_000, keyType.secondKey(), 0);
                scenario.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                try (TableReader reader = engine.getReader("p")) {
                    reader.openPartition(0);
                    ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
                    Assert.assertEquals(3, meta.getRowGroupCount());
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(4, meta.getRowGroupSize(i));
                        Assert.assertEquals(4, meta.getChunkNullCount(i, 2));
                    }
                }
                scenario.firstKey = keyType.firstKey();
                scenario.replay(0, false);
            }
            runReplayScenario(keyType, 4, 3, Shape.TOP, 2, false, 0, false);
        });
    }

    @Test
    public void testReplayAtServerDefaultRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            configure(100_000, 1, 1_048_576, false);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 100_000, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                scenario.replay(0, false);
                Assert.assertEquals(300_003, scenario.rows.size());
            }
        });
    }

    @Test
    public void testReplayKeepsMergeStrategy() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                long before = scenario.partitionTxn();
                scenario.replay(0, false);
                Assert.assertNotEquals(before, scenario.partitionTxn());
                try (TableReader reader = engine.getReader("p")) {
                    reader.openPartition(0);
                    ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
                    Assert.assertEquals(3, meta.getRowGroupCount());
                    Assert.assertEquals(5, meta.getRowGroupSize(0));
                    Assert.assertEquals(5, meta.getRowGroupSize(1));
                    Assert.assertEquals(4, meta.getRowGroupSize(2));
                    Assert.assertEquals(0, meta.getUnusedBytes());
                }
            }
        });
    }

    @Test
    public void testReplayKeepsPinnedReaderSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                ObjList<Row> snapshot = new ObjList<>(scenario.rows);
                CountDownLatch pinned = new CountDownLatch(1);
                CountDownLatch committed = new CountDownLatch(1);
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread reader = new Thread(() -> {
                    try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine);
                         RecordCursorFactory factory = engine.select("SELECT ts,x,b FROM p", context);
                         RecordCursor cursor = factory.getCursor(context)) {
                        scenario.assertCursor(snapshot, cursor);
                        pinned.countDown();
                        Assert.assertTrue(committed.await(30, TimeUnit.SECONDS));
                        cursor.toTop();
                        scenario.assertCursor(snapshot, cursor);
                    } catch (Throwable e) {
                        error.set(e);
                    } finally {
                        pinned.countDown();
                    }
                }, "parquet-dedup-snapshot");
                reader.start();
                try {
                    Assert.assertTrue(pinned.await(30, TimeUnit.SECONDS));
                    Assert.assertNull(error.get());
                    scenario.replay(0, false);
                } finally {
                    committed.countDown();
                    reader.join(30_000);
                }
                Assert.assertFalse(reader.isAlive());
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                scenario.replay(0, false);
            }
        });
    }

    @Test
    public void testReplayKeyValueClasses() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            for (int value : keyType.valuesToTest()) {
                for (int mode = 0; mode < 3; mode++) {
                    try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                        scenario.prepare(value == 0 ? keyType.firstKey() : value, null);
                        if (mode == 0) {
                            scenario.insertPair(value, keyType.literal(value));
                        } else if (mode == 1) {
                            execute("CREATE TABLE input AS (SELECT " + keyType.literal(value) + " AS b)");
                            try {
                                scenario.executeBoth("INSERT INTO %s SELECT '2024-01-01T00:01'::TIMESTAMP, 500_001, b FROM input UNION ALL SELECT '2024-01-01T00:03'::TIMESTAMP, 600_000, " + keyType.literal(keyType.secondKey()));
                                scenario.upsert(1, 500_001, value, 0);
                                scenario.upsert(3, 600_000, keyType.secondKey(), 0);
                            } finally {
                                execute("DROP TABLE input");
                            }
                        } else {
                            keyType.bind(value);
                            bindVariableService.setInt(1, 500_001);
                            for (String table : List.of("p", "n")) {
                                try (SqlCompiler compiler = engine.getSqlCompiler();
                                     InsertOperation insert = compiler.compile("INSERT INTO " + table + " VALUES ('2024-01-01T00:01', $2, $1), ('2024-01-01T00:03', 600_000, " + keyType.literal(keyType.secondKey()) + ")", sqlExecutionContext).popInsertOperation()) {
                                    for (int repeat = 0; repeat < 2; repeat++) {
                                        keyType.bind(repeat == 0 ? value : keyType.secondKey());
                                        bindVariableService.setInt(1, 500_001 + repeat);
                                        try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                                            method.execute(sqlExecutionContext);
                                            method.commit();
                                        }
                                        drainWalQueue();
                                    }
                                }
                            }
                            scenario.upsert(1, 500_001, value, 0);
                            scenario.upsert(1, 500_002, keyType.secondKey(), 0);
                            scenario.upsert(3, 600_000, keyType.secondKey(), 0);
                        }
                        scenario.assertRows();
                    }
                }
            }
        });
    }

    @Test
    public void testReplayNanosecondTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            configure(5, 2, 1_024, true);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 5, 3, Shape.TOP, false, true)) {
                scenario.prepare(keyType.firstKey(), null);
                scenario.replay(0, false);
            }
        });
    }

    @Test
    public void testReplayThenConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                scenario.replay(0, false);
                scenario.execute("ALTER TABLE p CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                scenario.assertRows();
                scenario.replay(1, false);
            }
        });
    }

    @Test
    public void testReplayWithMixedDedupKeys() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean hasIndex : new boolean[]{false, true}) {
                configure(4, 1, 1_024, true);
                try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, true, false)) {
                    scenario.prepare(keyType.firstKey(), null);
                    if (hasIndex) {
                        scenario.executeBoth("ALTER TABLE %s ALTER COLUMN s ADD INDEX");
                    }
                    scenario.replay(0, false);
                    scenario.insert(1, 700_000, keyType.firstKey(), 1);
                    scenario.assertRows();
                    scenario.insert(1, 700_001, keyType.firstKey(), 1);
                    scenario.assertRows();
                }
            }
        });
    }

    @Test
    public void testSingleGroupLeadingColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 1, Shape.PARTIAL, false, false)) {
                scenario.executeBoth("CREATE TABLE %s (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                scenario.executeBoth("INSERT INTO %s VALUES ('2024-01-01T00:00',0), ('2024-01-01T00:01',1)");
                scenario.executeBoth("ALTER TABLE %s ADD COLUMN b " + keyType);
                scenario.executeBoth("ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts,b)");
                scenario.rows.add(new Row(0, 0, 0, 0));
                scenario.rows.add(new Row(1, 1, 0, 0));
                scenario.insert(2, 2, keyType.firstKey(), 0);
                scenario.insert(3, 3, keyType.secondKey(), 0);
                scenario.insert(1440, 4, keyType.secondKey(), 0);
                scenario.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                try (TableReader reader = engine.getReader("p")) {
                    reader.openPartition(0);
                    ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
                    Assert.assertEquals(1, meta.getRowGroupCount());
                    Assert.assertEquals(2, meta.getChunkNullCount(0, 2));
                }
                scenario.insert(0, 10, 0, 0);
                scenario.insert(2, 12, keyType.firstKey(), 0);
                scenario.assertRows();
                Assert.assertEquals(5, scenario.rows.size());
            }
            runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 0, false);
        });
    }

    @Test
    public void testSingleRowGroupAndNoTopControls() throws Exception {
        assertMemoryLeak(() -> {
            for (Shape shape : List.of(Shape.NO_TOP, Shape.PARTIAL, Shape.TWO_SPANS)) {
                runReplayScenario(keyType, 4, shape == Shape.TWO_SPANS ? 4 : 3, shape, 2, false, 0, false);
            }
            configure(4, 1, 1_024, true);
            try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 3, Shape.TOP, false, false)) {
                scenario.prepare(keyType.firstKey(), null);
                long before = scenario.partitionTxn();
                scenario.insert(1, 500_001, keyType.firstKey(), 0);
                scenario.assertRows();
                Assert.assertEquals(before, scenario.partitionTxn());
                scenario.replay(1, false);
            }
        });
    }

    @Test
    public void testZeroDefaultKeyDoesNotOverwriteNonzeroKey() throws Exception {
        assertMemoryLeak(() -> runReplayScenario(keyType, 4, 3, Shape.TOP, 1, false, 4, false));
    }

    private static void assertNoFailures(List<Throwable> failures) throws Exception {
        try {
            MultipleFailureException.assertEmpty(failures);
        } catch (Exception e) {
            throw e;
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    private static void configure(int groupSize, int version, int pageSize, boolean isPermissive) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, groupSize);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, version);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, pageSize);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, isPermissive ? "1.0" : "0.5");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, isPermissive ? Long.MAX_VALUE : 1_073_741_824L);
    }

    private void assertMissingOrAllNull(boolean isMissing) throws Exception {
        configure(4, 1, 1_024, true);
        try (ReplayScenario scenario = new ReplayScenario(engine, sqlExecutionContext, keyType, 4, 1, Shape.TOP, false, false)) {
            scenario.executeBoth("CREATE TABLE %s (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            scenario.executeBoth("INSERT INTO %s SELECT dateadd('m', (x - 1)::INT, '2024-01-01'::TIMESTAMP), (x - 1)::INT FROM long_sequence(4)");
            scenario.executeBoth("INSERT INTO %s VALUES ('2024-01-02', 4)");
            if (isMissing) {
                scenario.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            scenario.executeBoth("ALTER TABLE %s ADD COLUMN b " + keyType);
            if (!isMissing) {
                scenario.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            scenario.executeBoth("ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts,b)");
            for (int i = 0; i < 4; i++) {
                scenario.rows.add(new Row(i, i, 0, 0));
            }
            scenario.rows.add(new Row(1440, 4, 0, 0));
            for (int repeat = 0; repeat < 2; repeat++) {
                scenario.executeBoth("INSERT INTO %s VALUES ('2024-01-01T00:00', 10, " + keyType.literal(0) + "), ('2024-01-01T00:01', 11, " + keyType.literal(keyType.firstKey()) + ")");
                scenario.upsert(0, 10, 0, 0);
                scenario.upsert(1, 11, keyType.firstKey(), 0);
                scenario.assertRows();
                Assert.assertEquals(6, scenario.rows.size());
            }
        }
    }

    public enum KeyType {
        BOOLEAN, BYTE, SHORT, CHAR;

        public int firstKey() {
            return this == BOOLEAN ? 1 : this == CHAR ? 69 : 5;
        }

        public int secondKey() {
            return this == BOOLEAN ? 1 : this == CHAR ? 70 : 6;
        }

        private void bind(int value) throws Exception {
            switch (this) {
                case BOOLEAN -> bindVariableService.setBoolean(0, value != 0);
                case BYTE -> bindVariableService.setByte(0, (byte) value);
                case SHORT -> bindVariableService.setShort(0, (short) value);
                case CHAR -> bindVariableService.setChar(0, (char) value);
            }
        }

        private String literal(int value) {
            return this == BOOLEAN ? value == 0 ? "false" : "true" : value + "::" + this;
        }

        private void put(TableWriter.Row row, int column, int value) {
            switch (this) {
                case BOOLEAN -> row.putBool(column, value != 0);
                case BYTE -> row.putByte(column, (byte) value);
                case SHORT -> row.putShort(column, (short) value);
                case CHAR -> row.putChar(column, (char) value);
            }
        }

        private int read(Record record) {
            return switch (this) {
                case BOOLEAN -> record.getBool(2) ? 1 : 0;
                case BYTE -> record.getByte(2);
                case SHORT -> record.getShort(2);
                case CHAR -> record.getChar(2);
            };
        }

        private int[] valuesToTest() {
            return switch (this) {
                case BOOLEAN -> new int[]{0, 1};
                case BYTE -> new int[]{-128, -127, -1, 0, 1, 126, 127};
                case SHORT -> new int[]{-32_768, -32_767, -1, 0, 1, 32_766, 32_767};
                case CHAR -> new int[]{0, 1, 65, 127, 128, 2047, 2048, 65_534, 65_535};
            };
        }
    }

    public enum Shape {
        TOP, PARTIAL, NO_TOP, TWO_SPANS
    }

    private record Row(int minute, int payload, int key, int extra) {
    }

    /**
     * Shared SQL history and independent last-write-wins oracle for the constrained storage fuzzer.
     */
    private static class ReplayScenario implements AutoCloseable {
        private final int groupSize;
        private final int groups;
        private final boolean hasExtraKeys;
        private final boolean isNanos;
        private final ObjList<Row> rows = new ObjList<>();
        private final Shape shape;
        private SqlExecutionContext context;
        private CairoEngine db;
        private int firstKey;
        private KeyType type;

        private ReplayScenario(CairoEngine db, SqlExecutionContext context, KeyType type, int groupSize, int groups,
                               Shape shape, boolean hasExtraKeys, boolean isNanos) {
            this.db = db;
            this.context = context;
            this.type = type;
            this.groupSize = groupSize;
            this.groups = groups;
            this.shape = shape;
            this.hasExtraKeys = hasExtraKeys;
            this.isNanos = isNanos;
        }

        @Override
        public void close() throws Exception {
            executeBoth("DROP TABLE IF EXISTS %s");
        }

        private void assertCursor(ObjList<Row> expected, RecordCursor cursor) {
            for (int i = 0; i < expected.size(); i++) {
                Assert.assertTrue("missing row " + i, cursor.hasNext());
                Row row = expected.getQuick(i);
                Record record = cursor.getRecord();
                Assert.assertEquals("timestamp at " + i, timestamp(row.minute), record.getTimestamp(0));
                Assert.assertEquals("payload at " + i, row.payload, record.getInt(1));
                Assert.assertEquals("key at " + i, row.key, type.read(record));
                if (hasExtraKeys) {
                    Assert.assertEquals(row.extra, record.getShort(3));
                    TestUtils.assertEquals(row.extra == 0 ? "a" : "b", record.getSymA(4));
                }
            }
            Assert.assertFalse("unexpected extra row", cursor.hasNext());
        }

        private void assertLayout() {
            try (TableReader reader = db.getReader("p")) {
                reader.openPartition(0);
                ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
                int keyIndex = meta.getColumnCount() - 1;
                Assert.assertEquals(groups, meta.getRowGroupCount());
                int top = shape == Shape.PARTIAL ? groupSize + groupSize / 2 : shape == Shape.NO_TOP ? 0 : groups * groupSize;
                for (int g = 0; g < groups; g++) {
                    boolean isRewritten = g == 0 || shape == Shape.TWO_SPANS && g == 2;
                    Assert.assertEquals(groupSize + (isRewritten ? 1 : 0), meta.getRowGroupSize(g));
                    Assert.assertEquals(isRewritten ? 0 : Math.min(groupSize, Math.max(0, top - g * groupSize)), meta.getChunkNullCount(g, keyIndex));
                }
                Assert.assertEquals(0, reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(0), reader.getMetadata().getColumnIndexQuiet("b")));
            }
        }

        private void assertRows() throws Exception {
            rows.sort(Comparator.comparingInt(Row::minute).thenComparingInt(Row::payload));
            // Typed reads also check NUL and nonprinting CHAR keys without printer escaping.
            try (RecordCursorFactory factory = db.select(query("p"), context); RecordCursor cursor = factory.getCursor(context)) {
                assertCursor(rows, cursor);
                cursor.toTop();
                assertCursor(rows, cursor);
            }
            TestUtils.assertSqlCursors(db, context, query("n"), query("p"), LOG);
        }

        private void changeType(KeyType target) throws Exception {
            executeBoth("ALTER TABLE %s ALTER COLUMN b TYPE " + target);
            for (int i = 0; i < rows.size(); i++) {
                Row row = rows.getQuick(i);
                int value = target == KeyType.BYTE ? (byte) row.key : (short) row.key;
                rows.setQuick(i, new Row(row.minute, row.payload, value, row.extra));
            }
            firstKey = target == KeyType.BYTE ? (byte) firstKey : (short) firstKey;
            type = target;
            assertRows();
        }

        private void execute(String sql) throws Exception {
            db.execute(sql, context);
            TestUtils.drainWalQueue(db);
        }

        private void executeBoth(String sql) throws Exception {
            execute(sql.formatted("p"));
            execute(sql.formatted("n"));
        }

        private String extraValues(int extra) {
            return hasExtraKeys ? ", " + extra + "::SHORT, '" + (extra == 0 ? "a" : "b") + "'" : "";
        }

        private void insert(int minute, int payload, int key, int extra) throws Exception {
            executeBoth("INSERT INTO %s (ts,x,b" + (hasExtraKeys ? ",k,s" : "") + ") VALUES (" + timestamp(minute) + "::" + timestampType() + "," + payload + "," + type.literal(key) + extraValues(extra) + ")");
            upsert(minute, payload, key, extra);
        }

        private void insertPair(int value, String expression) throws Exception {
            executeBoth("INSERT INTO %s (ts,x,b" + (hasExtraKeys ? ",k,s" : "") + ") VALUES ('2024-01-01T00:01',500_001," + expression + extraValues(0) + "), ('2024-01-01T00:03',600_000," + type.literal(type.secondKey()) + extraValues(0) + ")");
            upsert(1, 500_001, value, 0);
            upsert(3, 600_000, type.secondKey(), 0);
        }

        private long partitionTxn() {
            try (TableReader reader = db.getReader("p")) {
                return reader.getPartitionNameTxnByIndex(0);
            }
        }

        private void populate(int key, String encoding) throws Exception {
            firstKey = key;
            int count = groupSize * groups;
            int top = shape == Shape.PARTIAL ? groupSize + groupSize / 2 : shape == Shape.NO_TOP ? 0 : count;
            if (top > 0) {
                populateRange(0, top, false);
            }
            executeBoth("ALTER TABLE %s ADD COLUMN b " + type);
            if (top < count) {
                populateRange(top, count, true);
            }
            if (encoding != null) {
                execute("ALTER TABLE p ALTER COLUMN b SET PARQUET(" + encoding + ")");
            }
            executeBoth("ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts,b" + (hasExtraKeys ? ",k,s" : "") + ")");
            insert(1440, 900_000, type.secondKey(), 0);
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            long before = partitionTxn();
            insert(1, 500_000, firstKey, 0);
            if (shape == Shape.TWO_SPANS) {
                insert(5, 500_005, firstKey, 0);
            }
            Assert.assertEquals("first delivery must stay in-place", before, partitionTxn());
            assertLayout();
            assertRows();
        }

        private void populateRange(int lo, int hi, boolean hasKey) throws Exception {
            String minute = "CASE WHEN x = 1 THEN 0" + (shape == Shape.TWO_SPANS ? " WHEN x > " + (groupSize * 2 + 1) + " THEN 6" : "") + " ELSE 3 END";
            executeBoth("INSERT INTO %s (ts,x" + (hasExtraKeys ? ",k,s" : "") + (hasKey ? ",b" : "") + ") SELECT (" + timestamp(0) + "+(" + minute + ")*" + (isNanos ? 60_000_000_000L : 60_000_000L) + ")::" + timestampType() + ", (x-1)::INT" + extraValues(0) + (hasKey ? "," + type.literal(0) : "") + " FROM long_sequence(" + hi + ") WHERE x > " + lo);
            for (int i = lo; i < hi; i++) {
                rows.add(new Row(i == 0 ? 0 : shape == Shape.TWO_SPANS && i > groupSize * 2 ? 6 : 3, i, 0, 0));
            }
        }

        private void prepare(int key, String encoding) throws Exception {
            executeBoth("CREATE TABLE %s (ts " + timestampType() + ", x INT" + (hasExtraKeys ? ",k SHORT,s SYMBOL" : "") + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            populate(key, encoding);
        }

        private String query(String table) {
            return "SELECT ts,x,b" + (hasExtraKeys ? ",k,s" : "") + " FROM " + table + " ORDER BY ts,x";
        }

        private void replay(int kind, boolean isSplitDelivery) throws Exception {
            int value = kind >= 2 ? 0 : firstKey;
            int payload = kind == 0 ? 500_000 : 500_001;
            if (isSplitDelivery) {
                insert(1, payload, value, 0);
                assertRows();
            }
            String expression = kind == 3 || kind == 4 ? "NULL" : type.literal(value);
            String columns = "ts,x,b" + (hasExtraKeys ? ",k,s" : "");
            if (kind == 4) {
                // Omit b on only the replay row. Keep the boundary key nonzero: enabling
                // dedup does not collapse duplicates that predate it, even on replacement.
                for (String table : List.of("p", "n")) {
                    try (WalWriter writer = db.getWalWriter(db.verifyTableName(table))) {
                        TableWriter.Row row = writer.newRow(timestamp(1));
                        row.putInt(1, payload);
                        row.append();
                        row = writer.newRow(timestamp(3));
                        row.putInt(1, 600_000);
                        type.put(row, writer.getMetadata().getColumnIndexQuiet("b"), type.secondKey());
                        row.append();
                        writer.commit();
                    }
                    TestUtils.drainWalQueue(db);
                }
                upsert(1, payload, 0, 0);
                upsert(3, 600_000, type.secondKey(), 0);
            } else {
                executeBoth("INSERT INTO %s (" + columns + ") VALUES ('2024-01-01T00:01'," + payload + "," + expression + extraValues(0) + "), ('2024-01-01T00:03',600_000," + type.literal(type.secondKey()) + extraValues(0) + ")" + (shape == Shape.TWO_SPANS ? ", ('2024-01-01T00:05',500_005," + type.literal(firstKey) + extraValues(0) + "), ('2024-01-01T00:06',600_006," + type.literal(type.secondKey()) + extraValues(0) + ")" : ""));
                upsert(1, payload, value, 0);
                upsert(3, 600_000, type.secondKey(), 0);
                if (shape == Shape.TWO_SPANS) {
                    upsert(5, 500_005, firstKey, 0);
                    upsert(6, 600_006, type.secondKey(), 0);
                }
            }
            assertRows();
        }

        private long timestamp(int minute) {
            long micros = 1_704_067_200_000_000L + minute * 60_000_000L;
            return isNanos ? micros * 1_000 : micros;
        }

        private String timestampType() {
            return isNanos ? "TIMESTAMP_NS" : "TIMESTAMP";
        }

        private void upsert(int minute, int payload, int key, int extra) {
            // This history only upserts unique or new keys; pre-enable duplicate keys stay untouched.
            for (int i = rows.size() - 1; i >= 0; i--) {
                Row row = rows.getQuick(i);
                if (row.minute == minute && row.key == key && row.extra == extra) {
                    rows.remove(i);
                }
            }
            rows.add(new Row(minute, payload, key, extra));
        }
    }
}
