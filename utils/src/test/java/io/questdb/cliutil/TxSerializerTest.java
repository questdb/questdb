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

package io.questdb.cliutil;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class TxSerializerTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    private static DefaultCairoConfiguration configuration;
    private static CairoEngine engine;
    private static SqlExecutionContextImpl sqlExecutionContext;

    public static void createTestPath(CharSequence root) {
        try (Path path = new Path().of(root)) {
            if (Files.exists(path.$())) {
                return;
            }
            Files.mkdirs(path.of(root).slash(), 509);
        }
    }

    public static void removeTestPath(CharSequence root) {
        Path path = Path.getThreadLocal(root);
        Assert.assertTrue(Files.rmdir(path.slash(), true));
    }

    public static void setCairoStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
        configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
    }

    @BeforeClass
    public static void setUpStatic() {
        setCairoStatic();
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDownStatic() {
        engine.close();
    }

    @Before
    public void setUp() {
        createTestPath(root);
        engine.getTableIdGenerator().open();
        engine.getTableIdGenerator().reset();
        engine.reloadTableNames();
    }

    @After
    public void tearDown() {
        engine.getTableIdGenerator().close();
        engine.clear();
        engine.getTableSequencerAPI().releaseInactive();
        engine.closeNameRegistry();
        removeTestPath(root);
    }

    @Test
    public void testPartitionedDaily() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 1000000000000) ts " +
                "from long_sequence(10)" +
                ") timestamp(ts) PARTITION BY DAY";

        testRoundTxnSerialization(createTableSql, true);
    }

    @Test
    public void testPartitionedDailyWithTruncate() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 1000000000000) ts " +
                "from long_sequence(10)" +
                ") timestamp(ts) PARTITION BY DAY";

        engine.execute(createTableSql, sqlExecutionContext);

        TxSerializer serializer = new TxSerializer();
        String txPath = root.toString() + Files.SEPARATOR + "xxx" + Files.SEPARATOR + "_txn";
        String json = serializer.toJson(txPath);
        Assert.assertTrue(json.contains("\"TX_OFFSET_TRUNCATE_VERSION\": 0"));

        engine.execute("truncate table xxx", sqlExecutionContext);
        json = serializer.toJson(txPath);
        Assert.assertTrue(json.contains("\"TX_OFFSET_TRUNCATE_VERSION\": 1"));
    }

    @Test
    public void testPartitionedNoneNoTimestamp() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 1000000000000) ts " +
                "from long_sequence(10)" +
                ")";

        testRoundTxnSerialization(createTableSql, true);
    }

    @Test
    public void testPartitionedNoneWithTimestamp() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 1000000000000) ts " +
                "from long_sequence(10)" +
                ") timestamp(ts)";

        testRoundTxnSerialization(createTableSql, false);
    }

    private void assertFirstColumnValueLong(String sql, long expected) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(expected, cursor.getRecord().getLong(0));
            }
        }
    }

    private long getColumnValueLong(String sql) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private void testRoundTxnSerialization(String createTableSql, boolean oooSupports) throws SqlException {
        engine.execute(createTableSql, sqlExecutionContext);
        assertFirstColumnValueLong("select count() from xxx", 10);
        long symCount = getColumnValueLong("select count_distinct(sym1) from xxx");
        long symCount2 = getColumnValueLong("select count_distinct(sym2) from xxx");

        engine.releaseAllWriters();
        engine.releaseAllReaders();

        TxSerializer serializer = new TxSerializer();
        String txPath = root.toString() + Files.SEPARATOR + "xxx" + Files.SEPARATOR + "_txn";
        String json = serializer.toJson(txPath);
        serializer.serializeJson(json, txPath);

        // Repeat
        String json2 = serializer.toJson(txPath);
        Assert.assertEquals(json, json2);
        serializer.serializeJson(json, txPath);

        assertFirstColumnValueLong("select count() from xxx", 10);
        assertFirstColumnValueLong("select count_distinct(sym1) from xxx", symCount);
        assertFirstColumnValueLong("select count_distinct(sym2) from xxx", symCount2);
        assertFirstColumnValueLong("select x from xxx limit 1", 1);
        assertFirstColumnValueLong("select x from xxx limit 5, 6", 6);

        if (oooSupports) {
            // Insert same records
            engine.execute("insert into xxx select * from xxx", sqlExecutionContext);
        } else {
            engine.execute("insert into xxx select sym1, sym2, x, dateadd('y', 1, ts) from xxx", sqlExecutionContext);
        }
        assertFirstColumnValueLong("select count() from xxx", 20);
        assertFirstColumnValueLong("select count_distinct(sym1) from xxx", symCount);
        assertFirstColumnValueLong("select count_distinct(sym2) from xxx", symCount2);
        assertFirstColumnValueLong("select x from xxx limit 1", 1);
    }
}
