/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ServerMainCleanStartupTest extends AbstractBootstrapTest {
    private final AbstractCairoTest.WalFormat walFormat;

    public ServerMainCleanStartupTest(AbstractCairoTest.WalFormat walFormat) {
        this.walFormat = walFormat;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {AbstractCairoTest.WalFormat.ROW_FIRST},
                {AbstractCairoTest.WalFormat.COL_FIRST}
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.CAIRO_WAL_DEFAULT_FORMAT + "=" + (walFormat == AbstractCairoTest.WalFormat.ROW_FIRST ? "row" : "column")
        ));
        dbPath.parent().$();
    }

    @Test
    public void testServerMainCleanStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            StringSink sink = new StringSink();

            // create two tables:
            // 1. empty
            // 2. non-empty with a couple of translations

            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 1).with(AllowAllSecurityContext.INSTANCE, null)
            ) {
                serverMain.start();
                serverMain.getEngine().compile("create table x (a int, t timestamp) timestamp(t) partition by day wal", sqlExecutionContext);
                serverMain.getEngine().compile("create table y (b int, t timestamp) timestamp(t) partition by day wal", sqlExecutionContext);

                serverMain.getEngine().compile("insert into y values(100, 1)", sqlExecutionContext);
                serverMain.getEngine().compile("insert into y values(200, 2)", sqlExecutionContext);

                // wait for the row count
                try (RecordCursorFactory rfc = serverMain.getEngine().select("select count() from y", sqlExecutionContext)) {
                    while (true) {
                        try (RecordCursor cursor = rfc.getCursor(sqlExecutionContext)) {
                            Record rec = cursor.getRecord();
                            if (cursor.hasNext()) {
                                if (rec.getLong(0) == 2) {
                                    break;
                                }
                            }
                            Os.pause();
                        }
                    }
                }

                // ensure transactions
                TestUtils.assertSql(
                        serverMain.getEngine(),
                        sqlExecutionContext,
                        "select * from wal_tables order by 1",
                        sink,
                        "name\tsuspended\twriterTxn\twriterLagTxnCount\tsequencerTxn\n" +
                                "x\tfalse\t0\t0\t0\n" +
                                "y\tfalse\t2\t0\t2\n"
                );


                TestUtils.assertSql(
                        serverMain.getEngine(),
                        sqlExecutionContext,
                        "select table_name, ownership_reason from writer_pool where table_name in ('x','y') order by 1",
                        sink,
                        "table_name\townership_reason\n" +
                                "x\t\n" +
                                "y\t\n"
                );

            }

            // start a new server; it should not attempt to open new writers
            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 1).with(AllowAllSecurityContext.INSTANCE, null)
            ) {
                serverMain.start();

                TestUtils.assertSql(
                        serverMain.getEngine(),
                        sqlExecutionContext,
                        "select table_name, ownership_reason from writer_pool where table_name in ('x','y') order by 1",
                        sink,
                        "table_name\townership_reason\n"
                );
            }
        });
    }
}
