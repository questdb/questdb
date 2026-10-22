/*+*****************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class HttpHorizonJoinTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        testHttpClient.setKeepConnection(false);
    }

    @Test
    public void testProjectionOverExec() throws Exception {
        // One row per trade and offset, with the trades' designated timestamp, which the JSON
        // response reports by its column index.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
            engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
            engine.execute("""
                    INSERT INTO trades VALUES
                        ('2026-01-01T00:00:00Z', 'A', 10),
                        ('2026-01-01T00:00:00Z', 'A', 10),
                        ('2026-01-01T00:00:02Z', 'C', 30)
                    """, sqlExecutionContext);
            engine.execute("""
                    INSERT INTO quotes VALUES
                        ('2026-01-01T00:00:00Z', 'A', 10),
                        ('2026-01-01T00:00:01Z', 'A', 12)
                    """, sqlExecutionContext);
            final String sql = "SELECT t.sym, t.ts, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 1s) AS h";
            testHttpClient.assertGet(
                    "{\"query\":\"" + sql + "\"," +
                            "\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                            "{\"name\":\"offset\",\"type\":\"LONG\"},{\"name\":\"bid\",\"type\":\"DOUBLE\"}]," +
                            "\"timestamp\":1," +
                            "\"dataset\":[" +
                            "[\"A\",\"2026-01-01T00:00:00.000000Z\",0,10.0]," +
                            "[\"A\",\"2026-01-01T00:00:00.000000Z\",1000000,12.0]," +
                            "[\"A\",\"2026-01-01T00:00:00.000000Z\",0,10.0]," +
                            "[\"A\",\"2026-01-01T00:00:00.000000Z\",1000000,12.0]," +
                            "[\"C\",\"2026-01-01T00:00:02.000000Z\",0,null]," +
                            "[\"C\",\"2026-01-01T00:00:02.000000Z\",1000000,null]" +
                            "]," +
                            "\"count\":6}",
                    sql
            );
        }));
    }
}
