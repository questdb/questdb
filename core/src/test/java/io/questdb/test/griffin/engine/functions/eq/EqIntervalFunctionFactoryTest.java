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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * An untyped NULL compared with an INTERVAL is a NULL interval: {@code NULL = iv} and {@code iv IS NULL} hold
 * exactly when the interval is NULL, and {@code !=} and {@code <>} negate them, whichever side the NULL stands on.
 */
public class EqIntervalFunctionFactoryTest extends AbstractCairoTest {
    private static final String HEADER = "a\tb\tc\td\te\tf\tg\th\tt\n";
    private static final String NULL_ROW = "true\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue\tfalse\tBOOLEAN\n";
    private static final String OPS = "null = %1$s a, %1$s = null b, null != %1$s c, %1$s != null d, null <> %1$s e,"
            + " %1$s <> null f, %1$s IS NULL g, %1$s IS NOT NULL h, typeOf(null != %1$s) t";
    private static final String VALUE_ROW = "false\tfalse\ttrue\ttrue\ttrue\ttrue\tfalse\ttrue\tBOOLEAN\n";

    @Test
    public void testNonNullIntervalsStillCompareByValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO x VALUES ('2020-01-01', '2020-01-02'), ('2020-01-01', '2020-01-03'), (null, null)");
            assertQuery("""
                    SELECT iv = interval('2020-01-01', '2020-01-02') a, interval('2020-01-01', '2020-01-02') != iv b, iv = iv c
                    FROM (SELECT interval(lo, hi) iv FROM x)
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            true\tfalse\ttrue
                            false\ttrue\ttrue
                            false\ttrue\ttrue
                            """);
        });
    }

    @Test
    public void testNullComparedWithConstantInterval() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT " + String.format(OPS, "interval('2020-01-01', '2020-01-02')"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + VALUE_ROW);
            assertQuery("SELECT " + String.format(OPS, "interval(null::TIMESTAMP, null::TIMESTAMP)"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + NULL_ROW);
        });
    }

    @Test
    public void testNullComparedWithInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO x VALUES ('2020-01-01', '2020-01-02'), (null, null)");
            assertQuery("SELECT " + String.format(OPS, "iv") + " FROM (SELECT interval(lo, hi) iv FROM x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + VALUE_ROW + NULL_ROW);
            assertQuery("SELECT lo FROM x WHERE null != interval(lo, hi)")
                    .noLeakCheck()
                    .returns("lo\n2020-01-01T00:00:00.000000Z\n");
            assertQuery("SELECT lo FROM x WHERE interval(lo, hi) IS NULL")
                    .noLeakCheck()
                    .returns("lo\n\n");
        });
    }

    @Test
    public void testNullComparedWithNanosInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (lo TIMESTAMP_NS, hi TIMESTAMP_NS)");
            execute("INSERT INTO x VALUES ('2020-01-01T00:00:00.000000001Z', '2020-01-02'), (null, null)");
            assertQuery("SELECT " + String.format(OPS, "iv") + " FROM (SELECT interval(lo, hi) iv FROM x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + VALUE_ROW + NULL_ROW);
        });
    }

    @Test
    public void testNullComparedWithRuntimeConstantInterval() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, MicrosTimestampDriver.floor("2020-01-01"));
            bindVariableService.setTimestamp(1, MicrosTimestampDriver.floor("2020-01-02"));
            assertQuery("SELECT " + String.format(OPS, "interval($1, $2)"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + VALUE_ROW);
            bindVariableService.clear();
            bindVariableService.setTimestamp(0);
            bindVariableService.setTimestamp(1);
            assertQuery("SELECT " + String.format(OPS, "interval($1, $2)"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + NULL_ROW);
        });
    }
}
