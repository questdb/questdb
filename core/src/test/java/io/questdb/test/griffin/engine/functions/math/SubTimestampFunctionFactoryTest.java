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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SubTimestampFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNonNumericCharThrows() throws Exception {
        // A non-numeric CHAR cannot be converted to LONG; the user gets a typed cast error,
        // not an internal UnsupportedOperationException.
        assertMemoryLeak(() -> assertException(
                "SELECT '2020-01-01T00:00:00.000000Z'::timestamp - 'a'",
                0,
                "inconvertible value"));
    }

    @Test
    public void testNowMinusCharInIntervalScan() throws Exception {
        // Reproduces the production crash: "WHERE ts > now() - '1'" pushed into the interval
        // optimizer (RuntimeIntervalModel) evaluated the CHAR operand via getTimestamp() and
        // threw UnsupportedOperationException. now() is fixed via setCurrentMicros() to keep
        // the result deterministic.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (8::timestamp), (9::timestamp), (10::timestamp), (11::timestamp)");
            setCurrentMicros(10);
            // now() is 10 micros, now() - '1' is 9 micros, so ts > 9 keeps rows 10 and 11.
            assertQuery("SELECT ts FROM x WHERE ts > now() - '1'")
                    .timestamp("ts")
                    .returns("ts\n" +
                            "1970-01-01T00:00:00.000010Z\n" +
                            "1970-01-01T00:00:00.000011Z\n");
        });
    }

    @Test
    public void testNowPlusCharInIntervalScan() throws Exception {
        // The symmetric '+' operator already worked; assert it keeps working alongside the fix.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (8::timestamp), (9::timestamp), (10::timestamp), (11::timestamp)");
            setCurrentMicros(10);
            // now() is 10 micros, now() + '1' is 11 micros, so ts > 11 keeps nothing.
            assertQuery("SELECT ts FROM x WHERE ts > now() + '1'")
                    .timestamp("ts")
                    .returns("ts\n");
        });
    }

    @Test
    public void testTimestampMinusCharConstant() throws Exception {
        // A single-character literal is a CHAR. The '-' operator must accept it as the LONG
        // operand and subtract its numeric value, mirroring the '+' operator.
        assertMemoryLeak(() -> assertQuery("SELECT '2020-01-01T00:00:00.000000Z'::timestamp - '1'")
                .expectSize()
                .returns("column\n2019-12-31T23:59:59.999999Z\n"));
    }

    @Test
    public void testTimestampMinusLongConstant() throws Exception {
        // Regression: a plain numeric LONG operand keeps working.
        assertMemoryLeak(() -> assertQuery("SELECT '2020-01-01T00:00:00.000000Z'::timestamp - 1000000")
                .expectSize()
                .returns("column\n2019-12-31T23:59:59.000000Z\n"));
    }

    @Test
    public void testTimestampMinusNullChar() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT '2020-01-01T00:00:00.000000Z'::timestamp - null")
                .expectSize()
                .returns("column\n\n"));
    }

    @Test
    public void testTimestampNanoMinusCharConstant() throws Exception {
        // The nanosecond timestamp variant must subtract in nanoseconds.
        assertMemoryLeak(() -> assertQuery("SELECT '2020-01-01T00:00:00.000000000Z'::timestamp_ns - '1'")
                .expectSize()
                .returns("column\n2019-12-31T23:59:59.999999999Z\n"));
    }
}
