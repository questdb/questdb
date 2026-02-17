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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqStrCharFunctionTest extends AbstractCairoTest {

    @Test
    public void testSymEqChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    "insert into tanc2 \n" +
                            "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                            "1571270400000 + (x-1) * 100 timestamp,\n" +
                            "rnd_str(2,2,3) instrument,\n" +
                            "abs(cast(rnd_double(0)*100000 as int)) price,\n" +
                            "abs(cast(rnd_double(0)*10000 as int)) qty,\n" +
                            "rnd_str('B', 'S') side\n" +
                            "from long_sequence(100000) x"
            );

            String expected = "instrument\tsum\n" +
                    "CZ\t2886736\n";

            assertSql(
                    expected, "select instrument, sum(price) from tanc2  where instrument = 'CZ' and side = 'B'"
            );
        });
    }

    @Test
    public void testSymEqCharFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    "insert into tanc2 \n" +
                            "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                            "1571270400000 + (x-1) * 100 timestamp,\n" +
                            "rnd_str(2,2,3) instrument,\n" +
                            "abs(cast(rnd_double(0)*100000 as int)) price,\n" +
                            "abs(cast(rnd_double(0)*10000 as int)) qty,\n" +
                            "rnd_str('B', 'S') side\n" +
                            "from long_sequence(100000) x"
            );

            String expected = "instrument\tsum\n" +
                    "ML\t563832\n";

            assertSql(
                    expected, "select instrument, sum(price) from tanc2  where instrument = 'ML' and side = rnd_char()"
            );
        });
    }

    @Test
    public void testSymEqCharFunctionConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute("insert into tanc2 \n" +
                    "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                    "1571270400000 + (x-1) * 100 timestamp,\n" +
                    "rnd_str(2,2,3) instrument,\n" +
                    "abs(cast(rnd_double(0)*100000 as int)) price,\n" +
                    "abs(cast(rnd_double(0)*10000 as int)) qty,\n" +
                    "rnd_str('B', 'S') side\n" +
                    "from long_sequence(100000) x");

            String expected = "instrument\tsum\n" +
                    "ML\t2617153\n";

            assertSql(
                    expected, "select instrument, sum(price) from tanc2  where instrument = 'ML' and rnd_symbol('A', 'B', 'C') = 'B'"
            );
        });
    }

    @Test
    public void testSymEqCharNotFound() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    "insert into tanc2 \n" +
                            "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                            "1571270400000 + (x-1) * 100 timestamp,\n" +
                            "rnd_str(2,2,3) instrument,\n" +
                            "abs(cast(rnd_double(0)*100000 as int)) price,\n" +
                            "abs(cast(rnd_double(0)*10000 as int)) qty,\n" +
                            "rnd_str('B', 'S') side\n" +
                            "from long_sequence(100000) x"
            );

            String expected = "instrument\tsum\n";

            assertSql(
                    expected, "select instrument, sum(price) from tanc2  where instrument = 'KK' and side = 'C'"
            );
        });
    }
}