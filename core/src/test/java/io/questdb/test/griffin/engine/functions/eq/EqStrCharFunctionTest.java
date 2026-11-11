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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqStrCharFunctionTest extends AbstractCairoTest {

    @Test
    public void testCharNulConstantMatchesCharNulRow() throws Exception {
        // Regression: query fuzzer caught literal vs bind divergence on
        //   (0.645116::FLOAT)::CHAR != ((0.713754::FLOAT)::INT)::CHAR
        // Both sides evaluate to CHAR(0). The literal form folds the whole
        // comparison to FALSE; the bind form folded to TRUE. The dispatcher
        // ranked the swapped =(SA) overload as an exact match (the constant
        // CHAR side matches STRING via supportImplicitCastCharToStr) and
        // EqStrCharFunctionFactory's constant-CHAR-as-STRING branch read
        // CHAR(0) as a NULL string and short-circuited to a "false" constant,
        // ignoring that the runtime path treats NULL string as equal to
        // CHAR(0). Mirror the runtime semantics by probing the non-constant
        // CHAR side against (char) 0 instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            assertQuery("SELECT k FROM t WHERE (0.645116::FLOAT)::CHAR != ((0.713754::FLOAT)::INT)::CHAR")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "0.645116");
            assertQuery("SELECT k FROM t WHERE (:b0::FLOAT)::CHAR != ((0.713754::FLOAT)::INT)::CHAR")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n");
            assertQuery("SELECT k FROM t WHERE (0.645116::FLOAT)::CHAR = ((0.713754::FLOAT)::INT)::CHAR")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n1\n2\n");
            assertQuery("SELECT k FROM t WHERE (:b0::FLOAT)::CHAR = ((0.713754::FLOAT)::INT)::CHAR")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n1\n2\n");
        });
    }

    @Test
    public void testSymEqChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    """
                            insert into tanc2\s
                            select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,
                            1571270400000 + (x-1) * 100 timestamp,
                            rnd_str(2,2,3) instrument,
                            abs(cast(rnd_double(0)*100000 as int)) price,
                            abs(cast(rnd_double(0)*10000 as int)) qty,
                            rnd_str('B', 'S') side
                            from long_sequence(100000) x"""
            );

            String expected = """
                    instrument\tsum
                    CZ\t2886736
                    """;

            assertQuery("select instrument, sum(price) from tanc2  where instrument = 'CZ' and side = 'B'")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testSymEqCharFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    """
                            insert into tanc2\s
                            select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,
                            1571270400000 + (x-1) * 100 timestamp,
                            rnd_str(2,2,3) instrument,
                            abs(cast(rnd_double(0)*100000 as int)) price,
                            abs(cast(rnd_double(0)*10000 as int)) qty,
                            rnd_str('B', 'S') side
                            from long_sequence(100000) x"""
            );

            String expected = """
                    instrument\tsum
                    ML\t563832
                    """;

            assertQuery("select instrument, sum(price) from tanc2  where instrument = 'ML' and side = rnd_char()")
                    .noLeakCheck()
                    .returnsOnce(expected);
        });
    }

    @Test
    public void testSymEqCharFunctionConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute("""
                    insert into tanc2\s
                    select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,
                    1571270400000 + (x-1) * 100 timestamp,
                    rnd_str(2,2,3) instrument,
                    abs(cast(rnd_double(0)*100000 as int)) price,
                    abs(cast(rnd_double(0)*10000 as int)) qty,
                    rnd_str('B', 'S') side
                    from long_sequence(100000) x""");

            String expected = """
                    instrument\tsum
                    ML\t2617153
                    """;

            assertQuery("select instrument, sum(price) from tanc2  where instrument = 'ML' and rnd_symbol('A', 'B', 'C') = 'B'")
                    .noLeakCheck()
                    .returnsOnce(expected);
        });
    }

    @Test
    public void testSymEqCharNotFound() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            execute(
                    """
                            insert into tanc2\s
                            select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,
                            1571270400000 + (x-1) * 100 timestamp,
                            rnd_str(2,2,3) instrument,
                            abs(cast(rnd_double(0)*100000 as int)) price,
                            abs(cast(rnd_double(0)*10000 as int)) qty,
                            rnd_str('B', 'S') side
                            from long_sequence(100000) x"""
            );

            String expected = "instrument\tsum\n";

            assertQuery("select instrument, sum(price) from tanc2  where instrument = 'KK' and side = 'C'")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }
}