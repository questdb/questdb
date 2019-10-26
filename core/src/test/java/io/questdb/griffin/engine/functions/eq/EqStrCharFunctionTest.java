/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class EqStrCharFunctionTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testSymEqChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            compiler.compile("insert into tanc2 \n" +
                    "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                    "1571270400000 + (x-1) * 100 timestamp,\n" +
                    "rnd_str(2,2,3) instrument,\n" +
                    "abs(to_int(rnd_double(0)*100000)) price,\n" +
                    "abs(to_int(rnd_double(0)*10000)) qty,\n" +
                    "rnd_str('B', 'S') side\n" +
                    "from long_sequence(100000) x");

            String expected = "instrument\tsum\n" +
                    "CZ\t2886736\n";

            try (RecordCursorFactory factory = compiler.compile("select instrument, sum(price) from tanc2  where instrument = 'CZ' and side = 'B'").getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor()) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testSymEqCharNotFound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            compiler.compile("insert into tanc2 \n" +
                    "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                    "1571270400000 + (x-1) * 100 timestamp,\n" +
                    "rnd_str(2,2,3) instrument,\n" +
                    "abs(to_int(rnd_double(0)*100000)) price,\n" +
                    "abs(to_int(rnd_double(0)*10000)) qty,\n" +
                    "rnd_str('B', 'S') side\n" +
                    "from long_sequence(100000) x");

            String expected = "instrument\tsum\n";

            try (RecordCursorFactory factory = compiler.compile("select instrument, sum(price) from tanc2  where instrument = 'KK' and side = 'C'").getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor()) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testSymEqCharFunction() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            compiler.compile("insert into tanc2 \n" +
                    "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                    "1571270400000 + (x-1) * 100 timestamp,\n" +
                    "rnd_str(2,2,3) instrument,\n" +
                    "abs(to_int(rnd_double(0)*100000)) price,\n" +
                    "abs(to_int(rnd_double(0)*10000)) qty,\n" +
                    "rnd_str('B', 'S') side\n" +
                    "from long_sequence(100000) x");

            String expected = "instrument\tsum\n" +
                    "ML\t563832\n";

            try (RecordCursorFactory factory = compiler.compile("select instrument, sum(price) from tanc2  where instrument = 'ML' and side = rnd_char()").getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor()) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testSymEqCharFunctionConst() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table tanc2(ts timestamp, timestamp long, instrument symbol, price long, qty long, side symbol)");
            compiler.compile("insert into tanc2 \n" +
                    "select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) ts,\n" +
                    "1571270400000 + (x-1) * 100 timestamp,\n" +
                    "rnd_str(2,2,3) instrument,\n" +
                    "abs(to_int(rnd_double(0)*100000)) price,\n" +
                    "abs(to_int(rnd_double(0)*10000)) qty,\n" +
                    "rnd_str('B', 'S') side\n" +
                    "from long_sequence(100000) x");

            String expected = "instrument\tsum\n" +
                    "ML\t2617153\n";

            try (RecordCursorFactory factory = compiler.compile("select instrument, sum(price) from tanc2  where instrument = 'ML' and rnd_symbol('A', 'B', 'C') = 'B'").getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor()) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }
}