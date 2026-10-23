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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.groupby.LastNotNullLong256GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.CharSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FirstLastLong256GroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testFirstLastOverLong256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('a', NULL, 1_000_000),
                        ('a', 0x8000000000000000000000000000000000000000000000000000000000000001, 2_000_000),
                        ('a', 0x018000000000000000, 3_000_000),
                        ('a', NULL, 4_000_000),
                        ('b', NULL, 5_000_000),
                        ('c', 0x02, 6_000_000)
                    """);
            assertQuery("SELECT k, first(l), last(l), first_not_null(l), last_not_null(l) FROM t ORDER BY k")
                    .expectSize()
                    .returns("""
                            k\tfirst\tlast\tfirst_not_null\tlast_not_null
                            a\t\t\t0x8000000000000000000000000000000000000000000000000000000000000001\t0x018000000000000000
                            b\t\t\t\t
                            c\t0x02\t0x02\t0x02\t0x02
                            """);
            assertQuery("SELECT first(l), last(l), first_not_null(l), last_not_null(l) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            first\tlast\tfirst_not_null\tlast_not_null
                            \t0x02\t0x8000000000000000000000000000000000000000000000000000000000000001\t0x02
                            """);
        });
    }

    @Test
    public void testLastNotNullStoresTheValueItChecked() throws Exception {
        assertMemoryLeak(() -> {
            final NullOnSecondReadFunction arg = new NullOnSecondReadFunction();
            final LastNotNullLong256GroupByFunction function = new LastNotNullLong256GroupByFunction(arg);
            final ArrayColumnTypes types = new ArrayColumnTypes();
            function.initValueTypes(types);
            try (SimpleMapValue value = new SimpleMapValue(types.getColumnCount())) {
                function.setNull(value);
                function.computeNext(value, null, 7);
                Assert.assertEquals(1, arg.reads);
                Assert.assertEquals(7, value.getLong(0));
                Assert.assertEquals(NullOnSecondReadFunction.VALUE, value.getLong256A(1).getLong0());
            }
        });
    }

    private static class NullOnSecondReadFunction extends Long256Function {
        private static final long VALUE = 42;
        private final Long256Impl long256 = new Long256Impl();
        private int reads;

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long256 getLong256A(Record rec) {
            if (++reads > 1) {
                return Long256Impl.NULL_LONG256;
            }
            long256.setAll(VALUE, 0, 0, 0);
            return long256;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return getLong256A(rec);
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }
    }
}
