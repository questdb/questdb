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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactory.EmptyGroupValue;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Checks what group by factories declare an aggregate reports over a group of no rows, see
 * {@link FunctionFactory#getEmptyGroupValue()}, against what the functions they build report.
 */
public class EmptyGroupValueTest extends AbstractCairoTest {

    @Test
    public void testEveryGroupByFunctionReportsTheDeclaredValueOverNoRows() throws Exception {
        Assert.assertTrue("the check that a built function agrees with its factory needs -ea", FunctionParser.class.desiredAssertionStatus());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE czw_prices (" + GroupByCallSweep.PRICES_COLUMNS + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO czw_prices SELECT " + GroupByCallSweep.PRICES_VALUES + ", timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(3)");
            final FunctionFactoryCache cache = engine.getFunctionFactoryCache();
            final HashMap<String, EmptyGroupValue> declared = declaredValues(cache);
            final TreeSet<String> calls = new TreeSet<>();
            final TreeSet<String> functionNames = new TreeSet<>();
            GroupByCallSweep.collectCalls(cache, "", FunctionFactory::isGroupBy, calls, functionNames);
            final TreeSet<String> builtFunctionNames = new TreeSet<>();
            int nonNullCount = 0;
            for (String call : calls) {
                final String name = call.substring(0, call.indexOf('('));
                final EmptyGroupValue value = declared.get(name);
                try (
                        RecordCursorFactory factory = select("SELECT " + call + " v FROM czw_prices WHERE sym = 'z'");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    builtFunctionNames.add(name);
                    if (value == EmptyGroupValue.NULL) {
                        continue;
                    }
                    final Record record = cursor.getRecord();
                    Assert.assertTrue(call, cursor.hasNext());
                    final int type = factory.getMetadata().getColumnType(0);
                    switch (value) {
                        case ZERO -> {
                            if (ColumnType.tagOf(type) == ColumnType.DOUBLE) {
                                Assert.assertEquals(call, 0.0, record.getDouble(0), 0.0);
                            } else {
                                Assert.assertEquals(call, ColumnType.LONG, ColumnType.tagOf(type));
                                Assert.assertEquals(call, 0, record.getLong(0));
                            }
                        }
                        case TRUE -> {
                            Assert.assertEquals(call, ColumnType.BOOLEAN, ColumnType.tagOf(type));
                            Assert.assertTrue(call, record.getBool(0));
                        }
                    }
                    Assert.assertFalse(call, cursor.hasNext());
                    nonNullCount++;
                } catch (SqlException e) {
                    // the aggregate rejects these arguments, or needs a context this query does not give it
                }
            }
            // grouping() and grouping_id() need GROUPING SETS, ROLLUP or CUBE.
            functionNames.removeAll(builtFunctionNames);
            Assert.assertEquals("[grouping, grouping_id]", functionNames.toString());
            Assert.assertTrue("too few calls of aggregates with a value other than NULL over no rows: " + nonNullCount, nonNullCount >= 40);
        });
    }

    @Test
    public void testEveryGroupByOverloadOfANameDeclaresOneValue() {
        final TreeMap<String, EmptyGroupValue> nonNull = new TreeMap<>();
        declaredValues(engine.getFunctionFactoryCache()).forEach((name, value) -> {
            if (value != EmptyGroupValue.NULL) {
                nonNull.put(name, value);
            }
        });
        Assert.assertEquals(
                "{approx_count_distinct=ZERO, count=ZERO, count_distinct=ZERO, haversine_dist_deg=ZERO, isOrdered=TRUE}",
                nonNull.toString()
        );
    }

    /**
     * Reads the value each aggregate name declares over no rows, checking that every group by overload of the name
     * declares the same one and that no other factory declares any value but NULL.
     */
    private static HashMap<String, EmptyGroupValue> declaredValues(FunctionFactoryCache cache) {
        final HashMap<String, EmptyGroupValue> declared = new HashMap<>();
        cache.getFactories().forEach((name, descriptors) -> {
            for (int i = 0, n = descriptors.size(); i < n; i++) {
                final FunctionFactoryDescriptor descriptor = descriptors.getQuick(i);
                final FunctionFactory factory = descriptor.getFactory();
                if (!factory.isGroupBy()) {
                    Assert.assertEquals(descriptor.getName(), EmptyGroupValue.NULL, factory.getEmptyGroupValue());
                    continue;
                }
                final EmptyGroupValue previous = declared.put(descriptor.getName(), factory.getEmptyGroupValue());
                Assert.assertTrue(factory.getSignature(), previous == null || previous == factory.getEmptyGroupValue());
            }
        });
        return declared;
    }
}
