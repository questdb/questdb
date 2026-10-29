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
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.ApproxPercentileDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLong256GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctStringGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctUuidGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctVarcharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.HaversineDistDegreeGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.IsIPv4OrderedGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.IsLongOrderedGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.PercentileDiscDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins which group-by functions report a non-NULL value over an empty group: the count() family
 * reports 0, isOrdered() reports true and haversine_dist_deg() reports 0.0. A constant-false
 * WINDOW JOIN reads those values once at compile time, so a function that changes its
 * {@code setEmpty()} must change {@code hasNonNullEmptyValue()} with it.
 */
public class GroupByFunctionHasNonNullEmptyValueTest extends AbstractCairoTest {

    @Test
    public void testComputeNonNullEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            // NULL-empty functions sit before, between and after the functions with a non-NULL empty
            // value, so each value must land at its own function's position.
            final ObjList<GroupByFunction> functions = new ObjList<>();
            functions.add(new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)));
            addNonNullEmptyFunctions(functions);
            functions.add(new FirstDoubleGroupByFunction(DoubleColumn.newInstance(0)));
            functions.add(new PercentileDiscDoubleGroupByFunction(configuration, DoubleColumn.newInstance(0), new DoubleConstant(0.5), 0));
            functions.add(new CountLongConstGroupByFunction());
            functions.add(new ApproxPercentileDoubleGroupByFunction(DoubleColumn.newInstance(0), new DoubleConstant(0.5), 3, 0));
            try {
                final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getQuick(i).initValueTypes(valueTypes);
                }
                // Stale entries from an earlier call must not survive.
                final IntList emptyValueTypes = new IntList();
                emptyValueTypes.add(ColumnType.STRING);
                final LongList emptyValues = GroupByUtils.computeNonNullEmptyValues(functions, valueTypes.getColumnCount(), emptyValueTypes);
                Assert.assertNotNull(emptyValues);
                Assert.assertEquals(functions.size(), emptyValues.size());
                Assert.assertEquals(functions.size(), emptyValueTypes.size());
                int booleanCount = 0;
                int doubleCount = 0;
                int longCount = 0;
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final GroupByFunction function = functions.getQuick(i);
                    final String name = function.getClass().getSimpleName();
                    final short typeTag = ColumnType.tagOf(function.getType());
                    if (function.hasNonNullEmptyValue()) {
                        Assert.assertEquals(name, typeTag, emptyValueTypes.getQuick(i));
                        switch (typeTag) {
                            case ColumnType.BOOLEAN -> {
                                // isOrdered() reports true.
                                Assert.assertEquals(name, 1, emptyValues.getQuick(i));
                                booleanCount++;
                            }
                            case ColumnType.DOUBLE -> {
                                // haversine_dist_deg() reports 0.0.
                                Assert.assertEquals(name, Double.doubleToRawLongBits(0.0), emptyValues.getQuick(i));
                                doubleCount++;
                            }
                            case ColumnType.LONG -> {
                                // The count() family reports 0.
                                Assert.assertEquals(name, 0, emptyValues.getQuick(i));
                                longCount++;
                            }
                            default -> Assert.fail(name + " has an unsupported type: " + ColumnType.nameOf(typeTag));
                        }
                    } else {
                        Assert.assertEquals(name, ColumnType.UNDEFINED, emptyValueTypes.getQuick(i));
                        Assert.assertEquals(name, Numbers.LONG_NULL, emptyValues.getQuick(i));
                    }
                }
                Assert.assertEquals(2, booleanCount);
                Assert.assertEquals(1, doubleCount);
                Assert.assertEquals(12, longCount);
            } finally {
                Misc.freeObjList(functions);
            }
        });
    }

    @Test
    public void testComputeNonNullEmptyValuesWithoutCount() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<GroupByFunction> functions = new ObjList<>();
            addNullEmptyFunctions(functions);
            try {
                final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getQuick(i).initValueTypes(valueTypes);
                }
                final IntList emptyValueTypes = new IntList();
                emptyValueTypes.add(ColumnType.LONG);
                Assert.assertNull(GroupByUtils.computeNonNullEmptyValues(functions, valueTypes.getColumnCount(), emptyValueTypes));
                Assert.assertEquals(0, emptyValueTypes.size());
            } finally {
                Misc.freeObjList(functions);
            }
        });
    }

    @Test
    public void testHasNonNullEmptyValue() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<GroupByFunction> nonNullEmptyFunctions = new ObjList<>();
            final ObjList<GroupByFunction> nullEmptyFunctions = new ObjList<>();
            try {
                addNonNullEmptyFunctions(nonNullEmptyFunctions);
                addNullEmptyFunctions(nullEmptyFunctions);
                for (int i = 0, n = nonNullEmptyFunctions.size(); i < n; i++) {
                    final GroupByFunction function = nonNullEmptyFunctions.getQuick(i);
                    Assert.assertTrue(function.getClass().getSimpleName(), function.hasNonNullEmptyValue());
                }
                for (int i = 0, n = nullEmptyFunctions.size(); i < n; i++) {
                    final GroupByFunction function = nullEmptyFunctions.getQuick(i);
                    Assert.assertFalse(function.getClass().getSimpleName(), function.hasNonNullEmptyValue());
                }
            } finally {
                Misc.freeObjList(nonNullEmptyFunctions);
                Misc.freeObjList(nullEmptyFunctions);
            }
        });
    }

    // One instance per class that overrides hasNonNullEmptyValue(). Every count() variant extends
    // one of the count classes; isOrdered() and haversine_dist_deg() override it directly.
    private static void addNonNullEmptyFunctions(ObjList<GroupByFunction> functions) {
        functions.add(new CountDoubleGroupByFunction(DoubleColumn.newInstance(0)));
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new CountDistinctIntGroupByFunction(IntColumn.newInstance(0), 16, 0.7));
        functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 16, 0.7));
        functions.add(new CountDistinctLong256GroupByFunction(Long256Column.newInstance(0), 16, 0.7));
        functions.add(new CountDistinctStringGroupByFunction(new StrColumn(0), 16, 0.7));
        functions.add(new CountDistinctUuidGroupByFunction(UuidColumn.newInstance(0), 16, 0.7));
        functions.add(new CountDistinctVarcharGroupByFunction(new VarcharColumn(0), 16, 0.7));
        functions.add(new ApproxCountDistinctIntGroupByFunction(IntColumn.newInstance(0), 14));
        functions.add(new ApproxCountDistinctIPv4GroupByFunction(IPv4Column.newInstance(0), 14));
        functions.add(new ApproxCountDistinctLongGroupByFunction(LongColumn.newInstance(0), 14));
        functions.add(new IsLongOrderedGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new IsIPv4OrderedGroupByFunction(IPv4Column.newInstance(0)));
        functions.add(new HaversineDistDegreeGroupByFunction(
                DoubleColumn.newInstance(0),
                DoubleColumn.newInstance(1),
                TimestampColumn.newInstance(2, ColumnType.TIMESTAMP)
        ));
    }

    private static void addNullEmptyFunctions(ObjList<GroupByFunction> functions) {
        functions.add(new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)));
        functions.add(new FirstDoubleGroupByFunction(DoubleColumn.newInstance(0)));
        functions.add(new PercentileDiscDoubleGroupByFunction(configuration, DoubleColumn.newInstance(0), new DoubleConstant(0.5), 0));
        functions.add(new ApproxPercentileDoubleGroupByFunction(DoubleColumn.newInstance(0), new DoubleConstant(0.5), 3, 0));
    }
}
