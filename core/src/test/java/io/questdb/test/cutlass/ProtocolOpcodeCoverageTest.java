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


package io.questdb.test.cutlass;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.WireKind;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.line.LineUtils;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.test.cairo.types.TypeConformanceTypes;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * FR-016 for the protocols (T106): the opcode functions that choose a protocol's per-row writer
 * for a column, whose consumers javac cannot check. Every type of the conformance kit takes part,
 * the types registered later included ({@link TypeConformanceTypes}), so a later type fails here
 * unless every protocol's opcode function handles it.
 * <p>
 * The functions are called by reflection, as {@code TypeRelationGoldenTest} does; one that throws
 * or returns its "unhandled" value fails the test, unless the type is on that function's list
 * below. The lists name today's types a protocol does not write, each for a reason; a type
 * registered later is on none of them, and a listed type that becomes handled fails too, so the
 * lists cannot drift.
 */
public class ProtocolOpcodeCoverageTest {
    private static final Set<String> INTERVALS = Set.of("INTERVAL", "INTERVAL(us)", "INTERVAL(ns)");
    // the transient read_parquet type: no protocol describes a column as VARCHAR_SLICE
    private static final String NOT_STORED = "VARCHAR_SLICE";

    @Test
    public void testOpcodeFunctionsHandleEveryType() throws Exception {
        final Map<String, Set<String>> unhandled = new HashMap<>();
        unhandled.put("printOpcode", Set.of(NOT_STORED));
        // CSV and PostgreSQL wire have no representation for LONG128: its arm refuses the column
        unhandled.put("csvOpcode", Set.of("LONG128", NOT_STORED));
        unhandled.put("outColumnOpcode", Set.of("LONG128", NOT_STORED));
        // BINARY exports through a temporary table (ParquetExportMode.determineExportMode())
        unhandled.put("exportOpcode", Set.of("BINARY"));
        // the fixed-width targets of a Parquet conversion; var-size and text-rendered targets convert
        // through their own paths, and LONG128 has no Parquet form
        unhandled.put("fixedTargetOpcode", with(Set.of("STRING", "SYMBOL", "LONG256", "GEOBYTE", "GEOSHORT", "GEOINT",
                "GEOLONG", "GEOHASH(1c)", "GEOHASH(8b)", "GEOHASH(31b)", "GEOHASH(12c)", "BINARY", "LONG128", "VARCHAR",
                "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS));
        unhandled.put("columnKind", Set.of(NOT_STORED));
        // QWP egress has no wire code for LONG128, INTERVAL and the decimals narrower than DECIMAL64
        unhandled.put("appendOpcode", with(Set.of("LONG128", "DECIMAL8", "DECIMAL16", "DECIMAL32", "DECIMAL(5,2)", NOT_STORED), INTERVALS));

        final Method print = method(CursorPrinter.class, "printOpcode", int.class);
        final Method csv = method(ExportQueryProcessor.class, "csvOpcode", int.class);
        final Method pg = method(PGPipelineEntry.class, "outColumnOpcode", int.class, short.class);
        final Method export = method(HybridColumnMaterializer.class, "exportOpcode", int.class);
        final Method parquet = method(Class.forName("io.questdb.cairo.ParquetColumnTypeConverter"), "fixedTargetOpcode", int.class);
        final Method line = method(LineUtils.class, "columnKind", int.class);
        final Method qwp = method(QwpResultBatchBuffer.class, "appendOpcode", int.class);

        final StringBuilder failures = new StringBuilder();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            final int type = entry.columnType;
            check(failures, unhandled, "printOpcode", entry, () -> (int) print.invoke(null, type) == ColumnType.UNDEFINED);
            check(failures, unhandled, "csvOpcode", entry, () -> {
                final int opcode = (int) csv.invoke(null, type);
                return opcode == ColumnType.NULL || opcode == ColumnType.LONG128;
            });
            check(failures, unhandled, "outColumnOpcode", entry, () -> {
                // a type without a wire kind takes the pseudo route, whose raw (format code, tag)
                // pair outRecord() reports; LONG128's arm refuses the column
                boolean isUnhandled = WireKind.of(type) == null;
                for (short format = 0; format < 2; format++) {
                    isUnhandled |= (int) pg.invoke(null, type, format) == ColumnType.LONG128;
                }
                return isUnhandled;
            });
            check(failures, unhandled, "exportOpcode", entry, () -> (int) export.invoke(null, type) == ColumnType.UNDEFINED);
            check(failures, unhandled, "fixedTargetOpcode", entry, () -> (int) parquet.invoke(null, type) == ColumnType.UNDEFINED);
            check(failures, unhandled, "columnKind", entry, () -> (int) line.invoke(null, type) == ColumnType.UNDEFINED);
            check(failures, unhandled, "appendOpcode", entry, () -> (int) qwp.invoke(null, type) == ColumnType.UNDEFINED);
        }
        Assert.assertEquals("", failures.toString());
    }

    private static void check(StringBuilder failures, Map<String, Set<String>> unhandled, String function, TypeConformanceTypes.Entry entry, UnhandledCheck check) {
        boolean isUnhandled;
        try {
            isUnhandled = check.isUnhandled();
        } catch (InvocationTargetException e) {
            isUnhandled = true;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        if (isUnhandled != unhandled.get(function).contains(entry.label)) {
            failures.append(function).append(": ").append(entry.label)
                    .append(isUnhandled ? " is not handled" : " is handled but listed as unhandled")
                    .append('\n');
        }
    }

    private static Method method(Class<?> clazz, String name, Class<?>... parameterTypes) throws NoSuchMethodException {
        final Method method = clazz.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private static Set<String> with(Set<String> a, Set<String> b) {
        final Set<String> all = new HashSet<>(a);
        all.addAll(b);
        return all;
    }

    @FunctionalInterface
    private interface UnhandledCheck {
        boolean isUnhandled() throws Exception;
    }
}
