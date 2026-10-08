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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Builds every registered function over every argument type its signature accepts, as columns, constants,
 * typed and untyped NULLs and bind variables, and over the constant values factories branch on. Fails when
 * {@code getResultType} throws, when the {@code -ea} check in {@code FunctionParser.createFunction} finds a
 * built type other than the declared one, when a call the binder left unconstructed builds into a function
 * with other semantics than the binder recorded, or when compiling a call throws anything other than a
 * SqlException, CairoException or ImplicitCastException that is not a known crash master shares.
 */
public class FunctionResultTypeContractTest extends AbstractCairoTest {
    private static final int MAX_BASE_COMBINATIONS = 3_000;
    private static final int MAX_VALUE_COMBINATIONS = 300;
    private static final int MAX_VAR_ARGS = 3;
    private static final HashSet<String> knownMasterCrashes = new HashSet<>();
    private static final ObjList<Form> forms = new ObjList<>();
    private final HashSet<String> crashes = new HashSet<>();
    private final ObjList<String> failures = new ObjList<>();
    private final Rnd rnd = new Rnd(42, 4242);
    private final HashSet<String> seen = new HashSet<>();
    private int compiled;
    private String functionName;
    private int rejected;

    @Test
    public void testBuiltFunctionTypeMatchesDeclaredResultType() throws Exception {
        Assert.assertTrue("the contract check needs -ea", FunctionParser.class.desiredAssertionStatus());
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        bo BOOLEAN, b BYTE, sh SHORT, c CHAR, i INT, l LONG, dt DATE, tn TIMESTAMP_NS, f FLOAT, d DOUBLE,
                        s STRING, sym SYMBOL, l256 LONG256, g1 GEOHASH(1c), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c),
                        bin BINARY, u UUID, ip IPV4, vc VARCHAR, d8 DECIMAL(2,1), d16 DECIMAL(4,1), d32 DECIMAL(9,2),
                        d64 DECIMAL(18,2), d128 DECIMAL(38,2), d256 DECIMAL(76,2), da DOUBLE[], da2 DOUBLE[][], ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final List<FunctionFactoryDescriptor> descriptors = new ArrayList<>();
            engine.getFunctionFactoryCache().getFactories().forEach((name, overloads) -> {
                for (int i = 0, n = overloads.size(); i < n; i++) {
                    descriptors.add(overloads.getQuick(i));
                }
            });
            for (int i = 0, n = descriptors.size(); i < n; i++) {
                checkDescriptor(descriptors.get(i));
            }
            final StringBuilder sb = new StringBuilder();
            for (int i = 0, n = failures.size(); i < n; i++) {
                sb.append(failures.getQuick(i)).append('\n');
            }
            Assert.assertTrue("compiled too few calls: " + compiled + ", rejected: " + rejected, compiled > 10_000);
            Assert.assertEquals("", sb.toString());
        });
    }

    private static boolean accepts(int sigType, Form form) {
        if (form.tag == ColumnType.NULL) {
            return true;
        }
        if (FunctionFactoryDescriptor.isConstant(sigType) && !form.isConstant) {
            return false;
        }
        final short sigTag = FunctionFactoryDescriptor.toTypeTag(sigType);
        if (FunctionFactoryDescriptor.isArray(sigType)) {
            return form.tag == ColumnType.ARRAY;
        }
        return switch (sigTag) {
            case ColumnType.VAR_ARG -> true;
            case ColumnType.GEOHASH -> ColumnType.isGeoHash(form.tag);
            case ColumnType.DECIMAL -> ColumnType.isDecimal(form.tag);
            case ColumnType.REGCLASS, ColumnType.REGPROCEDURE -> form.tag == ColumnType.STRING && form.isConstant;
            case ColumnType.NULL, ColumnType.ARRAY_STRING, ColumnType.CURSOR, ColumnType.RECORD -> false;
            default -> form.tag == sigTag || ColumnType.overloadDistance(form.tag, sigTag) < ColumnType.OVERLOAD_NONE;
        };
    }

    private static void addColumn(String name, int type) {
        forms.add(new Form(name, ColumnType.tagOf(type), false, false));
    }

    private static void addConstant(String sql, int type) {
        forms.add(new Form(sql, ColumnType.tagOf(type), true, false));
    }

    private static void addValue(String sql, int type) {
        forms.add(new Form(sql, ColumnType.tagOf(type), true, true));
    }

    private static String crashKey(String name, Throwable e) {
        final StackTraceElement[] frames = e.getStackTrace();
        final String frame = frames.length > 0 ? frames[0].getClassName().substring(frames[0].getClassName().lastIndexOf('.') + 1) + '.' + frames[0].getMethodName() : "";
        return name + ' ' + e.getClass().getSimpleName() + " at " + frame;
    }

    private static boolean isContractViolation(Throwable e) {
        final String message = e.getMessage();
        if (message != null && (message.startsWith("function type differs") || message.startsWith("bound function semantics have changed"))) {
            return true;
        }
        for (StackTraceElement frame : e.getStackTrace()) {
            if (frame.getMethodName().equals("getResultType")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isIdentifier(String name) {
        final char c = name.charAt(0);
        return Character.isLetter(c) || c == '_';
    }

    private static String render(String name, ObjList<String> args, boolean isWindow) {
        final String lowerName = name.toLowerCase();
        final int minArgs = switch (lowerName) {
            case "cast", "in", "within", "[]", "and", "or", "like", "ilike" -> 2;
            case "between" -> 3;
            default -> 1;
        };
        if (args.size() < minArgs) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        switch (lowerName) {
            case "cast" -> sb.append('(').append(args.getQuick(0)).append(")::").append(args.getQuick(1));
            case "in", "within" -> {
                sb.append('(').append(args.getQuick(0)).append(") ").append(name).append(" (");
                for (int i = 1, n = args.size(); i < n; i++) {
                    sb.append(i > 1 ? ", " : "").append(args.getQuick(i));
                }
                sb.append(')');
            }
            case "between" -> sb.append('(').append(args.getQuick(0)).append(") BETWEEN (").append(args.getQuick(1))
                    .append(") AND (").append(args.getQuick(2)).append(')');
            case "[]" -> {
                sb.append('(').append(args.getQuick(0)).append(")[");
                for (int i = 1, n = args.size(); i < n; i++) {
                    sb.append(i > 1 ? ", " : "").append(args.getQuick(i));
                }
                sb.append(']');
            }
            case "not" -> sb.append("NOT (").append(args.getQuick(0)).append(')');
            case "and", "or", "like", "ilike" -> sb.append('(').append(args.getQuick(0)).append(") ").append(name)
                    .append(" (").append(args.getQuick(1)).append(')');
            default -> {
                if (isIdentifier(name)) {
                    sb.append(name).append('(');
                    for (int i = 0, n = args.size(); i < n; i++) {
                        sb.append(i > 0 ? ", " : "").append(args.getQuick(i));
                    }
                    sb.append(')');
                    if (isWindow) {
                        sb.append(" OVER (ORDER BY ts)");
                    }
                } else if (args.size() == 1) {
                    sb.append(name).append(" (").append(args.getQuick(0)).append(')');
                } else if (args.size() == 2) {
                    sb.append('(').append(args.getQuick(0)).append(") ").append(name).append(" (").append(args.getQuick(1)).append(')');
                } else {
                    return null;
                }
            }
        }
        return sb.toString();
    }

    private static ObjList<String> typeNames(int sigType) {
        final ObjList<String> names = new ObjList<>();
        if (FunctionFactoryDescriptor.isArray(sigType)) {
            names.add("double[]");
            names.add("double[][]");
            return names;
        }
        switch (FunctionFactoryDescriptor.toTypeTag(sigType)) {
            case ColumnType.TIMESTAMP -> {
                names.add("timestamp");
                names.add("timestamp_ns");
            }
            case ColumnType.GEOHASH -> {
                names.add("geohash(1c)");
                names.add("geohash(2c)");
                names.add("geohash(4c)");
                names.add("geohash(8c)");
                names.add("geohash(3b)");
            }
            case ColumnType.DECIMAL -> {
                names.add("decimal(2,1)");
                names.add("decimal(4,1)");
                names.add("decimal(9,2)");
                names.add("decimal(18,2)");
                names.add("decimal(38,2)");
                names.add("decimal(76,2)");
            }
            default -> names.add(ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(sigType)));
        }
        return names;
    }

    private void addCombinations(String name, boolean isWindow, ObjList<ObjList<String>> base, ObjList<ObjList<String>> values) {
        long product = 1;
        for (int i = 0, n = base.size(); i < n && product <= MAX_BASE_COMBINATIONS; i++) {
            product *= base.getQuick(i).size();
        }
        if (product == 0) {
            return;
        }
        final ObjList<String> args = new ObjList<>();
        if (product <= MAX_BASE_COMBINATIONS) {
            enumerate(name, isWindow, base, 0, args);
        } else {
            sample(name, isWindow, base, MAX_BASE_COMBINATIONS);
        }
        final ObjList<ObjList<String>> pinned = new ObjList<>();
        for (int i = 0, n = values.size(); i < n; i++) {
            final ObjList<String> positionValues = values.getQuick(i);
            for (int j = 0, m = positionValues.size(); j < m; j++) {
                pinned.clear();
                pinned.addAll(base);
                final ObjList<String> one = new ObjList<>();
                one.add(positionValues.getQuick(j));
                pinned.setQuick(i, one);
                long pinnedProduct = 1;
                for (int k = 0, p = pinned.size(); k < p && pinnedProduct <= MAX_VALUE_COMBINATIONS; k++) {
                    pinnedProduct *= pinned.getQuick(k).size();
                }
                if (pinnedProduct <= MAX_VALUE_COMBINATIONS) {
                    args.clear();
                    enumerate(name, isWindow, pinned, 0, args);
                } else {
                    sample(name, isWindow, pinned, MAX_VALUE_COMBINATIONS);
                }
            }
        }
    }

    private void checkCall(String call) {
        if (call == null || !seen.add(call)) {
            return;
        }
        final String sql = "SELECT " + call + " FROM t";
        bindVariableService.clear();
        try (RecordCursorFactory ignore = select(sql)) {
            compiled++;
        } catch (SqlException | CairoException | ImplicitCastException e) {
            rejected++;
        } catch (Throwable e) {
            final String crash = crashKey(functionName, e);
            if (!isContractViolation(e) && knownMasterCrashes.contains(crash)) {
                rejected++;
            } else if (crashes.add(crash)) {
                failures.add(crash + " :: " + sql + " -> " + e);
            }
        }
    }

    private void checkCase(boolean isSwitch) {
        final ObjList<String> values = valueForms();
        final ObjList<ObjList<String>> base = new ObjList<>();
        final ObjList<String> head = new ObjList<>();
        if (isSwitch) {
            for (int i = 0, n = forms.size(); i < n; i++) {
                final Form form = forms.getQuick(i);
                if (!form.isValue) {
                    head.add(form.isConstant ? form.sql + " WHEN " + form.sql : form.sql + " WHEN null");
                }
            }
        } else {
            head.add("WHEN bo");
        }
        base.add(head);
        base.add(values);
        base.add(values);
        final ObjList<String> args = new ObjList<>();
        for (int k = 0; k < 2 * MAX_BASE_COMBINATIONS; k++) {
            args.clear();
            for (int i = 0, n = base.size(); i < n; i++) {
                final ObjList<String> options = base.getQuick(i);
                args.add(options.getQuick(rnd.nextPositiveInt() % options.size()));
            }
            checkCall("CASE " + args.getQuick(0) + " THEN " + args.getQuick(1) + " ELSE " + args.getQuick(2) + " END");
            checkCall("CASE " + args.getQuick(0) + " THEN " + args.getQuick(1) + " END");
        }
    }

    private void checkDescriptor(FunctionFactoryDescriptor descriptor) {
        final FunctionFactory factory = descriptor.getFactory();
        if (factory.isCursor()) {
            return;
        }
        final String name = descriptor.getName();
        functionName = name;
        if (name.equalsIgnoreCase("case")) {
            checkCase(false);
            return;
        }
        if (name.equalsIgnoreCase("switch")) {
            checkCase(true);
            return;
        }
        final boolean isCast = name.equalsIgnoreCase("cast");
        final ObjList<ObjList<String>> base = new ObjList<>();
        final ObjList<ObjList<String>> values = new ObjList<>();
        int varArgIndex = -1;
        int bindIndex = 1;
        for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
            final int sigType = descriptor.getArgTypeWithFlags(i);
            if (FunctionFactoryDescriptor.toTypeTag(sigType) == ColumnType.VAR_ARG) {
                varArgIndex = i;
                continue;
            }
            if (isCast && i == 1) {
                base.add(typeNames(sigType));
                values.add(new ObjList<>());
                continue;
            }
            final ObjList<String> positionBase = new ObjList<>();
            final ObjList<String> positionValues = new ObjList<>();
            final short sigTag = FunctionFactoryDescriptor.toTypeTag(sigType);
            for (int j = 0, m = forms.size(); j < m; j++) {
                final Form form = forms.getQuick(j);
                if (accepts(sigType, form)) {
                    if (!form.isValue) {
                        positionBase.add(form.sql);
                    } else if (form.tag == sigTag || (sigTag == ColumnType.STRING && form.tag == ColumnType.VARCHAR)) {
                        positionValues.add(form.sql);
                    }
                }
            }
            if (!isCast) {
                positionBase.add("$" + bindIndex++);
            }
            base.add(positionBase);
            values.add(positionValues);
        }
        if (varArgIndex == -1) {
            addCombinations(name, factory.isWindow(), base, values);
            return;
        }
        final ObjList<String> varArgForms = valueForms();
        for (int k = 0; k <= MAX_VAR_ARGS; k++) {
            final ObjList<ObjList<String>> expandedBase = new ObjList<>();
            final ObjList<ObjList<String>> expandedValues = new ObjList<>();
            expandedBase.addAll(base);
            expandedValues.addAll(values);
            for (int j = 0; j < k; j++) {
                expandedBase.add(varArgForms);
                expandedValues.add(new ObjList<>());
            }
            addCombinations(name, factory.isWindow(), expandedBase, expandedValues);
        }
    }

    private void enumerate(String name, boolean isWindow, ObjList<ObjList<String>> base, int index, ObjList<String> args) {
        if (index == base.size()) {
            checkCall(render(name, args, isWindow));
            return;
        }
        final ObjList<String> options = base.getQuick(index);
        for (int i = 0, n = options.size(); i < n; i++) {
            args.add(options.getQuick(i));
            enumerate(name, isWindow, base, index + 1, args);
            args.setPos(index);
        }
    }

    private void sample(String name, boolean isWindow, ObjList<ObjList<String>> base, int count) {
        final ObjList<String> args = new ObjList<>();
        for (int k = 0; k < count; k++) {
            args.clear();
            for (int i = 0, n = base.size(); i < n; i++) {
                final ObjList<String> options = base.getQuick(i);
                args.add(options.getQuick(rnd.nextPositiveInt() % options.size()));
            }
            checkCall(render(name, args, isWindow));
        }
    }

    private ObjList<String> valueForms() {
        final ObjList<String> values = new ObjList<>();
        for (int i = 0, n = forms.size(); i < n; i++) {
            final Form form = forms.getQuick(i);
            if (!form.isValue) {
                values.add(form.sql);
            }
        }
        return values;
    }

    private record Form(String sql, short tag, boolean isConstant, boolean isValue) {
    }

    static {
        // Each entry is a crash master raises with the same exception class for the same SQL (probe-confirmed);
        // the key is the function name, the exception class and the throwing frame.
        // IPv4 bitwise/arithmetic operators read a SYMBOL operand as IPv4, which SymbolFunction does not support.
        knownMasterCrashes.add("& UnsupportedOperationException at SymbolFunction.getIPv4");
        knownMasterCrashes.add("- UnsupportedOperationException at SymbolFunction.getIPv4");
        knownMasterCrashes.add("| UnsupportedOperationException at SymbolFunction.getIPv4");
        // VARCHAR string functions fold a constant IPv4 or SYMBOL argument through getVarcharA/getVarcharSize.
        knownMasterCrashes.add("lpad UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("rpad UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("position UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("strpos UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("split_part UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("starts_with UnsupportedOperationException at IPv4Function.getVarcharA");
        knownMasterCrashes.add("length_bytes UnsupportedOperationException at IPv4Function.getVarcharSize");
        knownMasterCrashes.add("length_bytes UnsupportedOperationException at SymbolFunction.getVarcharSize");
        // nullif over IPv4 reads a CHAR or SYMBOL operand as IPv4, or an IPv4 operand as VARCHAR.
        knownMasterCrashes.add("nullif UnsupportedOperationException at CharFunction.getIPv4");
        knownMasterCrashes.add("nullif UnsupportedOperationException at SymbolFunction.getIPv4");
        knownMasterCrashes.add("nullif UnsupportedOperationException at IPv4Function.getVarcharA");
        // A timestamp IN list holding an interval reads its first list element as an interval.
        knownMasterCrashes.add("in UnsupportedOperationException at DateFunction.getInterval");
        knownMasterCrashes.add("in UnsupportedOperationException at IntFunction.getInterval");
        knownMasterCrashes.add("in UnsupportedOperationException at LongFunction.getInterval");
        knownMasterCrashes.add("in UnsupportedOperationException at StrFunction.getInterval");
        knownMasterCrashes.add("in UnsupportedOperationException at TimestampFunction.getInterval");
        // greatest/least over mixed temporal and integer constants read the integer constant as a timestamp.
        knownMasterCrashes.add("greatest UnsupportedOperationException at ByteFunction.getTimestamp");
        knownMasterCrashes.add("least UnsupportedOperationException at ByteFunction.getTimestamp");
        knownMasterCrashes.add("least UnsupportedOperationException at ShortFunction.getTimestamp");
        // l2price reads non-numeric var-arg constants as DOUBLE.
        knownMasterCrashes.add("l2price UnsupportedOperationException at AbstractGeoHashFunction.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at DecimalFunction.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at IPv4Function.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at IntervalFunction.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at Long256Function.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at SymbolFunction.getDouble");
        knownMasterCrashes.add("l2price UnsupportedOperationException at UuidFunction.getDouble");
        // matmul over constant 1-D arrays asks for a second dimension.
        knownMasterCrashes.add("matmul AssertionError at ArrayView.getDimLen");
        // Casting a NULL DOUBLE[] to DOUBLE[][] encodes a NULL element type.
        knownMasterCrashes.add("cast AssertionError at ColumnType.encodeArrayType");
        // Slicing a constant array by a timestamp interval overflows the int index.
        knownMasterCrashes.add("[] AssertionError at DoubleArrayAccessFunctionFactory$SliceDoubleArrayFunction.toIndex");

        addConstant("null", ColumnType.NULL);

        addColumn("bo", ColumnType.BOOLEAN);
        addColumn("b", ColumnType.BYTE);
        addColumn("sh", ColumnType.SHORT);
        addColumn("c", ColumnType.CHAR);
        addColumn("i", ColumnType.INT);
        addColumn("l", ColumnType.LONG);
        addColumn("dt", ColumnType.DATE);
        addColumn("ts", ColumnType.TIMESTAMP);
        addColumn("tn", ColumnType.TIMESTAMP_NANO);
        addColumn("f", ColumnType.FLOAT);
        addColumn("d", ColumnType.DOUBLE);
        addColumn("s", ColumnType.STRING);
        addColumn("sym", ColumnType.SYMBOL);
        addColumn("l256", ColumnType.LONG256);
        addColumn("g1", ColumnType.getGeoHashTypeWithBits(5));
        addColumn("g2", ColumnType.getGeoHashTypeWithBits(10));
        addColumn("g4", ColumnType.getGeoHashTypeWithBits(20));
        addColumn("g8", ColumnType.getGeoHashTypeWithBits(40));
        addColumn("bin", ColumnType.BINARY);
        addColumn("u", ColumnType.UUID);
        addColumn("ip", ColumnType.IPv4);
        addColumn("vc", ColumnType.VARCHAR);
        addColumn("d8", ColumnType.getDecimalType(2, 1));
        addColumn("d16", ColumnType.getDecimalType(4, 1));
        addColumn("d32", ColumnType.getDecimalType(9, 2));
        addColumn("d64", ColumnType.getDecimalType(18, 2));
        addColumn("d128", ColumnType.getDecimalType(38, 2));
        addColumn("d256", ColumnType.getDecimalType(76, 2));
        addColumn("da", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        addColumn("da2", ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));

        addConstant("true", ColumnType.BOOLEAN);
        addConstant("1::byte", ColumnType.BYTE);
        addConstant("1::short", ColumnType.SHORT);
        addConstant("'a'", ColumnType.CHAR);
        addConstant("1", ColumnType.INT);
        addConstant("1::long", ColumnType.LONG);
        addConstant("'2020-01-01'::date", ColumnType.DATE);
        addConstant("'2020-01-01T00:00:00.000000Z'::timestamp", ColumnType.TIMESTAMP);
        addConstant("'2020-01-01T00:00:00.000000000Z'::timestamp_ns", ColumnType.TIMESTAMP_NANO);
        addConstant("1.5::float", ColumnType.FLOAT);
        addConstant("1.5", ColumnType.DOUBLE);
        addConstant("'abc'", ColumnType.STRING);
        addConstant("'abc'::symbol", ColumnType.SYMBOL);
        addConstant("to_long256(1, 2, 3, 4)", ColumnType.LONG256);
        addConstant("to_long128(1, 2)", ColumnType.LONG128);
        addConstant("#s", ColumnType.getGeoHashTypeWithBits(5));
        addConstant("#sp", ColumnType.getGeoHashTypeWithBits(10));
        addConstant("#sp05", ColumnType.getGeoHashTypeWithBits(20));
        addConstant("#sp052w92", ColumnType.getGeoHashTypeWithBits(40));
        addConstant("'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", ColumnType.UUID);
        addConstant("'1.2.3.4'::ipv4", ColumnType.IPv4);
        addConstant("'abc'::varchar", ColumnType.VARCHAR);
        addConstant("1.5::decimal(2,1)", ColumnType.getDecimalType(2, 1));
        addConstant("1.5::decimal(4,1)", ColumnType.getDecimalType(4, 1));
        addConstant("1.5::decimal(9,2)", ColumnType.getDecimalType(9, 2));
        addConstant("1.5::decimal(18,2)", ColumnType.getDecimalType(18, 2));
        addConstant("1.5::decimal(38,2)", ColumnType.getDecimalType(38, 2));
        addConstant("1.5::decimal(76,2)", ColumnType.getDecimalType(76, 2));
        addConstant("ARRAY[1.0, 2.0]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        addConstant("ARRAY[[1.0, 2.0]]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
        addConstant("interval('2020-01-01T00:00:00.000000Z'::timestamp, '2020-01-02T00:00:00.000000Z'::timestamp)", ColumnType.INTERVAL);

        addConstant("null::boolean", ColumnType.BOOLEAN);
        addConstant("null::byte", ColumnType.BYTE);
        addConstant("null::short", ColumnType.SHORT);
        addConstant("null::char", ColumnType.CHAR);
        addConstant("null::int", ColumnType.INT);
        addConstant("null::long", ColumnType.LONG);
        addConstant("null::date", ColumnType.DATE);
        addConstant("null::timestamp", ColumnType.TIMESTAMP);
        addConstant("null::timestamp_ns", ColumnType.TIMESTAMP_NANO);
        addConstant("null::float", ColumnType.FLOAT);
        addConstant("null::double", ColumnType.DOUBLE);
        addConstant("null::string", ColumnType.STRING);
        addConstant("null::symbol", ColumnType.SYMBOL);
        addConstant("null::long256", ColumnType.LONG256);
        addConstant("null::geohash(1c)", ColumnType.getGeoHashTypeWithBits(5));
        addConstant("null::geohash(8c)", ColumnType.getGeoHashTypeWithBits(40));
        addConstant("null::uuid", ColumnType.UUID);
        addConstant("null::ipv4", ColumnType.IPv4);
        addConstant("null::varchar", ColumnType.VARCHAR);
        addConstant("null::decimal(9,2)", ColumnType.getDecimalType(9, 2));
        addConstant("null::decimal(38,2)", ColumnType.getDecimalType(38, 2));
        addConstant("null::double[]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        addConstant("null::interval", ColumnType.INTERVAL);

        addValue("false", ColumnType.BOOLEAN);
        addValue("0::byte", ColumnType.BYTE);
        addValue("-1::byte", ColumnType.BYTE);
        addValue("0::short", ColumnType.SHORT);
        for (String unit : new String[]{"u", "U", "n", "T", "s", "m", "h", "d", "w", "M", "y", "q", " "}) {
            addValue("'" + unit + "'", ColumnType.CHAR);
        }
        for (String value : new String[]{"0", "-1", "2", "3", "10", "100"}) {
            addValue(value, ColumnType.INT);
        }
        addValue("0::long", ColumnType.LONG);
        addValue("-1::long", ColumnType.LONG);
        addValue("100::long", ColumnType.LONG);
        addValue("0::date", ColumnType.DATE);
        addValue("0::timestamp", ColumnType.TIMESTAMP);
        addValue("0::timestamp_ns", ColumnType.TIMESTAMP_NANO);
        addValue("0::float", ColumnType.FLOAT);
        addValue("0.0", ColumnType.DOUBLE);
        addValue("-1.5", ColumnType.DOUBLE);
        addValue("2.0", ColumnType.DOUBLE);
        for (String value : new String[]{
                "", "a", "microsecond", "microseconds", "nanosecond", "nanoseconds", "millisecond", "milliseconds",
                "second", "minute", "hour", "day", "week", "month", "quarter", "year", "decade", "century",
                "millennium", "epoch", "dow", "doy", "isodow", "isoyear", "u", "U", "n", "T", "s", "m", "h", "d",
                "w", "M", "y", "1d", "1h", "UTC", "Europe/London", "+01:00", "yyyy-MM-dd", "1.2.3.4", "2020-01-01",
                "2020-01-01T00:00:00.000000Z", "%a%", "1", "NaN", "true", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "0x01"
        }) {
            addValue("'" + value + "'", ColumnType.STRING);
        }
        for (String value : new String[]{"", "a", "microsecond", "nanosecond", "day", "1.2.3.4", "%a%", "UTC"}) {
            addValue("'" + value + "'::varchar", ColumnType.VARCHAR);
        }
        addValue("''::symbol", ColumnType.SYMBOL);
        addValue("'0.0.0.0'::ipv4", ColumnType.IPv4);
        addValue("0::decimal(9,2)", ColumnType.getDecimalType(9, 2));
        addValue("ARRAY[]::double[]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
    }
}
