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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.bool.AllNotEqStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.AllNotEqVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.bool.InSymbolCursorFunctionFactory;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.functions.eq.ContainsEqIPv4Utils;
import io.questdb.griffin.engine.functions.eq.ContainsIPv4Utils;
import io.questdb.griffin.engine.functions.eq.EqBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqByteFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqGeoHashGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntervalFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqShortFunctionFactory;
import io.questdb.griffin.engine.functions.regex.MatchStrFunctionFactory;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.NotMatchStrFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A factory that folds a comparison to a constant owns the operands it drops and closes each exactly once.
 */
public class FoldedComparisonOwnershipTest extends AbstractCairoTest {
    private static final int GEO_5 = ColumnType.getGeoHashTypeWithBits(5);
    private static final int GEO_6 = ColumnType.getGeoHashTypeWithBits(6);

    @Test
    public void testFactoriesCloseDroppedOperands() throws Exception {
        assertMemoryLeak(() -> {
            assertFolded(new EqBooleanFunctionFactory(), new CountingBoolean(), NullConstant.NULL);
            assertFolded(new EqShortFunctionFactory(), new CountingShort(), NullConstant.NULL);
            assertFolded(new EqByteFunctionFactory(), new CountingByte(), NullConstant.NULL);
            assertFolded(new EqIntervalFunctionFactory(), new CountingInterval(), IntervalConstant.TIMESTAMP_NANO_NULL);
            assertFolded(new MatchStrFunctionFactory(), new CountingStr(), StrConstant.NULL);
            assertFolded(new NotMatchStrFunctionFactory(), new CountingStr(), StrConstant.NULL);
            assertFolded(new MatchSymbolFunctionFactory(), new CountingSymbol(true), StrConstant.NULL);
            assertFolded(new MatchSymbolFunctionFactory(), new CountingSymbol(false), StrConstant.NULL);
            assertFolded(new InIPv4FunctionFactory(), new CountingIPv4());
            assertFolded(new InCharFunctionFactory(), new CountingChar());

            final EqGeoHashGeoHashFunctionFactory geoHash = new EqGeoHashGeoHashFunctionFactory();
            CountingGeoByte left = new CountingGeoByte(GEO_6);
            assertResult(geoHash, left, new ObjList<>(new GeoByteConstant((byte) 1, GEO_5), left));
            left = new CountingGeoByte(GEO_6);
            assertResult(geoHash, left, new ObjList<>(left, new GeoByteConstant((byte) 1, GEO_5)));
            left = new CountingGeoByte(GEO_5);
            final CountingGeoByte right = new CountingGeoByte(GEO_6);
            assertResult(geoHash, left, new ObjList<>(left, right));
            Assert.assertEquals(1, right.closeCount);

            CountingStr value = new CountingStr();
            CountingStrArray array = new CountingStrArray();
            assertResult(new AllNotEqStrFunctionFactory(), value, new ObjList<>(value, array));
            Assert.assertEquals(1, array.getCloseCount());
            value = new CountingStr();
            array = new CountingStrArray();
            assertResult(new AllNotEqVarcharFunctionFactory(), value, new ObjList<>(value, array));
            Assert.assertEquals(1, array.getCloseCount());

            final CountingNullCursor cursor = new CountingNullCursor();
            assertResult(new InSymbolCursorFunctionFactory(), cursor, new ObjList<>(NullConstant.NULL, cursor));
        });
    }

    @Test
    public void testIPv4ContainsClosesDroppedOperand() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 4; i++) {
                final CountingIPv4 ip = new CountingIPv4();
                final Function result = switch (i) {
                    case 0 -> ContainsIPv4Utils.containsIPv4Str(ip, StrConstant.NULL, 0);
                    case 1 -> ContainsIPv4Utils.containsIPv4Varchar(ip, VarcharConstant.NULL, 0);
                    case 2 -> ContainsEqIPv4Utils.containsIPv4Str(ip, StrConstant.NULL, 0);
                    default -> ContainsEqIPv4Utils.containsIPv4Varchar(ip, VarcharConstant.NULL, 0);
                };
                Assert.assertSame(BooleanConstant.FALSE, result);
                Assert.assertEquals(1, ip.closeCount);
            }
        });
    }

    private static void assertFolded(FunctionFactory factory, Counting dropped, Function... others) throws Exception {
        final ObjList<Function> args = new ObjList<>();
        args.add(dropped);
        for (Function other : others) {
            args.add(other);
        }
        assertResult(factory, dropped, args);
    }

    private static void assertResult(FunctionFactory factory, Counting dropped, ObjList<Function> args) throws Exception {
        final IntList positions = new IntList();
        for (int i = 0, n = args.size(); i < n; i++) {
            positions.add(i);
        }
        final Function result = factory.newInstance(0, args, positions, configuration, sqlExecutionContext);
        Assert.assertTrue(result instanceof BooleanConstant);
        Assert.assertEquals(1, dropped.getCloseCount());
    }

    private interface Counting extends Function {
        int getCloseCount();
    }

    private static class CountingBoolean extends BooleanFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public boolean getBool(Record rec) {
            throw new AssertionError();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }
    }

    private static class CountingByte extends ByteFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public byte getByte(Record rec) {
            throw new AssertionError();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }
    }

    private static class CountingChar extends CharFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public char getChar(Record rec) {
            throw new AssertionError();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }
    }

    private static class CountingGeoByte extends GeoByteFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        private CountingGeoByte(int type) {
            super(type);
        }

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public byte getGeoByte(Record rec) {
            throw new AssertionError();
        }
    }

    private static class CountingIPv4 extends IPv4Function implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public int getIPv4(Record rec) {
            throw new AssertionError();
        }
    }

    private static class CountingInterval extends IntervalFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        private CountingInterval() {
            super(ColumnType.INTERVAL_TIMESTAMP_MICRO);
        }

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public Interval getInterval(Record rec) {
            throw new AssertionError();
        }
    }

    private static class CountingNullCursor extends BooleanFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public boolean getBool(Record rec) {
            throw new AssertionError();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public boolean isNullConstant() {
            return true;
        }
    }

    private static class CountingShort extends ShortFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public short getShort(Record rec) {
            throw new AssertionError();
        }
    }

    private static class CountingStr extends StrFunction implements Counting {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            throw new AssertionError();
        }

        @Override
        public CharSequence getStrB(Record rec) {
            throw new AssertionError();
        }
    }

    private static class CountingStrArray extends CountingStr implements FunctionExtension {
        @Override
        public FunctionExtension extendedOps() {
            return this;
        }

        @Override
        public int getArrayLength() {
            return 0;
        }

        @Override
        public Record getRecord(Record rec) {
            throw new AssertionError();
        }

        @Override
        public CharSequence getStrA(Record rec, int arrayIndex) {
            throw new AssertionError();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new AssertionError();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new AssertionError();
        }

        @Override
        public boolean isConstant() {
            return true;
        }
    }

    private static class CountingSymbol extends SymbolFunction implements Counting {
        private final boolean isStatic;
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        private CountingSymbol(boolean isStatic) {
            this.isStatic = isStatic;
        }

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public int getInt(Record rec) {
            throw new AssertionError();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            throw new AssertionError();
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            throw new AssertionError();
        }

        @Override
        public boolean isSymbolTableStatic() {
            return isStatic;
        }

        @Override
        public CharSequence valueBOf(int key) {
            throw new AssertionError();
        }

        @Override
        public CharSequence valueOf(int key) {
            throw new AssertionError();
        }
    }
}
