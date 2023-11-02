/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.eq.EqGeoHashGeoHashFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EqGeoHashGeoHashFunctionFactoryTest extends AbstractCairoTest {

    private static final ObjList<Function> args = new ObjList<>(2);
    private static final EqGeoHashGeoHashFunctionFactory factory = new EqGeoHashGeoHashFunctionFactory();
    private final Function geoByteNullNonConstFunction =
            createGeoValueFunction(ColumnType.getGeoHashTypeWithBits(1), GeoHashes.BYTE_NULL, false);

    @Before
    public void setUp3() {
        args.clear();
    }

    @Test
    public void testCastGeoHashToNullEqNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "column\n" +
                        "true\n", "select cast(null as geohash(1c)) = null"
        ));
    }

    @Test
    public void testConstConst1() {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.clear();
            args.add(Constants.getGeoHashConstant(0, b));
            args.add(Constants.getGeoHashConstant(0, b));
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testConstConst2() {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.clear();
            args.add(Constants.getGeoHashConstant(0, b));
            args.add(Constants.getGeoHashConstant(1, b));
            createEqFunctionAndAssert(false);
        }
    }

    @Test
    public void testConstConst3() {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.clear();
            args.add(Constants.getGeoHashConstant(1, b));
            args.add(Constants.getGeoHashConstant(1, ((b + 1) % 60) + 1));
            createEqFunctionAndAssert(false);
        }
    }

    @Test
    public void testConstHalfConst1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table geohash as (" +
                    "select " +
                    "    cast('sp052w92p1' as GeOhAsH(50b)) geohash from long_sequence(1)" +
                    ")");
            assertSql(
                    "geohash\n" +
                            "sp052w92p1\n", "geohash where cast('sp052w92p1p' as gEoHaSh(10c)) = geohash"
            );
        });
    }

    @Test
    public void testDifferentTypeAndValue() {
        createEqFunctionAndAssert(
                0, ColumnType.getGeoHashTypeWithBits(31),
                10, ColumnType.getGeoHashTypeWithBits(12),
                false,
                true);
    }

    @Test
    public void testDifferentTypeSameValue() {
        createEqFunctionAndAssert(
                10, ColumnType.getGeoHashTypeWithBits(31),
                10, ColumnType.getGeoHashTypeWithBits(30),
                false,
                true
        );
    }

    @Test
    public void testEq() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(11) b" +
                    " from long_sequence(5000)" +
                    ")");
            assertSql(
                    "a\tb\n" +
                            "11010001011\t11010001011\n", "x where a = b"
            );
        });
    }

    @Test
    public void testNotEq() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(13) b" +
                    " from long_sequence(8)" +
                    ")");
            assertSql(
                    "a\tb\n" +
                            "01001110110\t0010000110110\n" +
                            "10001101001\t1111101110110\n" +
                            "10000101010\t1110010000001\n" +
                            "11000000101\t0000101011100\n" +
                            "10011100111\t0011100001011\n" +
                            "01110110001\t1011000100110\n" +
                            "11010111111\t1000110001001\n" +
                            "10010110001\t0101011010111\n", "x where a != b"
            );
        });
    }

    @Test
    public void testNull1() {
        args.add(NullConstant.NULL);
        args.add(geoByteNullNonConstFunction);
        createEqFunctionAndAssert(false, true);
    }

    @Test
    public void testNull2() {
        args.add(geoByteNullNonConstFunction);
        args.add(NullConstant.NULL);
        createEqFunctionAndAssert(false, true);
    }

    @Test
    public void testNull3() {
        createEqFunctionAndAssert(
                GeoHashes.NULL, ColumnType.getGeoHashTypeWithBits(1),
                GeoHashes.NULL, ColumnType.getGeoHashTypeWithBits(1),
                true,
                false
        );
    }

    @Test
    public void testNull4() {
        createEqFunctionAndAssert(
                GeoHashes.NULL, ColumnType.getGeoHashTypeWithBits(12),
                GeoHashes.NULL, ColumnType.getGeoHashTypeWithBits(1),
                true,
                false
        );
    }

    @Test
    public void testNull5() {
        args.add(NullConstant.NULL);
        args.add(NullConstant.NULL);
        createEqFunctionAndAssert(true);
    }

    @Test
    public void testNull6() {
        args.add(NullConstant.NULL);
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.extendAndSet(1, nullConstantForBitsPrecision(b));
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testNull7() {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.clear();
            args.add(nullConstantForBitsPrecision(b));
            args.add(NullConstant.NULL);
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testNull8() {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            args.clear();
            args.add(nullConstantForBitsPrecision(b));
            args.add(nullConstantForBitsPrecision(((b + 1) % 60) + 1));
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testNull9() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table geohash as (" +
                    "select " +
                    "    cast(null as GeOhAsH(50b)) as geohash1, " +
                    "    cast('sp052w92' as GeOhAsH(2c)) as geohash2 " +
                    "from long_sequence(1)" +
                    ")");
            assertSql(
                    "geohash1\tgeohash2\n", "geohash where geohash1 = geohash2"
            );
        });
    }

    @Test
    public void testSameTypeAndValue() {
        createEqFunctionAndAssert(
                0, ColumnType.getGeoHashTypeWithBits(31),
                0, ColumnType.getGeoHashTypeWithBits(31),
                true,
                false
        );
    }

    @Test
    public void testSameTypeAndValueConst() {
        createEqFunctionAndAssertConst(
                ColumnType.getGeoHashTypeWithBits(31),
                ColumnType.getGeoHashTypeWithBits(31)
        );
    }

    @Test
    public void testSameTypeAndValueNonConst() {
        createEqFunctionAndAssert(
                0, ColumnType.getGeoHashTypeWithBits(31),
                0, ColumnType.getGeoHashTypeWithBits(31),
                true,
                false);
    }

    @Test
    public void testSameTypeDifferentValue() {
        createEqFunctionAndAssert(
                0, ColumnType.getGeoHashTypeWithBits(31),
                10, ColumnType.getGeoHashTypeWithBits(31),
                false,
                false);
    }

    @Test
    public void testSameTypeSameNonConstByte() {
        byte value = new Rnd().nextByte();
        createEqFunctionNonConstAndAssert(
                value, ColumnType.getGeoHashTypeWithBits(2),
                value, ColumnType.getGeoHashTypeWithBits(2),
                true
        );
    }

    @Test
    public void testSameTypeSameNonConstInt() {
        createEqFunctionNonConstAndAssert(
                (int) 1E9, ColumnType.getGeoHashTypeWithBits(30),
                (int) 1E9, ColumnType.getGeoHashTypeWithBits(30),
                true
        );
    }

    @Test
    public void testSameTypeSameNonConstLong() {
        createEqFunctionNonConstAndAssert(
                (long) 1E12, ColumnType.getGeoHashTypeWithBits(32),
                (long) 1E12 + 1, ColumnType.getGeoHashTypeWithBits(32),
                false
        );
    }

    @Test
    public void testSameTypeSameNonConstShort() {
        short value = new Rnd().nextShort();
        createEqFunctionNonConstAndAssert(
                value, ColumnType.getGeoHashTypeWithBits(15),
                value + 1, ColumnType.getGeoHashTypeWithBits(15),
                false
        );
    }

    private static Function createGeoValueFunction(int typep1, long hash1, boolean isConstant) {
        switch (ColumnType.tagOf(typep1)) {
            case ColumnType.GEOBYTE:
                return new EasyGeoByteFunction(typep1, isConstant) {
                    @Override
                    public byte getGeoByte(Record rec) {
                        return (byte) hash1;
                    }

                    @Override
                    public boolean isReadThreadSafe() {
                        return true;
                    }
                };
            case ColumnType.GEOSHORT:
                return new EasyGeoShortFunction(typep1, isConstant) {
                    @Override
                    public short getGeoShort(Record rec) {
                        return (short) hash1;
                    }

                    @Override
                    public boolean isReadThreadSafe() {
                        return true;
                    }
                };
            case ColumnType.GEOINT:
                return new EasyGeoIntFunction(typep1, isConstant) {
                    @Override
                    public int getGeoInt(Record rec) {
                        return (int) hash1;
                    }

                    @Override
                    public boolean isReadThreadSafe() {
                        return true;
                    }
                };

            case ColumnType.GEOLONG:
                return new EasyGeoLongFunction(typep1, isConstant) {
                    @Override
                    public long getGeoLong(Record rec) {
                        return hash1;
                    }

                    @Override
                    public boolean isReadThreadSafe() {
                        return true;
                    }
                };
        }
        throw new UnsupportedOperationException();
    }

    private static Function nullConstantForBitsPrecision(int bits) {
        return Constants.getNullConstant(ColumnType.getGeoHashTypeWithBits(bits));
    }

    private void createEqFunctionAndAssert(long hash1, int typep1, long hash2, int typep2, boolean expectedEq, boolean expectConst) {
        args.add(createGeoValueFunction(typep1, hash1, false));
        args.add(createGeoValueFunction(typep2, hash2, true));
        createEqFunctionAndAssert(expectConst, expectedEq);
    }

    private void createEqFunctionAndAssert(boolean expectedEq) {
        createEqFunctionAndAssert(true, expectedEq);
    }

    private void createEqFunctionAndAssert(boolean isConstant, boolean expectedEq) {
        try {
            Function func = factory.newInstance(-1, args, null, null, null);
            Assert.assertEquals(expectedEq, func.getBool(null));
            Assert.assertEquals(isConstant, func.isConstant());
            if (func instanceof NegatableBooleanFunction) {
                try {
                    NegatingFunctionFactory nf = new NegatingFunctionFactory("noteq", factory);
                    func = nf.newInstance(-1, args, null, null, null);
                    Assert.assertEquals(!expectedEq, func.getBool(null));
                } catch (SqlException e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        } catch (SqlException e) {
            Assert.fail(e.getMessage());
        }
    }

    private void createEqFunctionAndAssertConst(int typep1, int typep2) {
        args.add(createGeoValueFunction(typep1, 0, true));
        args.add(createGeoValueFunction(typep2, 0, true));
        createEqFunctionAndAssert(true, true);
    }

    private void createEqFunctionNonConstAndAssert(long hash1, int typep1, long hash2, int typep2, boolean expectedEq) {
        args.add(createGeoValueFunction(typep1, hash1, false));
        args.add(createGeoValueFunction(typep2, hash2, false));
        createEqFunctionAndAssert(false, expectedEq);
    }

    private abstract static class EasyGeoByteFunction extends GeoByteFunction {
        private final boolean isConst;

        protected EasyGeoByteFunction(int type, boolean isConst) {
            super(type);
            this.isConst = isConst;
        }

        @Override
        public boolean isConstant() {
            return isConst;
        }
    }

    private abstract static class EasyGeoIntFunction extends GeoIntFunction {
        private final boolean isConst;

        protected EasyGeoIntFunction(int type, boolean isConst) {
            super(type);
            this.isConst = isConst;
        }

        @Override
        public boolean isConstant() {
            return isConst;
        }
    }

    private abstract static class EasyGeoLongFunction extends GeoLongFunction {
        private final boolean isConst;

        protected EasyGeoLongFunction(int type, boolean isConst) {
            super(type);
            this.isConst = isConst;
        }

        @Override
        public boolean isConstant() {
            return isConst;
        }
    }

    private abstract static class EasyGeoShortFunction extends GeoShortFunction {
        private final boolean isConst;

        protected EasyGeoShortFunction(int type, boolean isConst) {
            super(type);
            this.isConst = isConst;
        }

        @Override
        public boolean isConstant() {
            return isConst;
        }
    }
}
