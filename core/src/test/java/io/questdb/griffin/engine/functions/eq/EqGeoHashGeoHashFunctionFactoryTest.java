/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EqGeoHashGeoHashFunctionFactoryTest extends AbstractGriffinTest {

    private static final EqGeoHashGeoHashFunctionFactory factory = new EqGeoHashGeoHashFunctionFactory();
    private final Function geoByteNullNonConstFunction =
            createGeoValueFunction(ColumnType.geohashWithPrecision(1), GeoHashes.BYTE_NULL, false);

    private ObjList<Function> args;

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
        args = new ObjList<>(2);
    }

    @Test
    public void testSameTypeAndValue() {
        createEqFunctionAndAssert(
                0, ColumnType.geohashWithPrecision(31),
                0, ColumnType.geohashWithPrecision(31),
                true,
                false);
    }

    @Test
    public void testSameTypeAndValueConst() {
        createEqFunctionAndAssertConst(
                0, ColumnType.geohashWithPrecision(31),
                0, ColumnType.geohashWithPrecision(31),
                true,
                true);
    }

    @Test
    public void testSameTypeAndValueNonConst() {
        createEqFunctionAndAssert(
                0, ColumnType.geohashWithPrecision(31),
                0, ColumnType.geohashWithPrecision(31),
                true,
                false);
    }

    @Test
    public void testDifferentTypeAndValue() {
        createEqFunctionAndAssert(
                0, ColumnType.geohashWithPrecision(31),
                10, ColumnType.geohashWithPrecision(12),
                false,
                true);
    }

    @Test
    public void testSameTypeDifferentValue() {
        createEqFunctionAndAssert(
                0, ColumnType.geohashWithPrecision(31),
                10, ColumnType.geohashWithPrecision(31),
                false,
                false);
    }

    @Test
    public void testDifferentTypeSameValue() {
        createEqFunctionAndAssert(
                10, ColumnType.geohashWithPrecision(31),
                10, ColumnType.geohashWithPrecision(30),
                false,
                true);
    }

    @Test
    public void testSameTypeSameNonConstInt() {
        createEqFunctionNonConstAndAssert(
                (int)1E9, ColumnType.geohashWithPrecision(30),
                (int)1E9, ColumnType.geohashWithPrecision(30),
                true,
                false);
    }

    @Test
    public void testSameTypeSameNonConstLong() {
        createEqFunctionNonConstAndAssert(
                (long) 1E12, ColumnType.geohashWithPrecision(32),
                (long) 1E12 + 1, ColumnType.geohashWithPrecision(32),
                false,
                false);
    }

    @Test
    public void testSameTypeSameNonConstShort() {
        short value = new Rnd().nextShort();
        createEqFunctionNonConstAndAssert(
                value, ColumnType.geohashWithPrecision(15),
                value + 1, ColumnType.geohashWithPrecision(15),
                false,
                false);
    }

    @Test
    public void testSameTypeSameNonConstByte() {
        byte value = new Rnd().nextByte();
        createEqFunctionNonConstAndAssert(
                value, ColumnType.geohashWithPrecision(2),
                value, ColumnType.geohashWithPrecision(2),
                true,
                false);
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
                GeoHashes.NULL, ColumnType.geohashWithPrecision(1),
                GeoHashes.NULL, ColumnType.geohashWithPrecision(1),
                true,
                false
        );
    }

    @Test
    public void testNull4() {
        createEqFunctionAndAssert(
                GeoHashes.NULL, ColumnType.geohashWithPrecision(12),
                GeoHashes.NULL, ColumnType.geohashWithPrecision(1),
                false,
                true
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
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.setPos(1);
            args.add(nullConstantForBitsPrecision(b));
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testNull7() {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.clear();
            args.add(nullConstantForBitsPrecision(b));
            args.add(NullConstant.NULL);
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testNull8() {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.clear();
            args.add(nullConstantForBitsPrecision(b));
            args.add(nullConstantForBitsPrecision(((b + 1) % 60) + 1));
            createEqFunctionAndAssert(false);
        }
    }

    @Test
    public void testNull9() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast(null as GeOhAsH(50b)) as geohash1, " +
                            "    cast('sp052w92' as GeOhAsH(2c)) as geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where geohash1 = geohash2",
                    "geohash1\tgeohash2\n"
            );
        });
    }

    @Test
    public void testConstConst1() {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.clear();
            int type = ColumnType.geohashWithPrecision(b);
            args.add(GeoHashConstant.newInstance(0, type));
            args.add(GeoHashConstant.newInstance(0, type));
            createEqFunctionAndAssert(true);
        }
    }

    @Test
    public void testConstConst2() {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.clear();
            int type = ColumnType.geohashWithPrecision(b);
            args.add(GeoHashConstant.newInstance(1, type));
            args.add(GeoHashConstant.newInstance(0, type));
            createEqFunctionAndAssert(false);
        }
    }

    @Test
    public void testConstConst3() {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            args.clear();
            args.add(GeoHashConstant.newInstance(1, ColumnType.geohashWithPrecision(b)));
            args.add(GeoHashConstant.newInstance(1, ColumnType.geohashWithPrecision(((b + 1) % 60) + 1)));
            createEqFunctionAndAssert(false);
        }
    }

    @Test
    public void testConstHalfConst1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                    "select " +
                    "    cast('sp052w92p1' as GeOhAsH(50b)) geohash from long_sequence(1)" +
                    ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where cast('sp052w92p1p' as gEoHaSh(10c)) = geohash",
                    "geohash\n" +
                            "sp052w92p1\n"
            );
        });
    }

    @Test
    public void testEq() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(11) b" +
                    " from long_sequence(5000)" +
                    ")", sqlExecutionContext);
            assertSql(
                    "x where a = b",
                    "a\tb\n" +
                            "11010001011\t11010001011\n"
            );
        });
    }

    @Test
    public void testNotEq() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(13) b" +
                    " from long_sequence(8)" +
                    ")", sqlExecutionContext);
            assertSql(
                    "x where a != b",
                    "a\tb\n"
            );
        });
    }

    private void createEqFunctionAndAssert(long hash1, int typep1, long hash2, int typep2, boolean expectedEq, boolean expectConst) {
        args.add(createGeoValueFunction(typep1, hash1, false));
        args.add(createGeoValueFunction(typep2, hash2, true));
        createEqFunctionAndAssert(expectConst, expectedEq);
    }

    private void createEqFunctionNonConstAndAssert(long hash1, int typep1, long hash2, int typep2, boolean expectedEq, boolean expectConst) {
        args.add(createGeoValueFunction(typep1, hash1, false));
        args.add(createGeoValueFunction(typep2, hash2, false));
        createEqFunctionAndAssert(expectConst, expectedEq);
    }

    private void createEqFunctionAndAssertConst(long hash1, int typep1, long hash2, int typep2, boolean expectedEq, boolean expectConst) {
        args.add(createGeoValueFunction(typep1, hash1, true));
        args.add(createGeoValueFunction(typep2, hash2, true));
        createEqFunctionAndAssert(expectConst, expectedEq);
    }

    private static Function createGeoValueFunction(int typep1, long hash1) {
        return createGeoValueFunction(typep1, hash1, true);
    }

    private static Function createGeoValueFunction(int typep1, long hash1, boolean isConstant) {
        switch (ColumnType.sizeOf(typep1)) {
            case 1:
                return new EasyGeoHashFunction(typep1, isConstant) {
                    @Override
                    public byte getGeoHashByte(Record rec) {
                        return (byte) hash1;
                    }
                };
            case 2:
                return new EasyGeoHashFunction(typep1, isConstant) {
                    @Override
                    public short getGeoHashShort(Record rec) {
                        return (short) hash1;
                    }
                };
            case 4:
                return new EasyGeoHashFunction(typep1, isConstant) {
                    @Override
                    public int getGeoHashInt(Record rec) {
                        return (int) hash1;
                    }
                };

            case 8:
                return new EasyGeoHashFunction(typep1, isConstant) {
                    @Override
                    public long getGeoHashLong(Record rec) {
                        return hash1;
                    }
                };
        }
        throw new UnsupportedOperationException();
    }

    private static Function nullConstantForBitsPrecision(int bits) {
        return Constants.getNullConstant(ColumnType.geohashWithPrecision(bits));
    }

    private void createEqFunctionAndAssert(boolean expectedEq) {
        createEqFunctionAndAssert(true, expectedEq);
    }

    private void createEqFunctionAndAssert(boolean isConstant, boolean expectedEq) {
        Function func = factory.newInstance(-1, args, null, null, null);
        Assert.assertEquals(expectedEq, func.getBool(null));
        Assert.assertEquals(isConstant, func.isConstant());
    }

    private static class EasyGeoHashFunction extends GeoHashFunction {
        private final boolean isConst;

        protected EasyGeoHashFunction(int type, boolean isConst) {
            super(type);
            this.isConst = isConst;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getGeoHashShort(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getGeoHashInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getGeoHashLong(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isConstant() {
            return isConst;
        }
    }
}
