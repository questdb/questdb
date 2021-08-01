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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EqGeoHashGeoHashFunctionFactoryTest extends AbstractGriffinTest {

    private static final EqGeoHashGeoHashFunctionFactory factory = new EqGeoHashGeoHashFunctionFactory();

    private ObjList<Function> args;

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
        args = new ObjList<>(2);
    }

    // TODO: WIP add more tests to cover both args not being constants, and only one not being constant

    @Test
    public void testConstConst1() {
        args.add(NullConstant.NULL);
        args.add(NullConstant.NULL);
        Function func = factory.newInstance(-1, args, null, null, null);
        Assert.assertTrue(func.isConstant());
        Assert.assertTrue(func.getBool(null));
    }

    @Test
    public void testConstConst2() {
        args.add(NullConstant.NULL);
        for (int b = 1; b <= GeoHashNative.MAX_BITS_LENGTH; b++) {
            args.setPos(1);
            int type = ColumnType.geohashWithPrecision(b);
            args.add(Constants.getNullConstant(type));
            Function func = factory.newInstance(-1, args, null, null, null);
            Assert.assertTrue(func.isConstant());
            Assert.assertTrue(func.getBool(null));
        }
    }

    @Test
    public void testConstConst3() {
        for (int b = 1; b <= GeoHashNative.MAX_BITS_LENGTH; b++) {
            args.clear();
            int type = ColumnType.geohashWithPrecision(b);
            args.add(Constants.getNullConstant(type));
            args.add(NullConstant.NULL);
            Function func = factory.newInstance(-1, args, null, null, null);
            Assert.assertTrue(func.isConstant());
            Assert.assertTrue(func.getBool(null));
        }
    }

    @Test
    public void testConstConst4() {
        for (int b = 1; b <= GeoHashNative.MAX_BITS_LENGTH; b++) {
            args.clear();
            int type = ColumnType.geohashWithPrecision(b);
            args.add(Constants.getNullConstant(type));
            type = ColumnType.geohashWithPrecision(((b + 1) % 60) + 1);
            args.add(Constants.getNullConstant(type));
            Function func = factory.newInstance(-1, args, null, null, null);
            Assert.assertTrue(func.isConstant());
            Assert.assertTrue(func.getBool(null));
        }
    }

    @Test
    public void testConstConst5() {
        for (int b = 1; b <= GeoHashNative.MAX_BITS_LENGTH; b++) {
            args.clear();
            int type = ColumnType.geohashWithPrecision(b);
            args.add(GeoHashConstant.newInstance(0, type));
            args.add(GeoHashConstant.newInstance(0, type));
            Function func = factory.newInstance(-1, args, null, null, null);
            Assert.assertTrue(func.isConstant());
            Assert.assertTrue(func.getBool(null));
        }
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
}
