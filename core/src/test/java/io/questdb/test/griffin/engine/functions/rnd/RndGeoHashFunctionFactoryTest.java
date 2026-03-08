/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.RndGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RndGeoHashFunctionFactoryTest extends BaseFunctionFactoryTest {
    private FunctionParser functionParser;
    private GenericRecordMetadata metadata;
    private Rnd rnd;

    @Before
    public void setUp5() {
        functions.add(new RndGeoHashFunctionFactory());
        functionParser = createFunctionParser();
        metadata = new GenericRecordMetadata();
        rnd = new Rnd();
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadArgumentValue1() {
        try {
            getFunction(0);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] precision must be in [1..60] range", e.getMessage());
        }
    }

    @Test
    public void testBadArgumentValue2() {
        try {
            getFunction(61);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] precision must be in [1..60] range", e.getMessage());
        }
    }

    @Test
    public void testGetGeoHashByte() throws SqlException {
        for (int bits = 1; bits < 8; bits++) {
            Function function = getFunction(bits);
            for (int i = 0, n = 1000; i < n; i++) {
                Assert.assertEquals(rnd.nextGeoHashByte(bits), function.getGeoByte(null));
            }
        }
    }

    @Test
    public void testGetGeoHashInt() throws SqlException {
        for (int bits = 16; bits < 32; bits++) {
            Function function = getFunction(bits);
            for (int i = 0, n = 1000; i < n; i++) {
                Assert.assertEquals(rnd.nextGeoHashInt(bits), function.getGeoInt(null));
            }
        }
    }

    @Test
    public void testGetGeoHashLong() throws SqlException {
        for (int bits = 32; bits <= 60; bits++) {
            Function function = getFunction(bits);
            for (int i = 0, n = 1000; i < n; i++) {
                Assert.assertEquals(rnd.nextGeoHashLong(bits), function.getGeoLong(null));
            }
        }
    }

    @Test
    public void testGetGeoHashShort() throws SqlException {
        // last bit of geo-short is reserved for null, therefore
        // geo-short is 15 bits max
        for (int bits = 8; bits < 16; bits++) {
            Function function = getFunction(bits);
            for (int i = 0, n = 1000; i < n; i++) {
                Assert.assertEquals(rnd.nextGeoHashShort(bits), function.getGeoShort(null));
            }
        }
    }

    private Function getFunction(int bits) throws SqlException {
        Function function = parseFunction(String.format("rnd_geohash(%d)", bits), metadata, functionParser);
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(bits), function.getType());
        Assert.assertFalse(function.isConstant());
        function.init(null, sqlExecutionContext);
        return function;
    }
}
