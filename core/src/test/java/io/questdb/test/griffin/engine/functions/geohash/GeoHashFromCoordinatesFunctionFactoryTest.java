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

package io.questdb.test.griffin.engine.functions.geohash;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.geohash.GeoHashFromCoordinatesFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashFromCoordinatesFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testMakeGeoHash13bits() throws Exception {
        assertQuery(
                "make_geohash\n0111101011101\n",
                "select make_geohash(-0.1275, 51.50722, 13)",
                null,
                true,
                true
        );
    }

    @Test
    public void testMakeGeoHash40bits() throws Exception {
        assertQuery(
                "make_geohash\ngcpvj0e5\n",
                "select make_geohash(-0.1275, 51.50722, 40)",
                null,
                true,
                true
        );
    }

    @Test
    public void testMakeGeoHashZero() throws Exception {
        assertQuery(
                "make_geohash\ns0000\n",
                "select make_geohash(0.0, 0.0, 25)",
                null,
                true,
                true
        );
    }

    @Test
    public void testMakeGeoHashZeroBits() throws Exception {
        assertQuery(
                "make_geohash\n11000000000000000000000\n",
                "select make_geohash(0.0, 0.0, 23)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOutOfRangeBits0() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(-0.1275, -91.50722, 61)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "precision must be in [1..60] range");
            }
        });
    }

    @Test
    public void testOutOfRangeBits1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(-0.1275, -91.50722, 0)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "precision must be in [1..60] range");
            }
        });
    }

    @Test
    public void testOutOfRangeLat1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(-0.1275, 91.50722, 40)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "latitude must be in [-90.0..90.0] range");
            }
        });
    }

    @Test
    public void testOutOfRangeLat2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(-0.1275, -91.50722, 40)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "latitude must be in [-90.0..90.0] range");
            }
        });
    }

    @Test
    public void testOutOfRangeLon1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(-195.0, 51.50722, 40)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "longitude must be in [-180.0..180.0] range");
            }
        });
    }

    @Test
    public void testOutOfRangeLon2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "make_geohash\n\n",
                        "select make_geohash(195.0, 51.50722, 40)",
                        null,
                        true,
                        true
                );
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "longitude must be in [-180.0..180.0] range");
            }
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new GeoHashFromCoordinatesFunctionFactory();
    }
}
