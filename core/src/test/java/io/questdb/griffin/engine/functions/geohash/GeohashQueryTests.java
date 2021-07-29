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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashExtra;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GeohashQueryTests extends AbstractGriffinTest {
    @Test
    public void testGeohashDowncast() throws SqlException {
        assertSql("select cast(cast('questdb' as geohash(7c)) as geohash(6c)) from long_sequence(1)\n" +
                        "UNION ALL\n" +
                        "select cast('questdb' as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "questd\n" +
                        "questd\n");
    }

    @Test
    public void testGeohashDowncastNull() throws SqlException {
        assertSql("select cast(cast(NULL as geohash(7c)) as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "\n");
    }

    @Test
    public void testGeohashUpcast() {
        try {
            compiler.compile("select cast(cast('questdb' as geohash(6c)) as geohash(7c)) from long_sequence(1)", sqlExecutionContext);
            Assert.fail();
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "CAST cannot decrease precision from GEOHASH(30b) to GEOHASH(35b)");
        }
    }

    @Test
    public void testInsertGeohashTooFewChars() throws SqlException {
        compiler.compile("create table pos(time timestamp, uuid symbol, hash8 geohash(8c))", sqlExecutionContext);
        try {
            executeInsert("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','f91t')");
            Assert.fail();
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "string is too short to cast to chosen GEOHASH precision");
        }
    }

    @Test
    public void testDynamicGeohashPrecisionTrim() throws SqlException {
        compiler.compile("create table pos(time timestamp, uuid symbol, hash8 geohash(8c))", sqlExecutionContext);
        executeInsert("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','0f91tzzz')");
        assertSql("select cast(hash8 as geohash(6c)) from pos",
                "cast\n" +
                        "0f91tz\n");
    }

    @Test
    public void testGeohashReadAllCharLengths() throws SqlException {
        for(int l = 12; l > -1; l--) {
            String tableName = "pos" + l;
            compiler.compile(String.format("create table %s(hash geohash(%sc))", tableName, l), sqlExecutionContext);
            executeInsert(String.format("insert into %s values('1234567890quest')", tableName));
            String value = "1234567890quest".substring(0, l);
            assertSql("select hash from " + tableName,
                    "hash\n"
                            + value + "\n");
        }
    }

    @Test
    public void testInsertNullGeoHash() throws Exception {
        for (int b = 1; b <= 60; b++) {
            if (b > 1) {
                setUp();
            }
            try {
                int typep = GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, b);
                final String type = ColumnType.nameOf(typep);
                assertQuery(
                        "geohash\n",
                        "x",
                        String.format("create table x (geohash %s)", type),
                        null,
                        "insert into x select null from long_sequence(1)",
                        "geohash\n" +
                                "\n",
                        true,
                        true,
                        true
                );
            } finally {
                tearDown();
            }
        }
    }
}
