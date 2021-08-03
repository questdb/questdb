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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GeoHashQueryTest extends AbstractGriffinTest {
    @Before
    public void initRandom() {
        sqlExecutionContext.setRandom(new Rnd());
    }

    @Test
    public void testGeohashDowncast() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast('questdb' as geohash(7c)) as geohash(6c)) from long_sequence(1)\n" +
                        "UNION ALL\n" +
                        "select cast('questdb' as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "questd\n" +
                        "questd\n"));
    }

    @Test
    public void testGeohashDowncastNull() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast(NULL as geohash(7c)) as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "\n"));
    }

    @Test
    public void testGeohashDowncastSameSize() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast('questdb' as geohash(7c)) as geohash(35b)) from long_sequence(1)",
                "cast\n" +
                        "questdb\n"));
    }


    @Test
    public void testGeohashUpcast() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("select cast(cast('questdb' as geohash(6c)) as geohash(7c)) from long_sequence(1)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "CAST cannot decrease precision from GEOHASH(30b) to GEOHASH(35b)");
            }
        });
    }

    @Test
    public void testInsertGeohashTooFewChars() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(time timestamp, uuid symbol, hash8 geohash(8c))", sqlExecutionContext);
            try {
                executeInsert("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','f91t')");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "string is too short to cast to chosen GEOHASH precision");
            }
        });
    }

    @Test
    public void testDynamicGeohashPrecisionTrim() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(" +
                    "time timestamp, " +
                    "uuid symbol, " +
                    "hash8 geohash(8c), " +
                    "hash4 geohash(4c), " +
                    "hash2 geohash(2c), " +
                    "hash1 geohash(1c)" +
                    ")", sqlExecutionContext);
            executeInsert("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','0f91tzzz','0f91tzzz','0f91tzzz','0f91tzzz')");
            assertSql("select cast(hash8 as geohash(6c)), cast(hash4 as geohash(3c)), cast(hash2 as geohash(1c)), cast(hash1 as geohash(1b)) from pos",
                    "cast\tcast1\tcast2\tcast3\n" +
                            "0f91tz\t0f9\t0\t0\n");
        });
    }

    @Test
    public void testGeohashReadAllCharLengths() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = 12; l > 0; l--) {
                String tableName = "pos" + l;
                compiler.compile(String.format("create table %s(hash geohash(%sc))", tableName, l), sqlExecutionContext);
                executeInsert(String.format("insert into %s values('1234567890quest')", tableName));
                String value = "1234567890quest".substring(0, l);
                assertSql("select hash from " + tableName,
                        "hash\n"
                                + value + "\n");
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = 12; l > 0; l--) {
                String tableName = "pos" + l;
                compiler.compile(String.format("create table %s(x long)", tableName), sqlExecutionContext);
                compiler.compile(String.format("alter table %s add hash geohash(%sc)", tableName, l), sqlExecutionContext);
                assertSql("show columns from " + tableName, "" +
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\n" +
                        String.format("hash\tGEOHASH(%sc)\tfalse\t256\tfalse\t0\tfalse\n", l));
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = GeoHashNative.MAX_BITS_LENGTH; l > 0; l--) {
                String tableName = "pos" + l;
                compiler.compile(String.format("create table %s(x long)", tableName), sqlExecutionContext);
                compiler.compile(String.format("alter table %s add hash geohash(%sb)", tableName, l), sqlExecutionContext);

                String columnType = l % 5 == 0 ? (l / 5) + "c" : l + "b";
                assertSql("show columns from " + tableName, "" +
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\n" +
                        String.format("hash\tGEOHASH(%s)\tfalse\t256\tfalse\t0\tfalse\n", columnType));
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash(1)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH size, must be number followed by 'C' or 'B' character");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax22() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash()", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax3() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash(11)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax4() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash(11c 1)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')' found='1'");
                Assert.assertEquals("alter table pos add hash geohash(11c ".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeohashBitsColumnInvlidSyntax5() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compiler.compile("alter table pos add hash geohash(11c", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')'");
                Assert.assertEquals("alter table pos add hash geohash(11c".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testInvalidGeohashRnd() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSql("select rnd_geohash(0) from long_sequence(1)", "");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "precision must be in [1..60] range");
            }
        });
    }

    @Test
    public void testInvalidGeohashRnd2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSql("select rnd_geohash(61) from long_sequence(1)", "");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "precision must be in [1..60] range");
            }
        });
    }

    @Test
    public void testGeohashJoinTest() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "x " +
                    "from long_sequence(3))", sqlExecutionContext);
            compiler.compile("create table t2 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(10) geo2," +
                    "x " +
                    "from long_sequence(2))", sqlExecutionContext);

            assertSql("select * from t1 join t2 on t1.x = t2.x",
                    "geo4\tgeo8\tx\tgeo1\tgeo2\tx1\n" +
                            "9v1s\t46swgj10\t1\ts\t1c\t1\n" +
                            "jnw9\tzfuqd3bf\t2\tm\t71\t2\n");
        });
    }

    @Test
    public void testGeohashJoinOnGeohash() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "x " +
                    "from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "x " +
                    "from long_sequence(2))", sqlExecutionContext);

            assertSql("select * from t1 join t2 on t1.geo4 = t2.geo4",
                    "geo4\tx\tgeo41\tx1\n" +
                            "1234\t3\t1234\t1\n" +
                            "3456\t4\t3456\t2\n" +
                            "3456\t5\t3456\t2\n" +
                            "3456\t6\t3456\t2\n" +
                            "3456\t7\t3456\t2\n" +
                            "1234\t8\t1234\t1\n" +
                            "1234\t10\t1234\t1\n");
        });
    }

    @Test
    public void assertInsertGeoHashWithTruncate() throws Exception {
        tearDown();
        for (int b = 5; b <= 40; b *= 2) {
            for (int i = b; i <= 40; i *= 2) {
                setUp();
                try {
                    assertQuery(
                            String.format("count\n%s\n", 5),
                            "select count() from gh",
                            String.format("create table gh as (select rnd_geohash(%s) from long_sequence(5))", b),
                            null,
                            String.format("insert into gh select rnd_geohash(%s) from long_sequence(5)", i),
                            String.format("count\n%s\n", 10),
                            false,
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

}
