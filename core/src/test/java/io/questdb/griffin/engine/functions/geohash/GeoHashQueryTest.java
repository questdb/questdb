/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
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
    public void testGeoHashDowncast() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast('questdb' as geohash(7c)) as geohash(6c)) from long_sequence(1)\n" +
                        "UNION ALL\n" +
                        "select cast('questdb' as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "questd\n" +
                        "questd\n"));
    }

    @Test
    public void testGeoHashDowncastNull() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast(NULL as geohash(7c)) as geohash(6c)) from long_sequence(1)",
                "cast\n" +
                        "\n"));
    }

    @Test
    public void testGeoHashDowncastSameSize() throws Exception {
        assertMemoryLeak(() -> assertSql("select cast(cast('questdb' as geohash(7c)) as geohash(35b)) from long_sequence(1)",
                "cast\n" +
                        "questdb\n"));
    }


    @Test
    public void testGeoHashUpcast() throws Exception {
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
    public void testInsertGeoHashTooFewChars() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(time timestamp, uuid symbol, hash8 geohash(8c))", sqlExecutionContext);
            try {
                executeInsert("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','f91t')");
                Assert.fail();
            } catch (ImplicitCastException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "inconvertible value: f91t [STRING -> GEOHASH(8c)] tuple: 0");
            }
        });
    }

    @Test
    public void testDynamicGeoHashPrecisionTrim() throws Exception {
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
    public void testGeoHashReadAllCharLengths() throws Exception {
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
    public void testAlterTableAddGeoHashColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = 12; l > 0; l--) {
                String tableName = "pos" + l;
                compiler.compile(String.format("create table %s(x long)", tableName), sqlExecutionContext);
                compile(String.format("alter table %s add hash geohash(%sc)", tableName, l), sqlExecutionContext);
                assertSql("show columns from " + tableName, "" +
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\n" +
                        String.format("hash\tGEOHASH(%sc)\tfalse\t256\tfalse\t0\tfalse\n", l));
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = ColumnType.GEO_HASH_MAX_BITS_LENGTH; l > 0; l--) {
                String tableName = "pos" + l;
                compiler.compile(String.format("create table %s(x long)", tableName), sqlExecutionContext);
                compile(String.format("alter table %s add hash geohash(%sb)", tableName, l), sqlExecutionContext);

                String columnType = l % 5 == 0 ? (l / 5) + "c" : l + "b";
                assertSql("show columns from " + tableName, "" +
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\n" +
                        String.format("hash\tGEOHASH(%s)\tfalse\t256\tfalse\t0\tfalse\n", columnType));
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash(1)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH size, must be number followed by 'C' or 'B' character");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax22() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash()", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax3() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash(11)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax4() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash(11c 1)", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')' found='1'");
                Assert.assertEquals("alter table pos add hash geohash(11c ".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvlidSyntax5() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table pos(x long)", sqlExecutionContext);
            try {
                compile("alter table pos add hash geohash(11c", sqlExecutionContext);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')'");
                Assert.assertEquals("alter table pos add hash geohash(11c".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testInvalidGeoHashRnd() throws Exception {
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
    public void testInvalidGeoHashRnd2() throws Exception {
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
    public void testGeoHashJoinTest() throws Exception {
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
    public void testGeoHashJoinOnGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(2))", sqlExecutionContext);

            assertSql("with g1 as (select distinct * from t1)," +
                            "g2 as (select distinct * from t2)" +
                            "select * from g1 join g2 on g1.geo4 = g2.geo4",
                    "geo4\tgeo1\tx\tgeo41\tgeo11\tx1\n" +
                            "ques\tq\t1\tques\t3\t2\n" +
                            "1234\t3\t2\t1234\tq\t1\n" +
                            "ques\t1\t5\tques\t3\t2\n" +
                            "1234\t3\t6\t1234\tq\t1\n" +
                            "1234\t1\t7\t1234\tq\t1\n" +
                            "1234\tq\t8\t1234\tq\t1\n" +
                            "ques\t1\t9\tques\t3\t2\n" +
                            "ques\t1\t10\tques\t3\t2\n");
        });
    }

    @Test
    public void testGeoHashJoinOnGeoHash2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2)) timestamp(ts)", sqlExecutionContext);

            assertSql("with g1 as (select distinct * from t1)," +
                            "g2 as (select distinct * from t2)" +
                            "select * from g1 lt join g2 on g1.geo4 = g2.geo4",
                    "geo4\tgeo1\tx\tts\tgeo41\tgeo11\tx1\tts1\n" +
                            "ques\tq\t1\t1970-01-01T00:00:00.000000Z\t\t\tNaN\t\n" +
                            "1234\t3\t2\t1970-01-01T00:00:01.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "3456\t3\t3\t1970-01-01T00:00:02.000000Z\t\t\tNaN\t\n" +
                            "3456\t1\t4\t1970-01-01T00:00:03.000000Z\t\t\tNaN\t\n" +
                            "ques\t1\t5\t1970-01-01T00:00:04.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1234\t3\t6\t1970-01-01T00:00:05.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1234\t1\t7\t1970-01-01T00:00:06.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1234\tq\t8\t1970-01-01T00:00:07.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "ques\t1\t9\t1970-01-01T00:00:08.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "ques\t1\t10\t1970-01-01T00:00:09.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n");
        });
    }

    @Test
    public void testWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2))", sqlExecutionContext);

            compile("alter table t1 add a1 geohash(1c)", sqlExecutionContext);
            compile("alter table t1 add a2 geohash(2c)", sqlExecutionContext);
            compile("alter table t1 add a4 geohash(4c)", sqlExecutionContext);
            compile("alter table t1 add a8 geohash(8c)", sqlExecutionContext);

            compiler.compile("insert into t1 select x," +
                    "timestamp_sequence(0, 1000000) ts," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('questdb123456', '12345672', '901234567') as geohash(8c)) geo8 " +
                    "from long_sequence(2)",
                    sqlExecutionContext);

            assertSql("t1",
                    "x\tts\ta1\ta2\ta4\ta8\n" +
                            "1\t1970-01-01T00:00:00.000000Z\t\t\t\t\n" +
                            "2\t1970-01-01T00:00:01.000000Z\t\t\t\t\n" +
                            "1\t1970-01-01T00:00:00.000000Z\tq\tqu\t1234\t90123456\n" +
                            "2\t1970-01-01T00:00:01.000000Z\t3\t34\t3456\t12345672\n");
        });
    }

    @Test
    public void testDistinctGeoHashJoin() throws Exception {
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
        for (int b = 1; b <= 60; b++) {
            for (int i = b; i <= 60; i++) {
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

    @Test
    public void assertInsertGeoHashFromLowResIntoHigh() throws Exception {
        tearDown();
        for (int b = 60; b > 2; b--) {
            for (int i = 1; i < b; i++) {
                setUp();
                try {
                    assertFailure(
                            String.format("insert into gh select rnd_geohash(%s) from long_sequence(5)", i),
                            String.format("create table gh as (select rnd_geohash(%s) from long_sequence(5))", b),
                            22,
                            "inconvertible types"
                    );
                } finally {
                    tearDown();
                }
            }
        }
    }

    @Test
    public void testDirectWrite() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(15) geo2," +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "rnd_geohash(40) geo9," +
                    "x " +
                    "from long_sequence(0))", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "t1", "test")) {
                for(int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putGeoStr(0, "qeustdb");
                    row.putGeoHash(1, GeoHashes.fromString("qeustdb", 0, 2));
                    row.putGeoHash(2, GeoHashes.fromString("qeustdb", 0, 4));
                    row.putGeoHash(3, GeoHashes.fromString("qeustdb123456", 0, 8));
                    row.putGeoHashDeg(4, -78.22, 113.22);
                    row.putGeoHash(5, i);
                    row.append();
                }
                writer.commit();
            }

            assertSql("t1",
                    "geo1\tgeo2\tgeo4\tgeo8\tgeo9\tx\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t0\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t1\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t2\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t3\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t4\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t5\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t6\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t7\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t8\n" +
                            "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t9\n"
            );
        });
    }


    @Test
    public void testDirectWriteEmpty() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(15) geo2," +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "x " +
                    "from long_sequence(0))", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "t1", "test")) {
                for(int i = 0; i < 2; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putGeoHash(4, i);
                    row.append();
                }
                writer.commit();
            }

            assertSql("t1",
                    "geo1\tgeo2\tgeo4\tgeo8\tx\n" +
                            "\t\t\t\t0\n" +
                            "\t\t\t\t1\n");
        });
    }

    @Test
    public void testGeoHashEqualsTest() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))", sqlExecutionContext);

            assertSql("select * from t1 where geo4 = cast('questdb' as geohash(7c))",
                    "geo4\tx\n" +
                            "questdb\t1\n" +
                            "questdb\t2\n");
        });
    }

    @Test
    public void testGeoHashNotEqualsTest() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))", sqlExecutionContext);

            assertSql("select * from t1 where geo4 != cast('questdb' as geohash(7c))",
                    "geo4\tx\n" +
                            "1234567\t3\n");
        });
    }

    @Test
    public void testGeoHashNotEqualsNullTest() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))", sqlExecutionContext);

            assertSql("select * from t1 where cast(geo4 as geohash(5c)) != geo4 ",
                    "geo4\tx\n" +
                            "questdb\t1\n" +
                            "questdb\t2\n" +
                            "1234567\t3\n");
        });
    }

    @Test
    public void testGeoHashSimpleGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))", sqlExecutionContext);

            assertSql("select first(geo4), last(geo4) from t1",
                    "first\tlast\n" +
                            "questdb\t1234567\n");
        });
    }

    @Test
    public void testMakeGeoHashFromCoords() throws Exception {
        assertMemoryLeak(() -> assertSql("select make_geohash(lon,lat,40) as h8c\n" +
                        "from ( select \n" +
                        "(rnd_double()*180.0 - 90.0) as lat,\n" +
                        "(rnd_double()*360.0 - 180.0) as lon\n" +
                        "from long_sequence(3))",
                        "h8c\n" +
                                "jr1nj0dv\n" +
                                "29tdrk0h\n" +
                                "9su67p3e\n"));
    }

    @Test
    public void testMakeGeoHashToDifferentColumnSize() throws Exception {
        assertMemoryLeak(() -> {

            compiler.compile("create table pos as ( " +
                    " select" +
                    "(rnd_double()*180.0 - 90.0) as lat, " +
                    "(rnd_double()*360.0 - 180.0) as lon " +
                    "from long_sequence(1))", sqlExecutionContext);

            compiler.compile("create table tb1 as ( select" +
                    " make_geohash(lon, lat, 5) as g1c, " +
                    " make_geohash(lon, lat, 10) as g2c, " +
                    " make_geohash(lon, lat, 20) as g4c, " +
                    " make_geohash(lon, lat, 40) as g8c  " +
                    " from pos)", sqlExecutionContext);

            assertSql("select * from tb1",
                    "g1c\tg2c\tg4c\tg8c\n" +
                            "9\t9v\t9v1s\t9v1s8hm7\n");
        });
    }

    @Test
    public void testMakeGeoHashNullOnOutOfRange() throws Exception {
        assertMemoryLeak(() -> assertSql("select make_geohash(lon, lat,40) as h8c\n" +
                        "from ( select \n" +
                        "(rnd_double()*180.0) as lat,\n" +
                        "(rnd_double()*360.0) as lon\n" +
                        "from long_sequence(3))",
                "h8c\n" +
                        "\n" +
                        "u9tdrk0h\n" +
                        "\n"));
    }
}
