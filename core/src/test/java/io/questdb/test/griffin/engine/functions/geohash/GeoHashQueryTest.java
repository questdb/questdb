/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GeoHashQueryTest extends AbstractCairoTest {
    @Test
    public void assertInsertGeoHashFromLowResIntoHigh() throws Exception {
        // This test can take a long time on CI, so we limit the number of iterations randomly.
        Rnd rnd = TestUtils.generateRandom(LOG);
        for (int b = 60; b > 2; b--) {
            for (int i = 1; i < b; i++) {
                if (allowed(rnd)) {
                    assertException(
                            String.format("insert into gh%s%s select rnd_geohash(%s) from long_sequence(5)", b, i, i),
                            String.format("create table gh%s%s as (select rnd_geohash(%s) from long_sequence(5))", b, i, b),
                            20 + String.format("gh%s%s", b, i).length(),
                            "inconvertible types"
                    );
                }
            }
        }
    }

    @Test
    public void assertInsertGeoHashWithTruncate() throws Exception {
        // This test can take a long time on CI, so we limit the number of iterations randomly.
        Rnd rnd = TestUtils.generateRandom(LOG);
        for (int b = 1; b <= 60; b++) {
            for (int i = b; i <= 60; i++) {
                if (allowed(rnd)) {
                    assertQuery(
                            String.format("count\n%s\n", 5),
                            String.format("select count() from gh%s%s", b, i),
                            String.format("create table gh%s%s as (select rnd_geohash(%s) from long_sequence(5))", b, i, b),
                            null,
                            String.format("insert into gh%s%s select rnd_geohash(%s) from long_sequence(5)", b, i, i),
                            String.format("count\n%s\n", 10),
                            false,
                            true,
                            true
                    );
                }
            }
        }
    }

    @Before
    public void initRandom() {
        sqlExecutionContext.setRandom(new Rnd());
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = ColumnType.GEOLONG_MAX_BITS; l > 0; l--) {
                String tableName = "pos" + l;
                execute(String.format("create table %s(x long)", tableName));
                execute(String.format("alter table %s add hash geohash(%sb)", tableName, l));

                String columnType = l % 5 == 0 ? (l / 5) + "c" : l + "b";
                assertSql(
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                                "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                                String.format("hash\tGEOHASH(%s)\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n", columnType),
                        "show columns from " + tableName
                );
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)");
            try {
                execute("alter table pos add hash geohash(1)");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "invalid GEOHASH size, must be number followed by 'C' or 'B' character"
                );
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)");
            try {
                assertExceptionNoLeakCheck("alter table pos add hash geohash");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax22() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)");
            try {
                assertExceptionNoLeakCheck("alter table pos add hash geohash()");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing GEOHASH precision");
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)", sqlExecutionContext);
            try {
                assertExceptionNoLeakCheck("alter table pos add hash geohash(11)");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits"
                );
                Assert.assertEquals("alter table pos add hash geohash(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)");
            try {
                assertExceptionNoLeakCheck("alter table pos add hash geohash(11c 1)");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')' found='1'"
                );
                Assert.assertEquals("alter table pos add hash geohash(11c ".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashBitsColumnInvalidSyntax5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(x long)");
            try {
                assertExceptionNoLeakCheck("alter table pos add hash geohash(11c");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "invalid GEOHASH type literal, expected ')'"
                );
                Assert.assertEquals("alter table pos add hash geohash(11c".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testAlterTableAddGeoHashColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = 12; l > 0; l--) {
                String tableName = "pos" + l;
                execute(String.format("create table %s(x long)", tableName));
                execute(String.format("alter table %s add hash geohash(%sc)", tableName, l));
                assertSql(
                        "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                                "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                                String.format("hash\tGEOHASH(%sc)\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n", l),
                        "show columns from " + tableName
                );
            }
        });
    }

    @Test
    public void testDirectWrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(15) geo2," +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "rnd_geohash(40) geo9," +
                    "x " +
                    "from long_sequence(0))");

            try (TableWriter writer = getWriter("t1")) {
                for (int i = 0; i < 10; i++) {
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

            assertSql("geo1\tgeo2\tgeo4\tgeo8\tgeo9\tx\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t0\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t1\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t2\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t3\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t4\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t5\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t6\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t7\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t8\n" +
                    "q\t0qe\tqeus\tqeustdb1\tnd0e02kr\t9\n", "t1"
            );
        });
    }

    @Test
    public void testDirectWriteEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(15) geo2," +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "x " +
                    "from long_sequence(0))");

            try (TableWriter writer = getWriter("t1")) {
                for (int i = 0; i < 2; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putGeoHash(4, i);
                    row.append();
                }
                writer.commit();
            }

            assertSql("geo1\tgeo2\tgeo4\tgeo8\tx\n" +
                    "\t\t\t\t0\n" +
                    "\t\t\t\t1\n", "t1"
            );
        });
    }

    @Test
    public void testDistinctGeoHashJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "x " +
                    "from long_sequence(10))");
            execute("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "x " +
                    "from long_sequence(2))");

            assertSql("geo4\tx\tgeo41\tx1\n" +
                    "1234\t3\t1234\t1\n" +
                    "3456\t4\t3456\t2\n" +
                    "3456\t5\t3456\t2\n" +
                    "3456\t6\t3456\t2\n" +
                    "3456\t7\t3456\t2\n" +
                    "1234\t8\t1234\t1\n" +
                    "1234\t10\t1234\t1\n", "select * from t1 join t2 on t1.geo4 = t2.geo4"
            );
        });
    }

    @Test
    public void testDynamicGeoHashPrecisionTrim() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(" +
                    "time timestamp, " +
                    "uuid symbol, " +
                    "hash8 geohash(8c), " +
                    "hash4 geohash(4c), " +
                    "hash2 geohash(2c), " +
                    "hash1 geohash(1c)" +
                    ")");
            execute("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','0f91tzzz','0f91tzzz','0f91tzzz','0f91tzzz')");
            assertSql("cast\tcast1\tcast2\tcast3\n" +
                    "0f91tz\t0f9\t0\t0\n", "select cast(hash8 as geohash(6c)), cast(hash4 as geohash(3c)), cast(hash2 as geohash(1c)), cast(hash1 as geohash(1b)) from pos"
            );
        });
    }

    @Test
    public void testGeoHashDowncast() throws Exception {
        assertMemoryLeak(() -> assertSql("cast\n" +
                "questd\n" +
                "questd\n", "select cast(cast('questdb' as geohash(7c)) as geohash(6c)) from long_sequence(1)\n" +
                "UNION ALL\n" +
                "select cast('questdb' as geohash(6c)) from long_sequence(1)"
        ));
    }

    @Test
    public void testGeoHashDowncastNull() throws Exception {
        assertMemoryLeak(() -> assertSql("cast\n" +
                "\n", "select cast(cast(NULL as geohash(7c)) as geohash(6c)) from long_sequence(1)"
        ));
    }

    @Test
    public void testGeoHashDowncastSameSize() throws Exception {
        assertMemoryLeak(() -> assertSql("cast\n" +
                "questdb\n", "select cast(cast('questdb' as geohash(7c)) as geohash(35b)) from long_sequence(1)"
        ));
    }

    @Test
    public void testGeoHashEqualsTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))");

            assertSql("geo4\tx\n" +
                    "questdb\t1\n" +
                    "questdb\t2\n", "select * from t1 where geo4 = cast('questdb' as geohash(7c))"
            );
        });
    }

    @Test
    public void testGeoHashJoinOnGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(10))");
            execute("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(2))");

            assertSql("geo4\tgeo1\tx\tgeo41\tgeo11\tx1\n" +
                    "ques\tq\t1\tques\t3\t2\n" +
                    "1234\t3\t2\t1234\tq\t1\n" +
                    "ques\t1\t5\tques\t3\t2\n" +
                    "1234\t3\t6\t1234\tq\t1\n" +
                    "1234\t1\t7\t1234\tq\t1\n" +
                    "1234\tq\t8\t1234\tq\t1\n" +
                    "ques\t1\t9\tques\t3\t2\n" +
                    "ques\t1\t10\tques\t3\t2\n", "with g1 as (select distinct * from t1)," +
                    "g2 as (select distinct * from t2)" +
                    "select * from g1 join g2 on g1.geo4 = g2.geo4"
            );
        });
    }

    @Test
    public void testGeoHashJoinOnGeoHash1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(10))");
            execute("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x " +
                    "from long_sequence(2))");

            assertSql("geo4\tgeo1\tx\tgeo41\tgeo11\tx1\n" +
                    "ques\tq\t1\tques\t3\t2\n" +
                    "1234\t3\t2\t1234\tq\t1\n" +
                    "ques\t1\t5\tques\t3\t2\n" +
                    "1234\t3\t6\t1234\tq\t1\n" +
                    "1234\t1\t7\t1234\tq\t1\n" +
                    "1234\tq\t8\t1234\tq\t1\n" +
                    "ques\t1\t9\tques\t3\t2\n" +
                    "ques\t1\t10\tques\t3\t2\n", "with g1 as (select geo4, geo1, x from (select *, count() from t1))," +
                    "g2 as (select geo4, geo1, x from (select *, count() from t2))" +
                    "select * from g1 join g2 on g1.geo4 = g2.geo4"
            );
        });
    }

    @Test
    public void testGeoHashJoinOnGeoHash2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(10)) timestamp(ts)");
            execute("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2)) timestamp(ts)");

            assertSql("geo4\tgeo1\tx\tts\tgeo41\tgeo11\tx1\tts1\n" +
                            "ques\tq\t1\t1970-01-01T00:00:00.000000Z\t\t\tnull\t\n" +
                            "1234\t3\t2\t1970-01-01T00:00:01.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "3456\t3\t3\t1970-01-01T00:00:02.000000Z\t\t\tnull\t\n" +
                            "3456\t1\t4\t1970-01-01T00:00:03.000000Z\t\t\tnull\t\n" +
                            "ques\t1\t5\t1970-01-01T00:00:04.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1234\t3\t6\t1970-01-01T00:00:05.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1234\t1\t7\t1970-01-01T00:00:06.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1234\tq\t8\t1970-01-01T00:00:07.000000Z\t1234\tq\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "ques\t1\t9\t1970-01-01T00:00:08.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "ques\t1\t10\t1970-01-01T00:00:09.000000Z\tques\t3\t2\t1970-01-01T00:00:01.000000Z\n",
                    "with g1 as (select distinct * from t1 order by ts)," +
                            "g2 as (select distinct * from t2 order by ts)" +
                            "select * from g1 lt join g2 on g1.geo4 = g2.geo4"
            );
        });
    }

    @Test
    public void testGeoHashJoinTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "rnd_geohash(20) geo4," +
                    "rnd_geohash(40) geo8," +
                    "x " +
                    "from long_sequence(3))");
            execute("create table t2 as (select " +
                    "rnd_geohash(5) geo1," +
                    "rnd_geohash(10) geo2," +
                    "x " +
                    "from long_sequence(2))");

            assertSql("geo4\tgeo8\tx\tgeo1\tgeo2\tx1\n" +
                    "9v1s\t46swgj10\t1\ts\t1c\t1\n" +
                    "jnw9\tzfuqd3bf\t2\tm\t71\t2\n", "select * from t1 join t2 on t1.x = t2.x"
            );
        });
    }

    @Test
    public void testGeoHashNotEqualsNullTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))");

            assertSql("geo4\tx\n" +
                    "questdb\t1\n" +
                    "questdb\t2\n" +
                    "1234567\t3\n", "select * from t1 where cast(geo4 as geohash(5c)) != geo4 "
            );
        });
    }

    @Test
    public void testGeoHashNotEqualsTest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))");

            assertSql("geo4\tx\n" +
                    "1234567\t3\n", "select * from t1 where geo4 != cast('questdb' as geohash(7c))"
            );
        });
    }

    @Test
    public void testGeoHashReadAllCharLengths() throws Exception {
        assertMemoryLeak(() -> {
            for (int l = 12; l > 0; l--) {
                String tableName = "pos" + l;
                execute(String.format("create table %s(hash geohash(%sc))", tableName, l));
                execute(String.format("insert into %s values('1234567890quest')", tableName));
                String value = "1234567890quest".substring(0, l);
                assertSql("hash\n"
                        + value + "\n", "select hash from " + tableName
                );
            }
        });
    }

    @Test
    public void testGeoHashSimpleGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('questdb', '1234567') as geohash(7c)) geo4, " +
                    "x " +
                    "from long_sequence(3))");

            assertSql("first\tlast\n" +
                    "questdb\t1234567\n", "select first(geo4), last(geo4) from t1"
            );
        });
    }

    @Test
    public void testGeoHashUpcast() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select cast(cast('questdb' as geohash(6c)) as geohash(7c)) from long_sequence(1)");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "CAST cannot narrow values from GEOHASH(30b) to GEOHASH(35b)");
            }
        });
    }

    @Test
    public void testInsertGeoHashTooFewChars() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table pos(time timestamp, uuid symbol, hash8 geohash(8c))", sqlExecutionContext);
            try {
                assertExceptionNoLeakCheck("insert into pos values('2021-05-10T23:59:59.160000Z','YYY','f91t')");
            } catch (ImplicitCastException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "inconvertible value: `f91t` [STRING -> GEOHASH(8c)]");
            }
        });
    }

    @Test
    public void testInvalidGeoHashRnd() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertSql("", "select rnd_geohash(0) from long_sequence(1)");
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
                assertSql("", "select rnd_geohash(61) from long_sequence(1)");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "precision must be in [1..60] range");
            }
        });
    }

    @Test
    public void testMakeGeoHashFromCoords() throws Exception {
        assertMemoryLeak(() -> assertSql("h8c\n" +
                "jr1nj0dv\n" +
                "29tdrk0h\n" +
                "9su67p3e\n", "select make_geohash(lon,lat,40) as h8c\n" +
                "from ( select \n" +
                "(rnd_double()*180.0 - 90.0) as lat,\n" +
                "(rnd_double()*360.0 - 180.0) as lon\n" +
                "from long_sequence(3))"
        ));
    }

    @Test
    public void testMakeGeoHashNullOnOutOfRange() throws Exception {
        assertMemoryLeak(() -> assertSql("h8c\n" +
                "\n" +
                "u9tdrk0h\n" +
                "\n", "select make_geohash(lon, lat,40) as h8c\n" +
                "from ( select \n" +
                "(rnd_double()*180.0) as lat,\n" +
                "(rnd_double()*360.0) as lon\n" +
                "from long_sequence(3))"
        ));
    }

    @Test
    public void testMakeGeoHashToDifferentColumnSize() throws Exception {
        assertMemoryLeak(() -> {

            execute("create table pos as ( " +
                    " select" +
                    "(rnd_double()*180.0 - 90.0) as lat, " +
                    "(rnd_double()*360.0 - 180.0) as lon " +
                    "from long_sequence(1))");

            execute("create table tb1 as ( select" +
                    " make_geohash(lon, lat, 5) as g1c, " +
                    " make_geohash(lon, lat, 10) as g2c, " +
                    " make_geohash(lon, lat, 20) as g4c, " +
                    " make_geohash(lon, lat, 40) as g8c  " +
                    " from pos)");

            assertSql("g1c\tg2c\tg4c\tg8c\n" +
                    "9\t9v\t9v1s\t9v1s8hm7\n", "select * from tb1"
            );
        });
    }

    @Test
    public void testWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2))");

            execute("alter table t1 add a1 geohash(1c)");
            execute("alter table t1 add a2 geohash(2c)");
            execute("alter table t1 add a4 geohash(4c)");
            execute("alter table t1 add a8 geohash(8c)");

            execute("insert into t1 select x," +
                    "timestamp_sequence(0, 1000000) ts," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('questdb123456', '12345672', '901234567') as geohash(8c)) geo8 " +
                    "from long_sequence(2)"
            );

            assertSql("x\tts\ta1\ta2\ta4\ta8\n" +
                    "1\t1970-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "2\t1970-01-01T00:00:01.000000Z\t\t\t\t\n" +
                    "1\t1970-01-01T00:00:00.000000Z\tq\tqu\t1234\t90123456\n" +
                    "2\t1970-01-01T00:00:01.000000Z\t3\t34\t3456\t12345672\n", "t1"
            );
        });
    }

    private static boolean allowed(Rnd rnd) {
        // when investigating a CI test failure make sure
        // to change the OS detection to match the CI OS.
        if (Os.isWindows()) {
            // windows has slow mmap(), we throttle aggressively: only 5% allowed
            int i = rnd.nextInt(20);
            return i == 19;
        } else {
            // other OSs are faster -> we allow to run with 20% probability
            int i = rnd.nextInt(5);
            return i == 4;
        }
    }
}
