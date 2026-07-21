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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;


public class HashJoinTest extends AbstractCairoTest {

    /**
     * Check that hash join factory doesn't allocate substantial amounts of memory prior to- and after cursor execution.
     * This is tricky because:
     * - memory allocation is delayed (so malloc() doesn't really allocate)
     * - most objects malloc but don't touch memory
     * - rss/wss can jump up and down due to gc, os, etc.
     */
    @Test
    public void testHashJoinDoesNotAllocateMemoryPriorToCursorOpenAndAfterCursorCloseForNonEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table weather_data_historical (
                      sensor_time timestamp not null,
                      sensor_day symbol,
                      min_temperature_out float,
                      max_temperature_out float,
                      avg_temperature_out float,
                      snow_height float,
                      rain_acc_24h float,
                      max_wind_gust_speed float
                    )
                    TIMESTAMP(sensor_time);""", sqlExecutionContext);

            execute("""
                    insert into weather_data_historical\s
                    select cast(x*1000000000 as timestamp), to_str( cast(x*1000000000 as timestamp), 'MM-dd'),\s
                           rnd_float()*100, rnd_float()*100, rnd_float()*100, rnd_float()*200, rnd_float()*100, rnd_float()*300
                    from long_sequence(1000);""");

            // allocate readers eagerly (at least one for each join) so that final getMem() doesn't report them as diff
            TableReader[] readers = new TableReader[10];
            try {
                for (int i = 0; i < readers.length; i++) {
                    readers[i] = getReader("weather_data_historical");
                }
            } finally {
                Misc.free(readers);
            }

            long tagBeforeFactory = getMemUsedByFactories();
            System.gc();

            try (
                    final RecordCursorFactory factory = select(
                            """
                                      select a1.sensor_day,\s
                                      warmest_day, to_str(a2.sensor_time, 'yyyy') as warmest_day_year,\s
                                      coldest_day, to_str(a3.sensor_time, 'yyyy') as coldest_day_year,
                                      warmest_night, to_str(a4.sensor_time, 'yyyy') as warmest_night_year,
                                      coldest_night, to_str(a5.sensor_time, 'yyyy') as coldest_night_year,
                                      max_snow_height, to_str(a6.sensor_time, 'yyyy') as max_snow_height_year,
                                      max_wind_gust_overall, to_str(a7.sensor_time, 'yyyy') as max_wind_gust_year,
                                      avg_temperature
                                      from
                                      (
                                        select sensor_day,\s
                                        max(max_temperature_out) as warmest_day, min(max_temperature_out) as coldest_day,\s
                                        max(min_temperature_out) as warmest_night, min(min_temperature_out) as coldest_night,\s
                                        max(rain_acc_24h) as max_rain, max(snow_height) as max_snow_height,\s
                                        max(max_wind_gust_speed) as max_wind_gust_overall,\s
                                        avg(avg_temperature_out) as avg_temperature
                                        from weather_data_historical\s
                                        group by sensor_day
                                      ) a1
                                      left join weather_data_historical a2 on (a1.sensor_day = a2.sensor_day and warmest_day = a2.max_temperature_out)
                                      left join weather_data_historical a3 on (a1.sensor_day = a3.sensor_day and coldest_day = a3.max_temperature_out)
                                      left join weather_data_historical a4 on (a1.sensor_day = a4.sensor_day and warmest_night = a4.min_temperature_out)
                                      left join weather_data_historical a5 on (a1.sensor_day = a5.sensor_day and coldest_night = a5.min_temperature_out)
                                      left join weather_data_historical a6 on (a1.sensor_day = a6.sensor_day and max_snow_height = a6.snow_height and a6.snow_height > 0)
                                      left join weather_data_historical a7 on (a1.sensor_day = a7.sensor_day and max_wind_gust_overall = a7.max_wind_gust_speed)\
                                    """
                    )
            ) {
                long freeCount;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.drainCursor(cursor);
                    freeCount = Unsafe.getFreeCount();
                }

                Assert.assertTrue(freeCount < Unsafe.getFreeCount());
                Assert.assertTrue(getMemUsedByFactories() < tagBeforeFactory + 1024 * 1024);
            }
        });
    }

    @Test
    public void testHashJoinEmptyNonAsciiVarcharKey() throws Exception {
        // Reproduces a crash reported against QuestDB 3.3.x: an inner (hash) join on a VARCHAR
        // key failed with an AssertionError in UnorderedVarcharMap.putVarchar when a join key
        // held an empty, non-ASCII VARCHAR. An empty varchar is ASCII by definition, but the
        // Utf8Sequence contract lets a value report isAscii() == false for it, and such values
        // exist at rest. Its packed (hash, size, flags) representation was 0, colliding with the
        // map's empty-slot sentinel. The same data worked through an ASOF join, which does not
        // use the varchar hash map.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k VARCHAR, v INT)");
            execute("CREATE TABLE s (k VARCHAR, w INT)");

            // "" written with the ASCII flag deliberately cleared, matching at-rest data that no
            // longer round-trips through a value producer that would re-flag it as ASCII.
            Utf8String emptyNonAscii = new Utf8String(new byte[0], false);
            Assert.assertEquals(0, emptyNonAscii.size());
            Assert.assertFalse(emptyNonAscii.isAscii());

            try (TableWriter writer = getWriter("m")) {
                TableWriter.Row row = writer.newRow();
                row.putVarchar(0, new Utf8String("a"));
                row.putInt(1, 1);
                row.append();

                row = writer.newRow();
                row.putVarchar(0, emptyNonAscii);
                row.putInt(1, 2);
                row.append();
                writer.commit();
            }

            try (TableWriter writer = getWriter("s")) {
                TableWriter.Row row = writer.newRow();
                row.putVarchar(0, new Utf8String("a"));
                row.putInt(1, 10);
                row.append();

                row = writer.newRow();
                row.putVarchar(0, emptyNonAscii);
                row.putInt(1, 20);
                row.append();
                writer.commit();
            }

            assertQuery("SELECT m.v, s.w FROM m JOIN s ON m.k = s.k ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            v\tw
                            1\t10
                            2\t20
                            """);
        });
    }

    @Test
    public void testHashJoinSymbolAndDecimalKeys() throws Exception {
        assertMemoryLeak(() -> {
            assertHashJoinSymbolAndDecimalKey("dec8", "DECIMAL(2,1)", "1.2", "2.3", "3.4");
            assertHashJoinSymbolAndDecimalKey("dec16", "DECIMAL(4,2)", "12.34", "23.45", "34.56");
            assertHashJoinSymbolAndDecimalKey("dec32", "DECIMAL(9,4)", "12345.6789", "23456.7890", "34567.8901");
            assertHashJoinSymbolAndDecimalKey("dec64", "DECIMAL(18,6)", "123456789012.345678", "234567890123.456789", "345678901234.567890");
            assertHashJoinSymbolAndDecimalKey(
                    "dec128",
                    "DECIMAL(38,10)",
                    "1234567890123456789012345678.9012345678",
                    "2345678901234567890123456789.0123456789",
                    "3456789012345678901234567890.1234567890"
            );
            assertHashJoinSymbolAndDecimalKey(
                    "dec256",
                    "DECIMAL(76,20)",
                    "12345678901234567890123456789012345678901234567890123456.78901234567890123456",
                    "23456789012345678901234567890123456789012345678901234567.89012345678901234567",
                    "34567890123456789012345678901234567890123456789012345678.90123456789012345678"
            );
        });
    }

    @Test
    public void testHashJoinSymbolAndUuidKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id UUID, sym SYMBOL, v INT)");
            execute("CREATE TABLE s (id UUID, sym SYMBOL, v INT)");
            execute("""
                    INSERT INTO m VALUES
                        ('11111111-1111-1111-1111-111111111111', 'A', 1),
                        ('22222222-2222-2222-2222-222222222222', 'B', 2),
                        ('33333333-3333-3333-3333-333333333333', 'C', 3),
                        (cast(null as UUID), 'N', 4),
                        ('44444444-4444-4444-4444-444444444444', null, 5)""");
            execute("""
                    INSERT INTO s VALUES
                        ('11111111-1111-1111-1111-111111111111', 'A', 10),
                        ('22222222-2222-2222-2222-222222222222', 'X', 20),
                        ('33333333-3333-3333-3333-333333333333', 'C', 30),
                        (cast(null as UUID), 'N', 40),
                        ('44444444-4444-4444-4444-444444444444', null, 50)""");

            assertQuery("SELECT m.id, m.sym, m.v, s.v FROM m JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            id\tsym\tv\tv1
                            11111111-1111-1111-1111-111111111111\tA\t1\t10
                            33333333-3333-3333-3333-333333333333\tC\t3\t30
                            \tN\t4\t40
                            44444444-4444-4444-4444-444444444444\t\t5\t50
                            """);
        });
    }

    @Test
    public void testHashJoinSymbolKeysSwap() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym1 SYMBOL, sym2 SYMBOL, v INT)");
            execute("CREATE TABLE s (sym1 SYMBOL, sym2 SYMBOL, v INT)");
            // Master has 2 rows, slave has 6 rows -> swap fires.
            execute("""
                    INSERT INTO m VALUES
                        ('A', 'X', 1),
                        ('B', 'Y', 2)""");
            execute("""
                    INSERT INTO s VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', 11),
                        ('B', 'X', 12),
                        ('B', 'Y', 13),
                        ('C', 'X', 14),
                        ('C', 'Y', 15)""");

            assertQuery("SELECT m.sym1, m.sym2, m.v, s.v FROM m JOIN s ON m.sym1 = s.sym1 AND m.sym2 = s.sym2 ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            sym1\tsym2\tv\tv1
                            A\tX\t1\t10
                            B\tY\t2\t13
                            """);
        });
    }

    @Test
    public void testHashJoinSymbolKeysSwapWithCrossColumnIndices() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym1 SYMBOL, sym2 SYMBOL, v INT)");
            execute("CREATE TABLE s (sym2 SYMBOL, sym1 SYMBOL, v INT)");
            execute("""
                    INSERT INTO m VALUES
                        ('A', 'X', 1),
                        ('B', 'Y', 2)""");
            execute("""
                    INSERT INTO s VALUES
                        ('X', 'A', 10),
                        ('Y', 'A', 11),
                        ('X', 'B', 12),
                        ('Y', 'B', 13),
                        ('X', 'C', 14),
                        ('Y', 'C', 15)""");

            assertQuery("SELECT m.sym1, m.sym2, m.v, s.v FROM m JOIN s ON m.sym1 = s.sym1 AND m.sym2 = s.sym2 ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            sym1\tsym2\tv\tv1
                            A\tX\t1\t10
                            B\tY\t2\t13
                            """);
        });
    }

    @Test
    public void testHashJoinSymbolKeysSymbolMissingFromOtherSide() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, v INT)");
            execute("CREATE TABLE s (sym SYMBOL, v INT)");
            execute("""
                    INSERT INTO m VALUES
                        ('A', 1),
                        ('B', 2),
                        ('C', 3)""");
            execute("""
                    INSERT INTO s VALUES
                        ('A', 10),
                        ('B', 20),
                        ('D', 40)""");

            assertQuery("SELECT * FROM m JOIN s ON m.sym = s.sym ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            sym\tv\tsym1\tv1
                            A\t1\tA\t10
                            B\t2\tB\t20
                            """);
            assertQuery("SELECT * FROM m LEFT JOIN s ON m.sym = s.sym ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            sym\tv\tsym1\tv1
                            A\t1\tA\t10
                            B\t2\tB\t20
                            C\t3\t\tnull
                            """);
            assertQuery("SELECT * FROM m RIGHT JOIN s ON m.sym = s.sym ORDER BY s.v")
                    .noLeakCheck()
                    .returns("""
                            sym\tv\tsym1\tv1
                            A\t1\tA\t10
                            B\t2\tB\t20
                            \tnull\tD\t40
                            """);
            assertQuery("SELECT * FROM m FULL JOIN s ON m.sym = s.sym ORDER BY m.v, s.v")
                    .noLeakCheck()
                    .returns("""
                            sym	v	sym1	v1
                            	null	D	40
                            A	1	A	10
                            B	2	B	20
                            C	3		null
                            """);
        });
    }

    @Test
    public void testHashOuterJoinSymbolAndDecimalKeys() throws Exception {
        assertMemoryLeak(() -> {
            assertHashOuterJoinSymbolAndDecimalKey("dec8", "DECIMAL(2,1)", "1.2", "2.3", "3.4");
            assertHashOuterJoinSymbolAndDecimalKey("dec16", "DECIMAL(4,2)", "12.34", "23.45", "34.56");
            assertHashOuterJoinSymbolAndDecimalKey("dec32", "DECIMAL(9,4)", "12345.6789", "23456.7890", "34567.8901");
            assertHashOuterJoinSymbolAndDecimalKey("dec64", "DECIMAL(18,6)", "123456789012.345678", "234567890123.456789", "345678901234.567890");
            assertHashOuterJoinSymbolAndDecimalKey(
                    "dec128",
                    "DECIMAL(38,10)",
                    "1234567890123456789012345678.9012345678",
                    "2345678901234567890123456789.0123456789",
                    "3456789012345678901234567890.1234567890"
            );
            assertHashOuterJoinSymbolAndDecimalKey(
                    "dec256",
                    "DECIMAL(76,20)",
                    "12345678901234567890123456789012345678901234567890123456.78901234567890123456",
                    "23456789012345678901234567890123456789012345678901234567.89012345678901234567",
                    "34567890123456789012345678901234567890123456789012345678.90123456789012345678"
            );
        });
    }

    @Test
    public void testHashOuterJoinSymbolAndUuidKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id UUID, sym SYMBOL, v INT)");
            execute("CREATE TABLE s (id UUID, sym SYMBOL, v INT)");
            execute("""
                    INSERT INTO m VALUES
                        ('11111111-1111-1111-1111-111111111111', 'A', 1),
                        ('22222222-2222-2222-2222-222222222222', 'B', 2),
                        ('33333333-3333-3333-3333-333333333333', 'C', 3),
                        (cast(null as UUID), 'N', 4),
                        ('44444444-4444-4444-4444-444444444444', null, 5)""");
            execute("""
                    INSERT INTO s VALUES
                        ('11111111-1111-1111-1111-111111111111', 'A', 10),
                        ('22222222-2222-2222-2222-222222222222', 'X', 20),
                        ('33333333-3333-3333-3333-333333333333', 'C', 30),
                        (cast(null as UUID), 'N', 40),
                        ('44444444-4444-4444-4444-444444444444', null, 50)""");

            assertQuery("SELECT m.id, m.sym, m.v, s.v FROM m LEFT JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v")
                    .noLeakCheck()
                    .returns("""
                            id\tsym\tv\tv1
                            11111111-1111-1111-1111-111111111111\tA\t1\t10
                            22222222-2222-2222-2222-222222222222\tB\t2\tnull
                            33333333-3333-3333-3333-333333333333\tC\t3\t30
                            \tN\t4\t40
                            44444444-4444-4444-4444-444444444444\t\t5\t50
                            """);
        });
    }

    @Test
    public void testHashOuterJoinSymbolKeysSwapFullOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym1 SYMBOL, sym2 SYMBOL, v INT)");
            execute("CREATE TABLE s (sym1 SYMBOL, sym2 SYMBOL, v INT)");
            execute("""
                    INSERT INTO m VALUES
                        ('A', 'X', 1),
                        ('C', 'Z', 3)""");
            execute("""
                    INSERT INTO s VALUES
                        ('A', 'X', 10),
                        ('B', 'Y', 20),
                        ('D', 'W', 30),
                        ('E', 'V', 40),
                        ('F', 'U', 50)""");

            assertQuery("SELECT * FROM m FULL JOIN s ON m.sym1 = s.sym1 AND m.sym2 = s.sym2 ORDER BY m.v, s.v")
                    .noLeakCheck()
                    .returns("""
                            sym1	sym2	v	sym11	sym21	v1
                            		null	B	Y	20
                            		null	D	W	30
                            		null	E	V	40
                            		null	F	U	50
                            A	X	1	A	X	10
                            C	Z	3			null
                            """);
        });
    }

    @Test
    public void testHashOuterJoinWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (i long, locale_name symbol )");
            execute("create table tabb (i long, state symbol, city symbol)");
            execute("insert into taba values (1, 'pl')");
            execute("insert into tabb values (1, 'a', 'pl')");
            execute("insert into tabb values (1, 'b', 'b')");

            assertQuery("select * from taba left join tabb on taba.i = tabb.i and (locale_name = state OR locale_name=city)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tlocale_name\ti1\tstate\tcity
                            1\tpl\t1\ta\tpl
                            """);
            assertQuery("select * from taba right join tabb on taba.i = tabb.i and (locale_name = state OR locale_name=city)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tlocale_name\ti1\tstate\tcity
                            1\tpl\t1\ta\tpl
                            null\t\t1\tb\tb
                            """);
            assertQuery("select * from taba full join tabb on taba.i = tabb.i and (locale_name = state OR locale_name=city)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tlocale_name\ti1\tstate\tcity
                            1\tpl\t1\ta\tpl
                            null\t\t1\tb\tb
                            """);
        });
    }

    private void assertHashJoinSymbolAndDecimalKey(String tableSuffix, String decimalType, String id1, String id2, String id3) throws Exception {
        final String mTableName = "m_" + tableSuffix;
        final String sTableName = "s_" + tableSuffix;
        execute("CREATE TABLE " + mTableName + " (id " + decimalType + ", sym SYMBOL, v INT)");
        execute("CREATE TABLE " + sTableName + " (id " + decimalType + ", sym SYMBOL, v INT)");
        execute("""
                INSERT INTO %s VALUES
                    (%s::%s, 'A', 1),
                    (%s::%s, 'B', 2),
                    (%s::%s, 'C', 3),
                    (cast(null as %s), 'N', 4),
                    (%s::%s, null, 5)""".formatted(mTableName, id1, decimalType, id2, decimalType, id3, decimalType, decimalType, id1, decimalType));
        execute("""
                INSERT INTO %s VALUES
                    (%s::%s, 'A', 10),
                    (%s::%s, 'X', 20),
                    (%s::%s, 'C', 30),
                    (cast(null as %s), 'N', 40),
                    (%s::%s, null, 50)""".formatted(sTableName, id1, decimalType, id2, decimalType, id3, decimalType, decimalType, id1, decimalType));
        final String query = """
                SELECT %s.v, %s.v
                FROM %s
                JOIN %s ON %s.id = %s.id AND %s.sym = %s.sym
                ORDER BY %s.v
                """.formatted(
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName
        );

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        v\tv1
                        1\t10
                        3\t30
                        4\t40
                        5\t50
                        """);
    }

    private void assertHashOuterJoinSymbolAndDecimalKey(String tableSuffix, String decimalType, String id1, String id2, String id3) throws Exception {
        final String mTableName = "m_" + tableSuffix;
        final String sTableName = "s_" + tableSuffix;
        execute("CREATE TABLE " + mTableName + " (id " + decimalType + ", sym SYMBOL, v INT)");
        execute("CREATE TABLE " + sTableName + " (id " + decimalType + ", sym SYMBOL, v INT)");
        execute("""
                INSERT INTO %s VALUES
                    (%s::%s, 'A', 1),
                    (%s::%s, 'B', 2),
                    (%s::%s, 'C', 3),
                    (cast(null as %s), 'N', 4),
                    (%s::%s, null, 5)""".formatted(mTableName, id1, decimalType, id2, decimalType, id3, decimalType, decimalType, id1, decimalType));
        execute("""
                INSERT INTO %s VALUES
                    (%s::%s, 'A', 10),
                    (%s::%s, 'X', 20),
                    (%s::%s, 'C', 30),
                    (cast(null as %s), 'N', 40),
                    (%s::%s, null, 50)""".formatted(sTableName, id1, decimalType, id2, decimalType, id3, decimalType, decimalType, id1, decimalType));
        final String query = """
                SELECT %s.v, %s.v
                FROM %s
                LEFT JOIN %s ON %s.id = %s.id AND %s.sym = %s.sym
                ORDER BY %s.v
                """.formatted(
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName,
                sTableName,
                mTableName
        );

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        v\tv1
                        1\t10
                        2\tnull
                        3\t30
                        4\t40
                        5\t50
                        """);
    }

}
