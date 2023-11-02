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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.griffin.InsertNullTest.expectedNullInserts;

public class InsertNullGeoHashTest extends AbstractCairoTest {

    private static final int NULL_INSERTS = 15;

    @Test
    public void testInsertGeoNullByte() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table g(a geohash(4b))");
            insert("insert into g values (cast(null as geohash(5b)))");
            assertSql("a\n\n", "g");
        });
    }

    @Test
    public void testInsertGeoNullInt() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table g(a geohash(22b))");
            insert("insert into g values (cast(null as geohash(24b)))");
            assertSql("a\n\n", "g");
        });
    }

    @Test
    public void testInsertGeoNullLong() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table g(a geohash(42b))");
            insert("insert into g values (cast(null as geohash(44b)))");
            assertSql("a\n\n", "g");
        });
    }

    @Test
    public void testInsertGeoNullShort() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table g(a geohash(12b))");
            insert("insert into g values (cast(null as geohash(14b)))");
            assertSql("a\n\n", "g");
        });
    }

    @Test
    public void testInsertNullGeoHash() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("", NULL_INSERTS, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterEq1() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where geohash = null", NULL_INSERTS, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterEq2() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where null = geohash", NULL_INSERTS, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterEq3() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where geohash = geohash", NULL_INSERTS, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterNotEq1() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where geohash != null", 0, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterNotEq2() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where null != geohash", 0, true);
    }

    @Test
    public void testInsertNullGeoHashThenFilterNotEq3() throws Exception {
        assertGeoHashQueryForAllValidBitSizes("where geohash != geohash", 0, false);
    }

    private void assertGeoHashQueryForAllValidBitSizes(String queryExtra,
                                                       int expectedEmptyLines,
                                                       boolean supportsRandomAccess) throws Exception {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            if (b > 1) {
                setUp();
            }
            try {
                assertQuery(
                        "geohash\n",
                        "geohash " + queryExtra,
                        String.format(
                                "create table geohash (geohash %s)",
                                ColumnType.nameOf(ColumnType.getGeoHashTypeWithBits(b))),
                        null,
                        String.format(
                                "insert into geohash select null from long_sequence(%d)",
                                expectedEmptyLines),
                        expectedNullInserts("geohash\n", "", expectedEmptyLines, true),
                        supportsRandomAccess,
                        expectedEmptyLines > 0,
                        expectedEmptyLines > 0
                );
            } finally {
                tearDown();
            }
        }
    }
}
