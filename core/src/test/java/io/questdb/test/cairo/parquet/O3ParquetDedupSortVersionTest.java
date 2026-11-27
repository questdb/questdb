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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The dedup sort version of the row groups that an O3 commit writes into a parquet partition, see
 * {@link ParquetMetaFileReader#getRowGroupDedupSortVersion}: a row group claims the commit's dedup version when its
 * rows are in dedup key order within each timestamp, and makes no claim otherwise. Every test reads the row groups
 * of partition 2024-01-01 of table {@code pt} as stored, see {@link #readRowGroups}: the claim and the rows of each
 * row group, in storage order.
 * <p>
 * The partition is converted with row groups of 4 rows unless the test says otherwise, and the table holds one row
 * on 2024-01-02. The O3 commit updates the parquet file in place or rewrites it, see {@link #setProperties()}.
 * <p>
 * A claim holds for the rows as a reader decodes them. Parquet stores every NaN of a FLOAT or a DOUBLE as a NULL,
 * which decodes as one NaN, and WAL apply compares every NaN as that one NaN: a row group claims a version when a
 * dedup key has such a type, see the tests of {@code DOUBLE} and {@code FLOAT} keys.
 */
@RunWith(Parameterized.class)
public class O3ParquetDedupSortVersionTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    // rows of 2024-01-01 of createFloatingPointKeyTable() in no key order
    private static final String FLOATING_POINT_KEY_STORED_ROWS = """
            ('2024-01-01T01:00:00.000000Z', 4, 1, 1),
            ('2024-01-01T01:00:00.000000Z', 2, 2, 1),
            ('2024-01-01T02:00:00.000000Z', 3, 3, 1),
            ('2024-01-01T02:00:00.000000Z', 1, 4, 1),
            ('2024-01-01T03:00:00.000000Z', 4, 5, 1),
            ('2024-01-01T03:00:00.000000Z', 2, 6, 1),
            ('2024-01-01T04:00:00.000000Z', 3, 7, 1),
            ('2024-01-01T04:00:00.000000Z', 1, 8, 1)
            """;
    private static final String FLOATING_POINT_KEY_STORED_ROW_GROUP_0 = "- | 1=1/1 1=2/1 2=3/1 2=4/1\n";
    private static final String FLOATING_POINT_KEY_STORED_ROW_GROUP_1 = "- | 3=5/1 3=6/1 4=7/1 4=8/1\n";
    // the bits of the NaN that the negation of a NULL gives: the NULL of the type with the sign bit set
    private static final long NEGATED_NULL_DOUBLE_BITS = 0xFFF8_0000_0000_0000L;
    private static final int NEGATED_NULL_FLOAT_BITS = 0xFFC0_0000;
    // rows of 2024-01-01 in no key order, 'b' is symbol key 0 and 'a' symbol key 1
    private static final String STORED_ROWS = """
            ('2024-01-01T01:00:00.000000Z', 9, 'b', 1),
            ('2024-01-01T01:00:00.000000Z', 7, 'b', 2),
            ('2024-01-01T01:00:00.000000Z', 7, 'a', 3),
            ('2024-01-01T01:00:00.000000Z', 3, 'b', 4),
            ('2024-01-01T03:00:00.000000Z', 8, 'b', 5),
            ('2024-01-01T03:00:00.000000Z', 6, 'b', 6),
            ('2024-01-01T04:00:00.000000Z', 4, 'b', 7),
            ('2024-01-01T04:00:00.000000Z', 2, 'b', 8)
            """;
    private static final String STORED_ROW_GROUP_0 = "- | 1=9/0/1 1=7/0/2 1=7/1/3 1=3/0/4\n";
    private static final String STORED_ROW_GROUP_1 = "- | 3=8/0/5 3=6/0/6 4=4/0/7 4=2/0/8\n";
    private final boolean isRewrite;
    private final TestTimestampType timestampType;

    public O3ParquetDedupSortVersionTest(boolean isRewrite, TestTimestampType timestampType) {
        this.isRewrite = isRewrite;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "rewrite={0}-{1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (boolean isRewrite : new boolean[]{false, true}) {
            for (TestTimestampType timestampType : TestTimestampType.values()) {
                params.add(new Object[]{isRewrite, timestampType});
            }
        }
        return params;
    }

    @Test
    public void testCoalescedMergeWithNewKeyClaimsCommitVersion() throws Exception {
        // row groups 01,02,03,03 | 03,03,04,05: 03:00 ties their boundary, so a commit at 03:00 merges both as one
        setProperties();
        assertMemoryLeak(() -> {
            createTable("""
                    ('2024-01-01T01:00:00.000000Z', 1, 'b', 1),
                    ('2024-01-01T02:00:00.000000Z', 1, 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 9, 'b', 3),
                    ('2024-01-01T03:00:00.000000Z', 7, 'b', 4),
                    ('2024-01-01T03:00:00.000000Z', 5, 'b', 5),
                    ('2024-01-01T03:00:00.000000Z', 3, 'b', 6),
                    ('2024-01-01T04:00:00.000000Z', 1, 'b', 7),
                    ('2024-01-01T05:00:00.000000Z', 1, 'b', 8)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups("""
                    - | 1=1/0/1 2=1/0/2 3=9/0/3 3=7/0/4
                    - | 3=5/0/5 3=3/0/6 4=1/0/7 5=1/0/8
                    """);

            // a new key: the nine rows of the merge are in key order, and so is each of the two row groups it writes
            insert("('2024-01-01T03:00:00.000000Z', 6, 'b', 100)");
            assertRowGroups("""
                    cur | 1=1/0/1 2=1/0/2 3=3/0/6 3=5/0/5 3=6/0/100
                    cur | 3=7/0/4 3=9/0/3 4=1/0/7 5=1/0/8
                    """);

            // A replacement at the tied timestamp merges both row groups again. The merge splits its rows wherever
            // its row count puts the boundary, so the claims of the row groups it read do not carry over to the ones
            // it writes: the merge checks the key order of all its rows, and claims the commit's version.
            insert("('2024-01-01T03:00:00.000000Z', 6, 'b', 200)");
            assertRowGroups("""
                    cur | 1=1/0/1 2=1/0/2 3=3/0/6 3=5/0/5 3=6/0/200
                    cur | 3=7/0/4 3=9/0/3 4=1/0/7 5=1/0/8
                    """);
        });
    }

    @Test
    public void testCopyWithSchemaChangeMakesNoClaim() throws Exception {
        // A rewrite that copies a row group into a new schema makes no claim for the copy: a column that the copy
        // drops or reads as NULL may be a key of the version the row group claims. The merged row group claims.
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);

            execute("ALTER TABLE pt ADD COLUMN x INT");
            insert("('2024-01-01T04:00:00.000000Z', 3, 'b', 200, 1)");
            assertRowGroups("""
                    - | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1
                    cur | 3=6/0/6 3=8/0/5 4=2/0/8 4=3/0/200 4=4/0/7
                    """);
        });
    }

    @Test
    public void testDedupSettingChangeClaimsNewVersion() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);

            // Other keys are another dedup version, which the row group does not claim. The merge of a replacement
            // puts the rows in the order of the new keys, which they already have, and claims the new version.
            enableDedup("UPSERT KEYS(ts, k)");
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 200)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/200 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);

            // a new key puts the rows in the order of the new keys
            insert("('2024-01-01T01:00:00.000000Z', 4, 'b', 300)");
            assertRowGroups("""
                    cur | 1=3/0/4 1=4/0/300 1=5/0/200 1=7/0/2 1=7/1/3 1=9/0/1
                    """ + STORED_ROW_GROUP_1);

            // without dedup an O3 row goes after the stored rows of its timestamp, in no key order
            execute("ALTER TABLE pt DEDUP DISABLE");
            insert("('2024-01-01T01:00:00.000000Z', 0, 'b', 400)");
            assertRowGroups("""
                    - | 1=3/0/4 1=4/0/300 1=5/0/200 1=7/0/2
                    - | 1=7/1/3 1=9/0/1 1=0/0/400
                    """ + STORED_ROW_GROUP_1);
        });
    }

    @Test
    public void testDoubleKeyMergeWithNewKeyClaimsCommitVersion() throws Exception {
        assertFloatingPointKeyMergeWithNewKeyClaimsCommitVersion("DOUBLE");
    }

    @Test
    public void testDoubleKeyNewRowGroupFromO3RowsClaimsCommitVersion() throws Exception {
        assertFloatingPointKeyNewRowGroupFromO3RowsClaimsCommitVersion("DOUBLE", NEGATED_NULL_DOUBLE_BITS);
    }

    @Test
    public void testDoubleKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim() throws Exception {
        assertFloatingPointKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim("DOUBLE");
    }

    @Test
    public void testFloatKeyMergeWithNewKeyClaimsCommitVersion() throws Exception {
        assertFloatingPointKeyMergeWithNewKeyClaimsCommitVersion("FLOAT");
    }

    @Test
    public void testFloatKeyNewRowGroupFromO3RowsClaimsCommitVersion() throws Exception {
        assertFloatingPointKeyNewRowGroupFromO3RowsClaimsCommitVersion("FLOAT", NEGATED_NULL_FLOAT_BITS);
    }

    @Test
    public void testFloatKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim() throws Exception {
        assertFloatingPointKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim("FLOAT");
    }

    @Test
    public void testKeyTypeChangeThatReordersKeysLeavesNoClaim() throws Exception {
        // The converted column follows s in the table while it precedes it by column id: WAL apply sorts the keys
        // in table order and the claim promises column id order, so no commit claims its version.
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            execute("ALTER TABLE pt ALTER COLUMN k TYPE INT");
            insert("""
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 100),
                    ('2024-01-01T02:00:00.000000Z', 2, 'b', 101),
                    ('2024-01-01T02:00:00.000000Z', 1, 'b', 102)
                    """);
            assertQuery("SELECT count() FROM pt WHERE ts IN '" + DAY + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n11\n");
            try (TableReader reader = engine.getReader("pt")) {
                reader.openPartition(0);
                final ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
                Assert.assertEquals(3, meta.getRowGroupCount());
                for (int rowGroup = 0, n = meta.getRowGroupCount(); rowGroup < n; rowGroup++) {
                    Assert.assertEquals(
                            "row group " + rowGroup,
                            ParquetMetaFileReader.DEDUP_SORT_VERSION_UNSET,
                            meta.getRowGroupDedupSortVersion(rowGroup)
                    );
                }
            }
        });
    }

    @Test
    public void testKeyTypesThatParquetKeepsClaimCommitVersion() throws Exception {
        // Each case is a key type and keys of it in ascending order, as WAL apply compares them and as the row
        // groups must hold them: a fixed-size value as a signed integer of its size, a var-size value by its length
        // and then by its bytes. Parquet stores and decodes each of these values with the bytes the commit had.
        setProperties();
        assertMemoryLeak(() -> {
            assertKeyTypeClaimsCommitVersion("BOOLEAN", "false", "true");
            assertKeyTypeClaimsCommitVersion("BYTE", "-5", "0", "3", "7");
            assertKeyTypeClaimsCommitVersion("SHORT", "-5", "0", "3", "7");
            // a char above 0x7FFF is a negative 16-bit integer
            assertKeyTypeClaimsCommitVersion("CHAR", "'\uAC00'", "'a'", "'b'", "'\u4E2D'");
            assertKeyTypeClaimsCommitVersion("INT", "NULL", "-5", "3", "7");
            assertKeyTypeClaimsCommitVersion("LONG", "NULL", "-5", "3", "7");
            // a FLOAT or DOUBLE compares its bits as a signed integer, so -1 sorts before -5, and every NaN, which is
            // NULL, as the one NaN, which sorts after the numbers
            assertKeyTypeClaimsCommitVersion("FLOAT", "-1.0", "-5.0", "3.0", "NULL");
            assertKeyTypeClaimsCommitVersion("DOUBLE", "-1.0", "-5.0", "3.0", "NULL");
            assertKeyTypeClaimsCommitVersion("DATE", "NULL", "'1969-12-31T00:00:00.000Z'", "'1970-01-02T00:00:00.000Z'", "'2024-01-01T00:00:00.000Z'");
            assertKeyTypeClaimsCommitVersion("TIMESTAMP", "NULL", "'1969-12-31T00:00:00.000000Z'", "'1970-01-02T00:00:00.000000Z'", "'2024-01-01T00:00:00.000000Z'");
            assertKeyTypeClaimsCommitVersion("TIMESTAMP_NS", "NULL", "'1969-12-31T00:00:00.000000Z'", "'1970-01-02T00:00:00.000000Z'", "'2024-01-01T00:00:00.000000Z'");
            // a symbol compares by its symbol key, which follows the order the symbols came in: 2024-01-02 takes them
            // in the order below
            assertKeyTypeClaimsCommitVersion("SYMBOL", "NULL", "'z'", "'m'", "'a'");
            // an address above 127.255.255.255 is a negative 32-bit integer, and NULL is zero
            assertKeyTypeClaimsCommitVersion("IPv4", "'200.1.1.1'", "NULL", "'1.1.1.1'", "'100.1.1.1'");
            // a geohash is a non-negative integer and its NULL is -1
            assertKeyTypeClaimsCommitVersion("GEOHASH(1c)", "NULL", "#0", "#b", "#z");
            assertKeyTypeClaimsCommitVersion("GEOHASH(2c)", "NULL", "#00", "#b0", "#zz");
            assertKeyTypeClaimsCommitVersion("GEOHASH(4c)", "NULL", "#0000", "#b000", "#zzzz");
            assertKeyTypeClaimsCommitVersion("GEOHASH(8c)", "NULL", "#00000000", "#b0000000", "#zzzzzzzz");
            // a 128-bit integer: the first half of the text is its high long, and NULL is the smallest long twice
            assertKeyTypeClaimsCommitVersion(
                    "UUID",
                    "NULL",
                    "'f0000000-0000-0000-0000-000000000001'",
                    "'00000000-0000-0000-0000-000000000002'",
                    "'10000000-0000-0000-0000-000000000000'"
            );
            // the first 16 digits are the highest long of the four, and NULL is the smallest long four times
            assertKeyTypeClaimsCommitVersion(
                    "LONG256",
                    "NULL",
                    "0xf000000000000000000000000000000000000000000000000000000000000000",
                    "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000000000000000000000000000002"
            );
            // the length in bytes, -1 for NULL, then the bytes: 'zz' is 7A 7A and U+00E9 is C3 A9
            assertKeyTypeClaimsCommitVersion("VARCHAR", "NULL", "''", "'z'", "'ab'");
            assertKeyTypeClaimsCommitVersion("VARCHAR", "'zz'", "'\u00E9'", "'zzzzzzzzz'", "'aaaaaaaaaa'");
            assertKeyTypeClaimsCommitVersion("VARCHAR", "'aaaaaaaaab'", "'aaaaaaaaac'", "'aaaaaaaaaaa'");
            // The length in chars, -1 for NULL, then the bytes of the chars, the low byte of a char first: U+0100 is
            // 00 01 and 'z' is 7A 00, the surrogate pair of U+1F600 is 3D D8 00 DE and 'ab' is 61 00 62 00.
            assertKeyTypeClaimsCommitVersion("STRING", "NULL", "''", "'z'", "'ab'");
            assertKeyTypeClaimsCommitVersion("STRING", "'\u0100'", "'z'", "'\uD83D\uDE00'", "'ab'");
            // a decimal of up to 18 digits is an integer of 1, 2, 4 or 8 bytes, and its NULL the smallest one
            assertKeyTypeClaimsCommitVersion("DECIMAL(2,0)", "NULL", "-5m", "3m", "7m");
            assertKeyTypeClaimsCommitVersion("DECIMAL(4,0)", "NULL", "-5m", "3m", "7m");
            assertKeyTypeClaimsCommitVersion("DECIMAL(9,0)", "NULL", "-5m", "3m", "7m");
            assertKeyTypeClaimsCommitVersion("DECIMAL(18,0)", "NULL", "-5m", "3m", "7m");
            // A wider decimal stores its highest long first, where WAL apply reads the lowest long of an integer:
            // the long that decides is the lowest one of the decimal, as a signed long. It is -5 for -5 and 0 for
            // NULL, whose highest long is the smallest long and whose other longs are 0.
            assertKeyTypeClaimsCommitVersion("DECIMAL(38,0)", "-5m", "NULL", "3m", "7m");
            assertKeyTypeClaimsCommitVersion("DECIMAL(76,0)", "-5m", "NULL", "3m", "7m");
        });
    }

    @Test
    public void testMergeWithNewKeyClaimsCommitVersion() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups(STORED_ROW_GROUP_0 + STORED_ROW_GROUP_1);

            // A new key and a replacement at 01:00: the merge puts the rows of 01:00 in key order, a number as a
            // signed integer and a symbol by its key, as WAL apply compares them.
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100), ('2024-01-01T01:00:00.000000Z', 7, 'a', 101)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/101 1=9/0/1\n" + STORED_ROW_GROUP_1);
            assertUpdateMode();
            assertQuery("SELECT k, s, v FROM pt WHERE ts IN '" + DAY + "'")
                    .noLeakCheck()
                    .returns("""
                            k\ts\tv
                            3\tb\t4
                            5\tb\t100
                            7\tb\t2
                            7\ta\t101
                            9\tb\t1
                            8\tb\t5
                            6\tb\t6
                            4\tb\t7
                            2\tb\t8
                            """);

            // the claim survives a reopen of the table
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/101 1=9/0/1\n" + STORED_ROW_GROUP_1);
        });
    }

    @Test
    public void testMergeWithNewKeysClaimsEveryRowGroupItWrites() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            // seven rows are more than a row group and a half: the merge writes two row groups
            insert("""
                    ('2024-01-01T01:00:00.000000Z', 8, 'a', 100),
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 101),
                    ('2024-01-01T01:00:00.000000Z', 1, 'b', 102)
                    """);
            assertRowGroups("""
                    cur | 1=1/0/102 1=3/0/4 1=5/0/101 1=7/0/2
                    cur | 1=7/1/3 1=8/1/100 1=9/0/1
                    """ + STORED_ROW_GROUP_1);
            assertUpdateMode();
        });
    }

    @Test
    public void testMergeWithNullKeysClaimsCommitVersion() throws Exception {
        // k2 is added after the rows of 01:00, which lie below its column top, and holds NULLs at 03:00
        setProperties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (ts " + timestampType.getTypeName() + ", k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pt VALUES
                    ('2024-01-01T01:00:00.000000Z', 9, 'b', 1),
                    ('2024-01-01T01:00:00.000000Z', 7, 'b', 2),
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 3),
                    ('2024-01-01T01:00:00.000000Z', 3, 'b', 4),
                    ('2024-01-02T00:00:00.000000Z', 0, 'z', 99)
                    """);
            execute("ALTER TABLE pt ADD COLUMN k2 LONG");
            execute("""
                    INSERT INTO pt VALUES
                    ('2024-01-01T03:00:00.000000Z', 1, 'b', 5, 8),
                    ('2024-01-01T03:00:00.000000Z', 1, 'b', 6, NULL),
                    ('2024-01-01T03:00:00.000000Z', 1, 'b', 7, 6),
                    ('2024-01-01T03:00:00.000000Z', 1, 'b', 8, 4)
                    """);
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
            enableDedup("UPSERT KEYS(ts, k, s, k2)");
            assertRowGroups("""
                    - | 1=9/0/1/null 1=7/0/2/null 1=5/0/3/null 1=3/0/4/null
                    - | 3=1/0/5/8 3=1/0/6/null 3=1/0/7/6 3=1/0/8/4
                    """);

            // At 01:00 a replacement of a row below the column top and a new key next to it, at 03:00 a new key:
            // a NULL key sorts first.
            insert("""
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 100, NULL),
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 101, 7),
                    ('2024-01-01T03:00:00.000000Z', 1, 'b', 102, 5)
                    """);
            assertRowGroups("""
                    cur | 1=3/0/4/null 1=5/0/100/null 1=5/0/101/7 1=7/0/2/null 1=9/0/1/null
                    cur | 3=1/0/6/null 3=1/0/8/4 3=1/0/102/5 3=1/0/7/6 3=1/0/5/8
                    """);
        });
    }

    @Test
    public void testNewPartitionFromO3RowsClaimsCommitVersion() throws Exception {
        // A FORMAT PARQUET table writes a new partition from the rows of the commit, in the key order WAL apply gave
        // them: every row group claims the commit's version.
        setProperties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (ts " + timestampType.getTypeName() + ", k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL DEDUP UPSERT KEYS(ts, k, s)");
            insert(STORED_ROWS);
            assertRowGroups("""
                    cur | 1=3/0/4 1=7/0/2 1=7/1/3 1=9/0/1
                    cur | 3=6/0/6 3=8/0/5 4=2/0/8 4=4/0/7
                    """);
        });
    }

    @Test
    public void testNewRowGroupFromO3RowsClaimsCommitVersion() throws Exception {
        // The O3 rows at 02:00 lie between the two row groups and the ones at 05:00 after the last one: each run
        // becomes a row group of its own, which holds the commit's rows in the key order WAL apply gave them.
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("""
                    ('2024-01-01T05:00:00.000000Z', 4, 'b', 100),
                    ('2024-01-01T02:00:00.000000Z', 6, 'a', 101),
                    ('2024-01-01T05:00:00.000000Z', 2, 'a', 102),
                    ('2024-01-01T02:00:00.000000Z', 6, 'b', 103),
                    ('2024-01-01T05:00:00.000000Z', 2, 'b', 104),
                    ('2024-01-01T02:00:00.000000Z', 1, 'a', 105),
                    ('2024-01-01T05:00:00.000000Z', 3, 'b', 106)
                    """);
            assertRowGroups(STORED_ROW_GROUP_0
                    + "cur | 2=1/1/105 2=6/0/103 2=6/1/101\n"
                    + STORED_ROW_GROUP_1
                    + "cur | 5=2/0/104 5=2/1/102 5=3/0/106 5=4/0/100\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testNoClaimWithoutDedup() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            // a merge into the first row group and a new row group after the last one
            insert("""
                    ('2024-01-01T01:00:00.000000Z', 5, 'b', 100),
                    ('2024-01-01T05:00:00.000000Z', 4, 'b', 101),
                    ('2024-01-01T05:00:00.000000Z', 2, 'b', 102)
                    """);
            assertRowGroups("- | 1=9/0/1 1=7/0/2 1=7/1/3 1=3/0/4 1=5/0/100\n"
                    + STORED_ROW_GROUP_1
                    + "- | 5=4/0/101 5=2/0/102\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testRandomCommitsClaimNoRowGroupOutOfKeyOrder() throws Exception {
        // Random commits of new keys, replacements, new timestamps and dedup setting changes, with many rows per
        // timestamp. After each commit readRowGroups() checks the rows of every row group that claims the current
        // dedup version, and pt holds the rows of its native twin tw, which takes the same SQL.
        setProperties();
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final String[] dedupSettings = {"ENABLE UPSERT KEYS(ts, k, s)", "ENABLE UPSERT KEYS(ts, k)", "ENABLE UPSERT KEYS(ts)", "ENABLE SKIP KEYS(ts, k, s)", "DISABLE"};
        assertMemoryLeak(() -> {
            final String[] tables = {"pt", "tw"};
            for (String table : tables) {
                execute("CREATE TABLE " + table + " (ts " + timestampType.getTypeName() + ", k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + table + " VALUES ('2024-01-01T01:00:00.000000Z', 0, 'c', 0), ('2024-01-01T01:00:00.000000Z', 0, 'b', 0), "
                        + "('2024-01-01T01:00:00.000000Z', 0, 'a', 0), ('2024-01-02T00:00:00.000000Z', 0, 'a', 0)");
            }
            int v = 0;
            final StringBuilder rows = new StringBuilder();
            for (int i = 0; i < 24; i++) {
                rows.append(i > 0 ? ", " : "").append(randomRow(rnd, ++v));
            }
            for (String table : tables) {
                execute("INSERT INTO " + table + " VALUES " + rows);
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
            for (String table : tables) {
                execute("ALTER TABLE " + table + " DEDUP " + dedupSettings[0]);
            }
            drainWalQueue();

            int claimCount = 0;
            for (int commit = 0; commit < 30; commit++) {
                if (commit > 0 && rnd.nextInt(6) == 0) {
                    final String dedupSetting = dedupSettings[rnd.nextInt(dedupSettings.length)];
                    for (String table : tables) {
                        execute("ALTER TABLE " + table + " DEDUP " + dedupSetting);
                    }
                }
                rows.setLength(0);
                // the first commit adds a key that sorts first at a stored timestamp, so its merge claims a version
                rows.append(commit == 0 ? "('2024-01-01T01:00:00.000000Z', -1, 'a', 0)" : randomRow(rnd, ++v));
                for (int i = 0, n = rnd.nextInt(6); i < n; i++) {
                    rows.append(", ").append(randomRow(rnd, ++v));
                }
                for (String table : tables) {
                    execute("INSERT INTO " + table + " VALUES " + rows);
                }
                drainWalQueue();
                for (String table : tables) {
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
                }
                final String rowGroups = readRowGroups();
                for (int from = rowGroups.indexOf("cur |"); from > -1; from = rowGroups.indexOf("cur |", from + 1)) {
                    claimCount++;
                }
                assertSqlCursors("SELECT * FROM tw ORDER BY ts, k, s, v", "SELECT * FROM pt ORDER BY ts, k, s, v");
            }
            Assert.assertTrue(claimCount > 0);
        });
    }

    @Test
    public void testReplacementsKeepClaimOfRowGroupInKeyOrder() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);

            // the replacements keep the stored order, which the row group claims under the commit's dedup version
            insert("('2024-01-01T01:00:00.000000Z', 9, 'b', 200), ('2024-01-01T01:00:00.000000Z', 3, 'b', 201)");
            assertRowGroups("cur | 1=3/0/201 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/200\n" + STORED_ROW_GROUP_1);
            assertUpdateMode();
        });
    }

    @Test
    public void testReplacementsPutRowGroupWithoutClaimInKeyOrder() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            // the stored rows are in no key order and claim nothing: the merges put them in key order, as a native
            // merge does, and claim the commit's version
            insert("('2024-01-01T01:00:00.000000Z', 7, 'a', 100), ('2024-01-01T04:00:00.000000Z', 2, 'b', 101)");
            assertRowGroups("""
                    cur | 1=3/0/4 1=7/0/2 1=7/1/100 1=9/0/1
                    cur | 3=6/0/6 3=8/0/5 4=2/0/101 4=4/0/7
                    """);
            assertUpdateMode();
        });
    }

    @Test
    public void testRewriteCopyKeepsClaim() throws Exception {
        // the second commit merges into the second row group only: an in-place update leaves the first one in
        // place, a rewrite copies it, and either way it keeps its claim
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);

            insert("('2024-01-01T04:00:00.000000Z', 3, 'b', 200)");
            assertRowGroups("""
                    cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1
                    cur | 3=6/0/6 3=8/0/5 4=2/0/8 4=3/0/200 4=4/0/7
                    """);
            assertUpdateMode();
        });
    }

    @Test
    public void testSkipMergeWithNewKeyClaimsCommitVersion() throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("SKIP KEYS(ts, k, s)");
            // the row group keeps the stored row of the resent key and takes the new key
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 100), ('2024-01-01T01:00:00.000000Z', 7, 'a', 101)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);
            assertUpdateMode();

            // a commit of resent keys only writes nothing
            insert("('2024-01-01T01:00:00.000000Z', 5, 'b', 200), ('2024-01-01T03:00:00.000000Z', 8, 'b', 201)");
            assertRowGroups("cur | 1=3/0/4 1=5/0/100 1=7/0/2 1=7/1/3 1=9/0/1\n" + STORED_ROW_GROUP_1);
        });
    }

    @Test
    public void testTimestampOnlyDedupClaimsCommitVersion() throws Exception {
        // the designated timestamp is the only key: any order of the rows of a timestamp is the key order
        setProperties();
        assertMemoryLeak(() -> {
            createTable(STORED_ROWS);
            enableDedup("UPSERT KEYS(ts)");
            // the row at 03:00 replaces both stored rows of 03:00, the row at 03:30 is new
            insert("('2024-01-01T03:00:00.000000Z', 0, 'b', 100), ('2024-01-01T03:30:00.000000Z', 0, 'b', 101)");
            assertRowGroups(STORED_ROW_GROUP_0 + "cur | 3=0/0/100 3=0/0/100 3=0/0/101 4=4/0/7 4=2/0/8\n");
            assertUpdateMode();

            // An in-place update leaves the second row group and its claim in place. A rewrite copies the row group
            // and its claim.
            insert("('2024-01-01T01:00:00.000000Z', 0, 'a', 200)");
            assertRowGroups("cur | 1=0/1/200 1=0/1/200 1=0/1/200 1=0/1/200\n"
                    + "cur | 3=0/0/100 3=0/0/100 3=0/0/101 4=4/0/7 4=2/0/8\n");
        });
    }

    @Test
    public void testUpsertOfIdenticalRowsInKeyOrderWithRowsPastPartitionMaxClaimsCommitVersion() throws Exception {
        // As testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsStoredOrderWithoutClaim, with the stored rows of
        // the last row group in key order although they claim nothing: the merge keeps them as they are, checks
        // their key order and claims the commit's version.
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            createSmallLastRowGroupTable("""
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups(firstRowGroupOf16() + "- | 5=7/0/50/1 5=9/0/50/1\n");

            insert("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T06:00:00.000000Z', 2, 'b', 200, 1),
                    ('2024-01-01T06:00:00.000000Z', 1, 'b', 201, 1)
                    """);
            assertRowGroups(firstRowGroupOf16() + "cur | 5=7/0/50/1 5=9/0/50/1 6=1/0/201/1 6=2/0/200/1\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim() throws Exception {
        // As testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsStoredOrderWithoutClaim, with the last row
        // group in key order: the commit before put it there.
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            createSmallLastRowGroupTable("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            insert("('2024-01-01T05:00:00.000000Z', 8, 'b', 100, 1)");
            assertRowGroups(firstRowGroupOf16() + "cur | 5=7/0/50/1 5=8/0/100/1 5=9/0/50/1\n");

            insert("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T06:00:00.000000Z', 2, 'b', 200, 1),
                    ('2024-01-01T06:00:00.000000Z', 1, 'b', 201, 1)
                    """);
            assertRowGroups(firstRowGroupOf16() + "cur | 5=7/0/50/1 5=8/0/100/1 5=9/0/50/1 6=1/0/201/1 6=2/0/200/1\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsStoredOrderWithoutClaim() throws Exception {
        // The last row group holds two rows, few enough for its merge to take the O3 rows past the partition max
        // too. The O3 rows at 05:00 hold the values of the rows they replace, and the table has a BYTE column: the
        // merge keeps the stored rows as they are, in their stored order, which is no key order, and adds the rows
        // of 06:00 after them.
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            createSmallLastRowGroupTable("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups(firstRowGroupOf16() + "- | 5=9/0/50/1 5=7/0/50/1\n");

            insert("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T06:00:00.000000Z', 2, 'b', 200, 1),
                    ('2024-01-01T06:00:00.000000Z', 1, 'b', 201, 1)
                    """);
            assertRowGroups(firstRowGroupOf16() + "- | 5=9/0/50/1 5=7/0/50/1 6=1/0/201/1 6=2/0/200/1\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testUpsertOfSwappedValuesAfterKeptRowWithRowsPastPartitionMaxReplacesRows() throws Exception {
        // As testUpsertOfSwappedValuesWithRowsPastPartitionMaxReplacesRows, with a row before the commit's first
        // timestamp in the last row group: the output keeps the column tops below it, and finding that row count
        // prepares the merge of the row group before the UPSERT identity check takes it over.
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            createSmallLastRowGroupTable("""
                    ('2024-01-01T04:00:00.000000Z', 1, 'b', 40, 1),
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 60, 1)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups(firstRowGroupOf16() + "- | 4=1/0/40/1 5=9/0/50/1 5=7/0/60/1\n");

            insert("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 60, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T06:00:00.000000Z', 2, 'b', 200, 1),
                    ('2024-01-01T06:00:00.000000Z', 1, 'b', 201, 1)
                    """);
            assertQuery("SELECT ts, k, v FROM pt WHERE ts >= '2024-01-01T04:00:00.000000Z' AND ts < '2024-01-02'")
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            ts\tk\tv
                            2024-01-01T04:00:00.000000Z\t1\t40
                            2024-01-01T05:00:00.000000Z\t7\t50
                            2024-01-01T05:00:00.000000Z\t9\t60
                            2024-01-01T06:00:00.000000Z\t1\t201
                            2024-01-01T06:00:00.000000Z\t2\t200
                            """, timestampType.getTypeName()));
            // the merge has new keys, so it writes its rows in key order and claims the commit's version
            assertRowGroups(firstRowGroupOf16() + "cur | 4=1/0/40/1 5=7/0/50/1 5=9/0/60/1 6=1/0/201/1 6=2/0/200/1\n");
            assertUpdateMode();
        });
    }

    @Test
    public void testUpsertOfSwappedValuesWithRowsPastPartitionMaxReplacesRows() throws Exception {
        // As testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsStoredOrderWithoutClaim, with the values of the
        // two keys at 05:00 swapped by the commit: in key order the O3 values match the stored values by position,
        // in stored order they do not. The commit replaces both rows.
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            createSmallLastRowGroupTable("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 50, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 60, 1)
                    """);
            enableDedup("UPSERT KEYS(ts, k, s)");
            assertRowGroups(firstRowGroupOf16() + "- | 5=9/0/50/1 5=7/0/60/1\n");

            insert("""
                    ('2024-01-01T05:00:00.000000Z', 9, 'b', 60, 1),
                    ('2024-01-01T05:00:00.000000Z', 7, 'b', 50, 1),
                    ('2024-01-01T06:00:00.000000Z', 2, 'b', 200, 1),
                    ('2024-01-01T06:00:00.000000Z', 1, 'b', 201, 1)
                    """);
            assertQuery("SELECT ts, k, v FROM pt WHERE ts >= '2024-01-01T05:00:00.000000Z' AND ts < '2024-01-02'")
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            ts\tk\tv
                            2024-01-01T05:00:00.000000Z\t7\t50
                            2024-01-01T05:00:00.000000Z\t9\t60
                            2024-01-01T06:00:00.000000Z\t1\t201
                            2024-01-01T06:00:00.000000Z\t2\t200
                            """, timestampType.getTypeName()));
            // the merge has new keys, so it writes its rows in key order and claims the commit's version
            assertRowGroups(firstRowGroupOf16() + "cur | 5=7/0/50/1 5=9/0/60/1 6=1/0/201/1 6=2/0/200/1\n");
            assertUpdateMode();
        });
    }

    // Compares two fixed-size values of the given size as WAL apply compares them: as signed integers.
    private static int compareFixedSizeKeys(int size, long addr1, long addr2, int columnType) {
        return switch (size) {
            case Byte.BYTES -> Byte.compare(Unsafe.getUnsafe().getByte(addr1), Unsafe.getUnsafe().getByte(addr2));
            case Short.BYTES -> Short.compare(Unsafe.getUnsafe().getShort(addr1), Unsafe.getUnsafe().getShort(addr2));
            case Integer.BYTES -> Integer.compare(Unsafe.getUnsafe().getInt(addr1), Unsafe.getUnsafe().getInt(addr2));
            case Long.BYTES -> Long.compare(Unsafe.getUnsafe().getLong(addr1), Unsafe.getUnsafe().getLong(addr2));
            case 2 * Long.BYTES -> compareInt128(addr1, addr2);
            case 4 * Long.BYTES -> {
                final int diff = compareInt128(addr1 + 2 * Long.BYTES, addr2 + 2 * Long.BYTES);
                yield diff != 0 ? diff : compareInt128(addr1, addr2);
            }
            default -> throw new AssertionError("unexpected column type: " + ColumnType.nameOf(columnType));
        };
    }

    // Compares the 16 bytes at the two addresses as WAL apply compares a 16-byte dedup key: as signed 128-bit
    // integers, the low long first.
    private static int compareInt128(long addr1, long addr2) {
        final int diff = Long.compare(Unsafe.getUnsafe().getLong(addr1 + Long.BYTES), Unsafe.getUnsafe().getLong(addr2 + Long.BYTES));
        return diff != 0 ? diff : Long.compareUnsigned(Unsafe.getUnsafe().getLong(addr1), Unsafe.getUnsafe().getLong(addr2));
    }

    // Compares the values of a dedup key column in two rows of a decoded row group as WAL apply compares them. A
    // fixed-size value is a signed integer of its size, whatever its type: the bits of a FLOAT or a DOUBLE too, with
    // every NaN as the one NaN.
    // WAL apply takes 32 bytes as two signed 128-bit integers, the high one last. A var-size value compares by its
    // length, -1 for NULL, and then by its bytes.
    private static int compareKeys(RowGroupBuffers buffers, int chunk, int columnType, int row1, int row2) {
        if (ColumnType.isVarSize(columnType)) {
            final byte[] value1 = readVarSizeKey(buffers, chunk, columnType, row1);
            final byte[] value2 = readVarSizeKey(buffers, chunk, columnType, row2);
            final int length1 = value1 != null ? value1.length : -1;
            final int length2 = value2 != null ? value2.length : -1;
            if (length1 != length2) {
                return Integer.compare(length1, length2);
            }
            return length1 > 0 ? Arrays.compareUnsigned(value1, value2) : 0;
        }
        final long addr = buffers.getChunkDataPtr(chunk);
        if (addr == 0) {
            // the decoder gives a chunk that holds NULLs only no data
            return 0;
        }
        final int size = ColumnType.sizeOf(columnType);
        final long addr1 = addr + (long) row1 * size;
        final long addr2 = addr + (long) row2 * size;
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.FLOAT -> Integer.compare(
                    Float.floatToIntBits(Unsafe.getUnsafe().getFloat(addr1)),
                    Float.floatToIntBits(Unsafe.getUnsafe().getFloat(addr2))
            );
            case ColumnType.DOUBLE -> Long.compare(
                    Double.doubleToLongBits(Unsafe.getUnsafe().getDouble(addr1)),
                    Double.doubleToLongBits(Unsafe.getUnsafe().getDouble(addr2))
            );
            default -> compareFixedSizeKeys(size, addr1, addr2, columnType);
        };
    }

    // the first row group of createSmallLastRowGroupTable(), as assertRowGroups() reads it
    private static String firstRowGroupOf16() {
        final StringBuilder sb = new StringBuilder("- |");
        for (int i = 1; i <= 16; i++) {
            sb.append(" 1=").append(i).append("/0/").append(i).append("/1");
        }
        return sb.append('\n').toString();
    }

    // the first row group of the table of assertFloatingPointKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim()
    private static String floatingPointKeyFirstRowGroupOf16() {
        final StringBuilder sb = new StringBuilder("- |");
        for (int i = 1; i <= 16; i++) {
            sb.append(" 1=").append(i).append("/1");
        }
        return sb.append('\n').toString();
    }

    // a row at one of the hours 01:00 to 08:00 of 2024-01-01, with one of ten keys and one of three symbols
    private static String randomRow(Rnd rnd, int v) {
        return "('2024-01-01T0" + (1 + rnd.nextInt(8)) + ":00:00.000000Z', " + rnd.nextInt(10) + ", '" + (char) ('a' + rnd.nextInt(3)) + "', " + v + ')';
    }

    private static long readValue(RowGroupBuffers buffers, int chunk, int columnType, int row) {
        final long addr = buffers.getChunkDataPtr(chunk);
        if (addr == 0) {
            // the decoder gives a chunk that holds NULLs only no data
            return Numbers.LONG_NULL;
        }
        return switch (ColumnType.sizeOf(columnType)) {
            case Long.BYTES -> Unsafe.getUnsafe().getLong(addr + (long) row * Long.BYTES);
            case Integer.BYTES -> Unsafe.getUnsafe().getInt(addr + (long) row * Integer.BYTES);
            case Byte.BYTES -> Unsafe.getUnsafe().getByte(addr + row);
            default -> throw new AssertionError("unexpected column type: " + ColumnType.nameOf(columnType));
        };
    }

    // The bytes that WAL apply compares of a var-size dedup key after its length, null for NULL: the UTF-8 bytes
    // of a VARCHAR and the chars of a STRING, the low byte of a char first.
    private static byte[] readVarSizeKey(RowGroupBuffers buffers, int chunk, int columnType, int row) {
        final long auxAddr = buffers.getChunkAuxPtr(chunk);
        final long dataAddr = buffers.getChunkDataPtr(chunk);
        if (auxAddr == 0) {
            // the decoder gives a chunk that holds NULLs only no data
            return null;
        }
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.VARCHAR -> {
                final Utf8Sequence value = VarcharTypeDriver.getSplitValue(
                        auxAddr,
                        auxAddr + buffers.getChunkAuxSize(chunk),
                        dataAddr,
                        dataAddr + buffers.getChunkDataSize(chunk),
                        row,
                        new Utf8SplitString()
                );
                if (value == null) {
                    return null;
                }
                final byte[] bytes = new byte[value.size()];
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = value.byteAt(i);
                }
                return bytes;
            }
            case ColumnType.STRING -> {
                final long valueAddr = dataAddr + Unsafe.getUnsafe().getLong(auxAddr + (long) row * Long.BYTES);
                final int length = Unsafe.getUnsafe().getInt(valueAddr);
                if (length < 0) {
                    return null;
                }
                final byte[] bytes = new byte[length * Character.BYTES];
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = Unsafe.getUnsafe().getByte(valueAddr + Integer.BYTES + i);
                }
                return bytes;
            }
            default -> throw new AssertionError("unexpected column type: " + ColumnType.nameOf(columnType));
        }
    }

    // A merge with a new key puts its rows in key order. The NaN with the sign bit set compares as the one NaN, which
    // sorts last, and decodes as that NaN: parquet stores it as a NULL.
    private void assertFloatingPointKeyMergeWithNewKeyClaimsCommitVersion(String keyType) throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createFloatingPointKeyTable(keyType, FLOATING_POINT_KEY_STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, key)");
            assertRowGroups(FLOATING_POINT_KEY_STORED_ROW_GROUP_0 + FLOATING_POINT_KEY_STORED_ROW_GROUP_1);

            // the negated NULL is a new key at 01:00, where the row group holds the keys 4 and 2
            insertNegatedKeys("('2024-01-01T01:00:00.000000Z', NULL, 100, 1)");
            assertRowGroups("cur | 1=2/1 1=1/1 1=100/1 2=4/1 2=3/1\n" + FLOATING_POINT_KEY_STORED_ROW_GROUP_1);
            assertUpdateMode();
        });
    }

    // O3 rows past the last row group become a row group of their own, in the key order WAL apply gave them: the
    // NaN with the sign bit set compares as the one NaN, which sorts last, and decodes as that NaN.
    private void assertFloatingPointKeyNewRowGroupFromO3RowsClaimsCommitVersion(String keyType, long negatedNullBits) throws Exception {
        setProperties();
        assertMemoryLeak(() -> {
            createFloatingPointKeyTable(keyType, FLOATING_POINT_KEY_STORED_ROWS);
            enableDedup("UPSERT KEYS(ts, key)");

            // the negation of a NULL key sets its sign bit, and the table takes the bits: the native partition
            // 2024-01-02 holds them in its second row
            insertNegatedKeys("('2024-01-02T01:00:00.000000Z', NULL, 0, 1)");
            Assert.assertEquals(negatedNullBits, readNativeKeyBits(1));

            // the keys 1 and the negated NULL at 05:00
            insertNegatedKeys("('2024-01-01T05:00:00.000000Z', -1, 100, 1), ('2024-01-01T05:00:00.000000Z', NULL, 101, 1)");
            assertRowGroups(FLOATING_POINT_KEY_STORED_ROW_GROUP_0 + FLOATING_POINT_KEY_STORED_ROW_GROUP_1 + "cur | 5=100/1 5=101/1\n");
            assertUpdateMode();
        });
    }

    // As testUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim(): the merge keeps the stored rows of the last
    // row group in their stored order and adds the O3 rows past the partition max after them, in the key order WAL
    // apply gave them, the NaN with the sign bit set last: it compares as the one NaN, which it decodes as.
    private void assertFloatingPointKeyUpsertOfIdenticalRowsWithRowsPastPartitionMaxKeepsClaim(String keyType) throws Exception {
        setProperties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            // 16 rows at 01:01 to 01:16, and two rows at 05:00 with equal values, which make the last row group
            final StringBuilder rows = new StringBuilder();
            for (int i = 1; i <= 16; i++) {
                rows.append("('2024-01-01T01:").append(i < 10 ? "0" : "").append(i).append(":00.000000Z', ").append(i).append(", ").append(i).append(", 1), ");
            }
            createFloatingPointKeyTable(keyType, rows + "('2024-01-01T05:00:00.000000Z', 9, 50, 1), ('2024-01-01T05:00:00.000000Z', 7, 50, 1)");
            enableDedup("UPSERT KEYS(ts, key)");
            // a new key puts the rows of the last row group in key order: 7, 8, 9
            insert("('2024-01-01T05:00:00.000000Z', 8, 100, 1)");

            // the rows of the keys 9 and 7 as stored, and the keys 1 and the negated NULL at 06:00
            insertNegatedKeys("""
                    ('2024-01-01T05:00:00.000000Z', -9, 50, 1),
                    ('2024-01-01T05:00:00.000000Z', -7, 50, 1),
                    ('2024-01-01T06:00:00.000000Z', -1, 200, 1),
                    ('2024-01-01T06:00:00.000000Z', NULL, 201, 1)
                    """);
            assertRowGroups(floatingPointKeyFirstRowGroupOf16() + "cur | 5=50/1 5=100/1 5=50/1 6=200/1 6=201/1\n");
            assertUpdateMode();
        });
    }

    // A table with a key of the given type and the given keys, which are in ascending order as WAL apply compares
    // them. Each row holds the rank of its key as its value. One commit adds a new key to the first row group and
    // makes a row group of the O3 rows past the last one: both hold their rows in key order and claim the commit's
    // dedup version.
    private void assertKeyTypeClaimsCommitVersion(String keyType, String... ascendingKeys) throws Exception {
        final int keyCount = ascendingKeys.length;
        Assert.assertTrue(keyCount > 1 && keyCount < 5);
        final StringBuilder rows = new StringBuilder();
        // 2024-01-02 takes the keys in ascending order
        for (int rank = 0; rank < keyCount; rank++) {
            rows.append("('2024-01-02T00:00:0").append(rank).append(".000000Z', ").append(ascendingKeys[rank]).append(", ").append(rank).append("), ");
        }
        // 01:00 takes every key but the second one, in descending order
        final StringBuilder storedRowGroups = new StringBuilder("- |");
        for (int rank = keyCount - 1; rank > -1; rank--) {
            if (rank != 1) {
                rows.append("('2024-01-01T01:00:00.000000Z', ").append(ascendingKeys[rank]).append(", ").append(rank).append("), ");
                storedRowGroups.append(" 1=").append(rank);
            }
        }
        // rows of the last key, one a minute from 03:00, fill up the two row groups of four rows
        final StringBuilder fillerRowGroups = new StringBuilder();
        for (int row = keyCount - 1; row < 8; row++) {
            rows.append(row > keyCount - 1 ? ", " : "").append("('2024-01-01T03:0").append(row).append(":00.000000Z', ").append(ascendingKeys[keyCount - 1]).append(", 9)");
            fillerRowGroups.append(row == 4 ? "\n- |" : "").append(" 3=9");
        }
        fillerRowGroups.append('\n');

        execute("CREATE TABLE pt (ts " + timestampType.getTypeName() + ", key " + keyType + ", v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO pt VALUES " + rows);
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        enableDedup("UPSERT KEYS(ts, key)");
        TestUtils.assertEquals(keyType + '\n' + storedRowGroups + fillerRowGroups, keyType + '\n' + readRowGroups());

        // the second key at 01:00, which its row group does not hold, and every key at 05:00, in descending order
        rows.setLength(0);
        rows.append("('2024-01-01T01:00:00.000000Z', ").append(ascendingKeys[1]).append(", 1)");
        final StringBuilder mergedRowGroup = new StringBuilder("cur |");
        final StringBuilder newRowGroup = new StringBuilder("cur |");
        for (int rank = keyCount - 1; rank > -1; rank--) {
            rows.append(", ('2024-01-01T05:00:00.000000Z', ").append(ascendingKeys[rank]).append(", ").append(rank).append(')');
        }
        for (int rank = 0; rank < keyCount; rank++) {
            mergedRowGroup.append(" 1=").append(rank);
            newRowGroup.append(" 5=").append(rank);
        }
        insert(rows.toString());
        TestUtils.assertEquals(keyType + '\n' + mergedRowGroup + fillerRowGroups + newRowGroup + '\n', keyType + '\n' + readRowGroups());
        assertUpdateMode();

        execute("DROP TABLE pt");
        drainWalQueue();
    }

    // the row groups of 2024-01-01 as readRowGroups() reads them
    private void assertRowGroups(String expected) {
        TestUtils.assertEquals(expected, readRowGroups());
    }

    // The commit took the path the test runs for: an in-place update leaves the bytes it replaced in the file, a
    // rewrite leaves none.
    private void assertUpdateMode() {
        try (TableReader reader = engine.getReader("pt")) {
            reader.openPartition(0);
            final long unusedBytes = reader.getAndInitParquetPartitionDecoder(0).metadata().getUnusedBytes();
            if (isRewrite) {
                Assert.assertEquals(0, unusedBytes);
            } else {
                Assert.assertTrue(unusedBytes > 0);
            }
        }
    }

    // pt without dedup, with a key of the given type, FLOAT or DOUBLE, and a BYTE column: the given rows of
    // 2024-01-01 in the given order and one row on 2024-01-02; 2024-01-01 converted to parquet. And st, which holds
    // the rows of insertNegatedKeys().
    private void createFloatingPointKeyTable(String keyType, String rows) throws Exception {
        final String columns = " (ts " + timestampType.getTypeName() + ", key " + keyType + ", v INT, b BYTE)";
        execute("CREATE TABLE st" + columns);
        execute("CREATE TABLE pt" + columns + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO pt VALUES " + rows + ", ('2024-01-02T00:00:00.000000Z', 0, 99, 1)");
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
    }

    // pt without dedup and with a BYTE column, converted with row groups of 16 rows: 16 rows at 01:01 to 01:16, and
    // the given rows, which make the last row group
    private void createSmallLastRowGroupTable(String lastRowGroupRows) throws Exception {
        execute("CREATE TABLE pt (ts " + timestampType.getTypeName() + ", k LONG, s SYMBOL, v INT, b BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final StringBuilder rows = new StringBuilder();
        for (int i = 1; i <= 16; i++) {
            rows.append("('2024-01-01T01:").append(i < 10 ? "0" : "").append(i).append(":00.000000Z', ").append(i).append(", 'b', ").append(i).append(", 1), ");
        }
        execute("INSERT INTO pt VALUES " + rows + lastRowGroupRows + ", ('2024-01-02T00:00:00.000000Z', 0, 'z', 99, 1)");
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
    }

    // pt without dedup, with the given rows of 2024-01-01 in the given order and one row on 2024-01-02;
    // 2024-01-01 converted to parquet
    private void createTable(String rows) throws Exception {
        execute("CREATE TABLE pt (ts " + timestampType.getTypeName() + ", k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO pt VALUES " + rows + ", ('2024-01-02T00:00:00.000000Z', 0, 'z', 99)");
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
    }

    private void enableDedup(String modeAndKeys) throws Exception {
        execute("ALTER TABLE pt DEDUP ENABLE " + modeAndKeys);
        drainWalQueue();
    }

    private void insert(String rows) throws Exception {
        execute("INSERT INTO pt VALUES " + rows);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
    }

    // Inserts the given rows into the table of createFloatingPointKeyTable() in one commit, each with its key
    // negated: the negation of a NULL is the NaN with the sign bit set, which no literal gives.
    private void insertNegatedKeys(String rows) throws Exception {
        execute("INSERT INTO st VALUES " + rows);
        execute("INSERT INTO pt SELECT ts, -key, v, b FROM st");
        execute("TRUNCATE TABLE st");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
    }

    // the bits of the key in the given row of the native partition 2024-01-02 of the table of
    // createFloatingPointKeyTable()
    private long readNativeKeyBits(int row) {
        try (TableReader reader = engine.getReader("pt")) {
            reader.openPartition(1);
            final int columnIndex = reader.getMetadata().getColumnIndex("key");
            final MemoryCR column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(1), columnIndex));
            return ColumnType.sizeOf(reader.getMetadata().getColumnType(columnIndex)) == Long.BYTES
                    ? column.getLong((long) row * Long.BYTES)
                    : column.getInt((long) row * Integer.BYTES);
        }
    }

    /**
     * Reads the row groups of 2024-01-01 as stored, one line per row group:
     * its claim, {@code cur} for the table's current dedup version, {@code old} for another version and {@code -}
     * for none, then its rows in storage order as {@code hour=k/symbol key/v}, with {@code /k2} and {@code /b} when
     * the table has those columns. A column {@code key} takes the place of {@code k} and {@code s} in the tables
     * that have it: its type varies, and the rows show its value by the one of {@code v}.
     * <p>
     * On top of that, in a row group that claims the current dedup version the rows of each timestamp ascend by the
     * table's dedup keys in column order {@code key, k, s, k2}, compared as WAL apply compares them, see
     * {@link #compareKeys}: a number as a signed integer with NULL first, a symbol by its key.
     */
    private String readRowGroups() {
        final StringSink actual = new StringSink();
        try (
                TableReader reader = engine.getReader("pt");
                RowGroupBuffers buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                DirectIntList columns = new DirectIntList(16, MemoryTag.NATIVE_DEFAULT)
        ) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final long dedupVersion = metadata.getDedupVersion();
            reader.openPartition(0);
            final ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(0);
            final ParquetMetaFileReader meta = decoder.metadata();
            final List<String> names = new ArrayList<>();
            for (String name : new String[]{"ts", "key", "k", "s", "v", "k2", "b"}) {
                if (metadata.getColumnIndexQuiet(name) > -1) {
                    names.add(name);
                }
            }
            final int[] types = new int[names.size()];
            final boolean[] isKey = new boolean[names.size()];
            for (int i = 0; i < types.length; i++) {
                final int columnIndex = metadata.getColumnIndex(names.get(i));
                types[i] = metadata.getColumnType(columnIndex);
                isKey[i] = i > 0 && metadata.isDedupKey(columnIndex);
                columns.add(meta.getColumnIndex(names.get(i)));
                columns.add(types[i]);
            }
            final long hourTicks = timestampType.getDriver().fromHours(1);
            for (int rowGroup = 0, n = meta.getRowGroupCount(); rowGroup < n; rowGroup++) {
                final long claim = meta.getRowGroupDedupSortVersion(rowGroup);
                final boolean isCurrent = claim == dedupVersion;
                actual.put(isCurrent ? "cur" : claim == ParquetMetaFileReader.DEDUP_SORT_VERSION_UNSET ? "-" : "old").put(" |");
                final int rowCount = (int) meta.getRowGroupSize(rowGroup);
                Assert.assertEquals(rowCount, decoder.decodeRowGroup(buffers, columns, rowGroup, 0, rowCount));
                for (int row = 0; row < rowCount; row++) {
                    final long ts = Unsafe.getUnsafe().getLong(buffers.getChunkDataPtr(0) + (long) row * Long.BYTES);
                    actual.put(' ').put(ts / hourTicks % 24).put('=');
                    boolean isFirstValue = true;
                    for (int i = 1; i < types.length; i++) {
                        if (names.get(i).equals("key")) {
                            continue;
                        }
                        final long value = readValue(buffers, i, types[i], row);
                        if (!isFirstValue) {
                            actual.put('/');
                        }
                        isFirstValue = false;
                        if (value == Numbers.LONG_NULL) {
                            actual.put("null");
                        } else {
                            actual.put(value);
                        }
                    }
                    if (isCurrent && row > 0) {
                        final long prevTs = Unsafe.getUnsafe().getLong(buffers.getChunkDataPtr(0) + (long) (row - 1) * Long.BYTES);
                        Assert.assertTrue("row group " + rowGroup + ", row " + row + ": timestamp order", prevTs <= ts);
                        if (prevTs == ts) {
                            int diff = 0;
                            for (int i = 1; i < types.length && diff == 0; i++) {
                                if (isKey[i]) {
                                    diff = compareKeys(buffers, i, types[i], row - 1, row);
                                }
                            }
                            Assert.assertTrue("row group " + rowGroup + ", row " + row + ": key order", diff <= 0);
                        }
                    }
                }
                actual.put('\n');
            }
        }
        return actual.toString();
    }

    private void setProperties() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        if (isRewrite) {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
        } else {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        }
    }
}
