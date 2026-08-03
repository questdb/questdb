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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.FSSTNative;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.DirectBitSet;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class CoveringIndexTest extends AbstractCairoTest {

    private static final ColumnVersionReader EMPTY_CVR = new ColumnVersionReader();

    @Test
    public void testAddPostingCoveringIndexAcrossManyParquetRowGroupsAllVarSizeWal() throws Exception {
        // The VARCHAR-only multi-row-group test pins ONE aux vector format
        // across the cross-row-group ColumnTypeDriver.shiftCopyAuxVector
        // boundary. STRING and BINARY have their own aux layouts and their own
        // shiftCopyAuxVector drivers; this covers all three varsize types in
        // the same row-group split. rnd_bin produces deterministic bytes for a
        // given seed so the no_covering vs covering comparison is exact.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_rg_vs (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        v_str STRING,
                        v_vc VARCHAR,
                        v_bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_rg_vs
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        'S' || x,
                        'V' || x,
                        rnd_bin(4, 8, 0)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_repro_rg_vs CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t_repro_rg_vs ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (v_str, v_vc, v_bin)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_rg_vs'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // Every covered value across all three varsize column types must
            // agree between the covering scan and the no_covering fallback,
            // for every row across the ~7 row groups -- proves cross-row-group
            // aux offset rewriting works for STRING, VARCHAR, and BINARY.
            assertSqlCursors(
                    "SELECT ts, sym, v_str, v_vc, v_bin FROM t_repro_rg_vs ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, v_str, v_vc, v_bin FROM t_repro_rg_vs ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexAcrossManyParquetRowGroupsWal() throws Exception {
        // Force a tiny Parquet row group size (16 rows) so a 100-row partition
        // spans ~7 row groups. The cross-row-group case is the one that goes
        // through ColumnTypeDriver.shiftCopyAuxVector to rewrite the VARCHAR
        // aux offsets to be cumulative across row groups -- the default-size
        // tests stay inside a single row group and never exercise it.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_rg (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_rg
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_repro_rg CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t_repro_rg ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_rg'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // Same headline assertion as the default-row-group test; if the
            // multi-row-group VARCHAR aux rewriting were wrong, tag values
            // beyond the first row group would be misaligned.
            assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_repro_rg WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
            // Full cursor comparison: every covered (price, tag) pair must match
            // the no_covering scan across all 100 rows / ~7 row groups.
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_repro_rg ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_repro_rg ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexAcrossMixedPartitionsWal() throws Exception {
        // Mixed-layout WAL test: historic partition NATIVE, last partition
        // PARQUET. Exercises indexHistoricPartitions -> indexNativePartition
        // for partition 0 alongside indexLastPartition -> indexParquetPartition
        // (the PR #7141 fix plus the covering provider) for partition 1.
        // Assertions sum across both partitions so a regression in either
        // path is visible.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_mixed
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO t_repro_mixed
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            // Convert only the last partition to Parquet.
            execute("ALTER TABLE t_repro_mixed CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            drainWalQueue();

            execute("ALTER TABLE t_repro_mixed ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_mixed'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // 25 rows per partition with sym='A0' (x%4==0 -> x in {4,8,..,100});
            // sum(price) per partition = 1300, totals: 50 rows and 2600.0.
            // tag = 'V0' on every one of those rows.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, first(tag) first_tag FROM t_repro_mixed WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tfirst_tag\n50\t2600.0\tV0\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexAcrossMultipleParquetPartitionsWal() throws Exception {
        // Two Parquet partitions: exercises the historic-partition Parquet
        // covering path AND the last-partition Parquet covering path in the
        // same ADD INDEX. With the provider broken, sidecars for both
        // partitions are empty; with the fix, both materialise correctly.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_multi (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_multi
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO t_repro_multi
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro_multi CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            execute("ALTER TABLE t_repro_multi ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_multi'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT count(*) rows, sum(price) sum_price, first(tag) first_tag FROM t_repro_multi WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tfirst_tag\n50\t2600.0\tV0\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexAllNullFixedAndVarSizeNativeWal() throws Exception {
        // Native counterpart to ...SkipsScratchForAllNullParquetWal /
        // ...AllNullRowGroupsParquetWal. A fixed-size (qty INT) and a var-size
        // (tag VARCHAR) covered column are added AFTER two native partitions are
        // fully populated, so column_top == partition_size on both and qty.d /
        // tag.d / tag.i are 0-byte files there. A third native partition carries
        // real qty/tag values. ALTER ADD INDEX builds the covering index for all
        // three partitions: the native build path (PostingIndexWriter.mapColumnFile)
        // must skip the empty covered files for the two back-filled partitions
        // and let the indexer synthesise nulls via the rowId < colTop fast path,
        // while materialising the third. The covering scan must match the
        // no_covering fallback row-for-row. The pre-existing native all-null
        // tests cover only a VARCHAR column; this pins the fixed-size case too.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_allnull_native (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_allnull_native
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    UNION ALL
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            // Added after 2024-01-01 and 2024-01-02 are full: column_top equals
            // partition_size on both, so qty.d / tag.d / tag.i are 0-byte files.
            execute("ALTER TABLE t_allnull_native ADD COLUMN qty INT");
            execute("ALTER TABLE t_allnull_native ADD COLUMN tag VARCHAR");
            drainWalQueue();

            // A later native partition where qty/tag carry real values, so the
            // index build exercises both the all-null skip and a populated seal.
            execute("""
                    INSERT INTO t_allnull_native
                    SELECT
                        dateadd('m', x::INT, '2024-01-03T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, x::INT, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            // No CONVERT -- every partition stays native.
            execute("ALTER TABLE t_allnull_native ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_allnull_native'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // 25 sym='A0' rows per partition (75 total). qty/tag null on the two
            // back-filled partitions (50 rows) and populated on the third (25).
            assertQuery("SELECT count(*) rows, " +
                    "sum(CASE WHEN qty IS NULL THEN 1 ELSE 0 END) null_qty, " +
                    "sum(CASE WHEN tag IS NULL THEN 1 ELSE 0 END) null_tag, " +
                    "sum(CASE WHEN tag = 'V0' THEN 1 ELSE 0 END) v0_tag " +
                    "FROM t_allnull_native WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tnull_qty\tnull_tag\tv0_tag\n75\t50\t50\t25\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, qty, tag FROM t_allnull_native WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, qty, tag FROM t_allnull_native WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexAllNullRowGroupsParquetWal() throws Exception {
        // A fixed-size (qty INT) and a var-size (tag VARCHAR) covered column are
        // both added MID-PARTITION: 50 rows exist before ADD COLUMN, 50 after,
        // all in one day partition, then converted to Parquet. CONVERT zeroes
        // column_top and stores nulls for rows 0..49. With a tiny Parquet row
        // group (16 rows), the pre-add region spans whole row groups that are
        // entirely null. If the decoder hands those row groups back with a zero
        // data pointer, accumulateCoveredColumnsFromRowGroup routes them through
        // accumulateAllNullFixedChunk / accumulateAllNullVarSizeChunk. The
        // covering scan must agree with the no_covering fallback row-for-row,
        // including the null pre-add region.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_allnull_rg (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // First batch: qty/tag do not exist yet.
            execute("""
                    INSERT INTO t_allnull_rg
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_allnull_rg ADD COLUMN qty INT");
            execute("ALTER TABLE t_allnull_rg ADD COLUMN tag VARCHAR");
            drainWalQueue();
            // Second batch into the same day partition: qty/tag have values.
            execute("""
                    INSERT INTO t_allnull_rg
                    SELECT
                        dateadd('m', (x + 60)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, x::INT, 'V' || (x % 4)
                    FROM long_sequence(50)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_allnull_rg CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t_allnull_rg ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_allnull_rg'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // 25 rows of sym='A0' across the partition; the pre-add rows have
            // qty/tag null, the rest have values. Covering must match the
            // fallback exactly, including the all-null pre-add row groups.
            assertSqlCursors(
                    "SELECT ts, sym, price, qty, tag FROM t_allnull_rg WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, qty, tag FROM t_allnull_rg WHERE sym = 'A0' ORDER BY ts"
            );
            assertSqlCursors(
                    "SELECT ts, sym, price, qty, tag FROM t_allnull_rg ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, qty, tag FROM t_allnull_rg ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexHandlesMidPartitionColumnTopNativeWal() throws Exception {
        // Native counterpart to ...ParquetWal below. Column added mid-partition
        // on a WAL table, NO convert -- the partition stays native. The existing
        // native covering build must also handle column_top correctly. The two
        // tests share assertions so any divergence between native and Parquet
        // covering for the column_top region surfaces immediately.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_top_n (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_top_n
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_repro_top_n ADD COLUMN tag VARCHAR");
            drainWalQueue();
            execute("""
                    INSERT INTO t_repro_top_n
                    SELECT
                        dateadd('m', (x + 60)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            // No CONVERT -- partition stays native, native covering path runs.
            execute("ALTER TABLE t_repro_top_n ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_top_n'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_repro_top_n WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_repro_top_n WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexHandlesMidPartitionColumnTopParquetWal() throws Exception {
        // Edge: a covering column was added MID-PARTITION (rows 0..49 inserted
        // before ADD COLUMN, rows 50..99 inserted after, all in the same day
        // partition). The partition is then converted to Parquet -- which
        // zeroes column_top and stores nulls in the parquet file for the
        // pre-add region. The provider decodes the whole column from Parquet
        // (nulls included); the covering scan must agree with the no_covering
        // scan on every row, including the nulls in the pre-add region.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_top (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // First batch: tag column doesn't exist yet.
            execute("""
                    INSERT INTO t_repro_top
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            // ADD COLUMN: existing rows now have tag=null via column_top.
            execute("ALTER TABLE t_repro_top ADD COLUMN tag VARCHAR");
            drainWalQueue();
            // Second batch into the same day partition: tag has values here.
            execute("""
                    INSERT INTO t_repro_top
                    SELECT
                        dateadd('m', (x + 60)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(50)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro_top CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t_repro_top ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_top'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // assertSqlCursors compares the covering scan to the no_covering
            // fallback. The pre-add region must come back as null on BOTH; any
            // mismatch (e.g. covering decoding garbage for the null region)
            // surfaces as cursor divergence.
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_repro_top WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_repro_top WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexIncludesColumnAddedAfterParquetConvertWal() throws Exception {
        // Edge: a covering column was added AFTER the partition was converted
        // to Parquet, so it isn't in the Parquet file's schema. The provider's
        // findParquetColumnIndex returns -1 -- materialiseAt returns false --
        // the .pc sidecar stays at its zero-filled maxCompressedSize. The
        // reader is expected to honour column_top (column did not exist in
        // this partition) and synthesise nulls, NOT read the empty sidecar.
        // This test pins that contract: a covering scan must return null for
        // every row in the pre-existing Parquet partition, not whatever
        // garbage the empty sidecar would decode to.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_added (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_added
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_repro_added CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // Add the new column AFTER the partition is already Parquet.
            execute("ALTER TABLE t_repro_added ADD COLUMN tag VARCHAR");
            drainWalQueue();

            execute("ALTER TABLE t_repro_added ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_added'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // price (in the Parquet schema) builds normally; tag (not in the
            // Parquet schema) must read back null on every row -- column_top
            // semantics, not data smuggled in from a zero-filled sidecar.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, " +
                    "sum(CASE WHEN tag IS NULL THEN 1 ELSE 0 END) null_tag " +
                    "FROM t_repro_added WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tnull_tag\n25\t1300.0\t25\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexIncludesColumnAddedToFullyPopulatedNativeWal() throws Exception {
        // Native counterpart to ...AfterParquetConvertWal above. Column added
        // AFTER all partition data exists; column_top == partition_row_count
        // so every row reads as null. The native covering path must produce
        // the same null result that the Parquet provider variant produces.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_added_n (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_added_n
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_repro_added_n ADD COLUMN tag VARCHAR");
            drainWalQueue();
            // No CONVERT -- native partition. column_top for tag == 100; all
            // rows read tag as null.
            execute("ALTER TABLE t_repro_added_n ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_added_n'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // Identical assertion shape to the Parquet variant: 25 sym='A0'
            // rows, sum=1300, all-null tag.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, " +
                    "sum(CASE WHEN tag IS NULL THEN 1 ELSE 0 END) null_tag " +
                    "FROM t_repro_added_n WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tnull_tag\n25\t1300.0\t25\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexIncludesFixedSizeColumnAddedToFullyPopulatedNativeWal() throws Exception {
        // Fixed-size sibling of ...IncludesColumnAddedToFullyPopulatedNativeWal.
        // qty INT is added AFTER the native partition is fully populated, so
        // column_top == partition_row_count and qty.d is a 0-byte file; every
        // row reads qty as null. The native covering build
        // (PostingIndexWriter.mapColumnFile) must skip the empty fixed-size file
        // and let the indexer synthesise nulls via the rowId < colTop fast path,
        // matching the Parquet provider variant. The existing native test covers
        // only a VARCHAR column, which always took the empty-file skip; this
        // pins the fixed-size case.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_added_fixed_n (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_added_fixed_n
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_added_fixed_n ADD COLUMN qty INT");
            drainWalQueue();
            // No CONVERT -- native partition. column_top for qty == 100; all rows
            // read qty as null.
            execute("ALTER TABLE t_added_fixed_n ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_added_fixed_n'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // 25 sym='A0' rows, sum(price)=1300, all-null qty -- identical shape
            // to the VARCHAR sibling test above.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, " +
                    "sum(CASE WHEN qty IS NULL THEN 1 ELSE 0 END) null_qty " +
                    "FROM t_added_fixed_n WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tnull_qty\n25\t1300.0\t25\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, qty FROM t_added_fixed_n WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, qty FROM t_added_fixed_n WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexNewColumnAfterAllParquetThenNativeAppendWal() throws Exception {
        // Two parquet historic partitions + a native last partition where the
        // new column has actual data, exercising column_top on both sides at
        // once. Layout after the prepare steps:
        //   2024-01-01 PARQUET  tag column_top == partition_size (not in schema)
        //   2024-01-02 PARQUET  tag column_top == partition_size (not in schema)
        //   2024-01-03 NATIVE   tag column_top == 0 (regular .d/.i)
        // ADD INDEX TYPE POSTING ... INCLUDE (price, tag) then runs all three
        // paths in one go:
        //   * indexHistoricPartitions -> indexParquetPartition twice: tag has
        //     no parquet chunk so the merged decode skips it and seal must
        //     synthesise null for every row from column_top, not read whatever
        //     stale bytes a zero-filled sidecar would hold.
        //   * indexLastPartition -> indexNativePartition once: tag is a normal
        //     column on disk and seal reads it through the standard native
        //     covering path.
        // Covering vs no_covering cursor compare pins both behaviours: any
        // mismatch (parquet leg synthesising the wrong value, or native leg
        // wiring the wrong .d/.i, or the merged decode silently truncating
        // price on parquet) surfaces as cursor divergence.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_post_add (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_post_add
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    UNION ALL
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            // Convert ALL existing partitions to parquet, including the last.
            execute("ALTER TABLE t_repro_post_add CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            // ADD COLUMN after every partition is already parquet -- the new
            // column lives in NO parquet schema and gets column_top equal to
            // each partition's row count.
            execute("ALTER TABLE t_repro_post_add ADD COLUMN tag VARCHAR");
            drainWalQueue();

            // Append into a NEW day partition -- this stays native and the new
            // column has real values here, not column_top synthesis.
            execute("""
                    INSERT INTO t_repro_post_add
                    SELECT
                        dateadd('m', x::INT, '2024-01-03T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro_post_add ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_post_add'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // Aggregate sanity: 25 sym='A0' rows per partition (x%4==0 for x in
            // {4,8,..,100}) -> 75 rows total, sum(price) = 1300*3 = 3900, and
            // tag is NULL on the two parquet partitions, 'V0' on the native one,
            // so exactly 50 nulls and 25 'V0's.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, " +
                    "sum(CASE WHEN tag IS NULL THEN 1 ELSE 0 END) null_tag, " +
                    "sum(CASE WHEN tag = 'V0' THEN 1 ELSE 0 END) v0_tag " +
                    "FROM t_repro_post_add WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\tnull_tag\tv0_tag\n75\t3900.0\t50\t25\n");

            // Strongest assertion: covering scan must agree row-for-row with
            // the no_covering fallback across both parquet and native legs.
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_repro_post_add WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_repro_post_add WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexNonWalActivePartitionCannotBeParquet() throws Exception {
        // Non-WAL counterpart to the WAL bug below. That bug needs a Parquet
        // LAST partition -- but a non-WAL table can never have one: CONVERT
        // PARTITION TO PARQUET skips the active (last) partition by design (see
        // TableWriter.convertPartitionNativeToParquet, "conversion is
        // unsupported for non-WAL tables"). So on a non-WAL table the last
        // partition stays native, ADD INDEX TYPE POSTING INCLUDE (...) takes the
        // native indexLastPartition() path, and there is no crash to reproduce.
        // This test pins that boundary.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_nowal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t_repro_nowal
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        x::DOUBLE,
                        'V' || (x % 4)
                    FROM long_sequence(100)
                    """);

            // CONVERT is a no-op here: 2024-01-01 is the active partition.
            execute("ALTER TABLE t_repro_nowal CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertQuery("SELECT name, isParquet FROM table_partitions('t_repro_nowal')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("name\tisParquet\n2024-01-01\tfalse\n");

            // The last partition is native, so the covering index builds fine.
            execute("ALTER TABLE t_repro_nowal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            assertQuery("SELECT indexed FROM table_columns('t_repro_nowal') WHERE \"column\" = 'sym'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");
            // x % 4 == 0 for x in {4, 8, ..., 100}: sum = 4*(1+2+...+25) = 1300;
            // tag = 'V0' on every one of those rows.
            assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_repro_nowal WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexOnSymbolAddedAfterParquetConvertWal() throws Exception {
        // Mirror of the NewColumnAfterAllParquet... test above, but with the
        // new column being the INDEXED SYMBOL rather than a covered one. ADD
        // COLUMN sym2 SYMBOL after parquet conversion gives sym2 column_top
        // == partition_size on every existing parquet partition -- the column
        // simply isn't in those parquet schemas at all. So during ADD INDEX:
        //   * indexHistoricPartitions -> indexParquetPartition: the
        //     findParquetColumnIndex == -1 early-return at "could not find
        //     symbol column for indexing in parquet" fires for both parquet
        //     partitions; no index files, no covering sidecars are created
        //     for them, and -- importantly -- the merged decode at
        //     TableWriter.java:6914 is never reached on those partitions.
        //   * indexLastPartition -> indexNativePartition: the native
        //     2024-01-03 partition (where sym2 has real values from a
        //     post-ADD-COLUMN insert) is indexed via the regular native
        //     path.
        // Layout after the prepare steps:
        //   2024-01-01 PARQUET  sym2 column_top == 100, not in schema
        //   2024-01-02 PARQUET  sym2 column_top == 100, not in schema
        //   2024-01-03 NATIVE   sym2 column_top == 0,   real B0-B3 values
        // A WHERE sym2 = 'B0' covering scan must produce exactly the 25
        // matching rows from 2024-01-03 with their price values, and agree
        // row-for-row with the no_covering fallback. NULL probes against the
        // two parquet partitions must come back via column_top, not via the
        // (non-existent) index files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_sym2 (
                        ts TIMESTAMP,
                        sym1 SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_sym2
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    UNION ALL
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro_sym2 CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            // New SYMBOL column AFTER all partitions are parquet.
            execute("ALTER TABLE t_repro_sym2 ADD COLUMN sym2 SYMBOL");
            drainWalQueue();

            // Native partition where sym2 has real values.
            execute("""
                    INSERT INTO t_repro_sym2
                    SELECT
                        dateadd('m', x::INT, '2024-01-03T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'B' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro_sym2 ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (price)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_sym2'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT indexed FROM table_columns('t_repro_sym2') WHERE \"column\" = 'sym2'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");

            // x % 4 == 0 for x in {4,8,..,100}: 25 rows in 2024-01-03,
            // sum(price) = 4*(1+2+...+25) = 1300. The two parquet partitions
            // contribute zero rows (sym2 is null there).
            assertQuery("SELECT count(*) rows, sum(price) sum_price FROM t_repro_sym2 WHERE sym2 = 'B0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tsum_price\n25\t1300.0\n");

            // 200 rows across the two parquet partitions where sym2 IS NULL
            // -- this is the column_top synthesis path, not the index. Any
            // missing partition's worth of nulls would surface here.
            assertQuery("SELECT count(*) null_rows FROM t_repro_sym2 WHERE sym2 IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("null_rows\n200\n");

            // Covering vs no_covering must agree row-for-row.
            assertSqlCursors(
                    "SELECT ts, sym2, price FROM t_repro_sym2 WHERE sym2 = 'B0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym2, price FROM t_repro_sym2 WHERE sym2 = 'B0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexParquetVarSizeScratchMmapExtendsWal() throws Exception {
        // Forces the parquet single-pass build to extend (mremap) the var-size
        // covered-column DATA scratch mmap mid-decode. prepareCoveredColumnMmaps
        // pre-sizes that scratch to the data-append page size (2 MB in tests) and
        // grows it on demand as accumulateCoveredColumnsFromRowGroup appends each
        // row group's decoded data via putBlockOfBytes. A STRING covered column
        // whose decoded data exceeds 2 MB across several row groups triggers an
        // extend that may relocate the mapping base; configureCoveringFromMmaps
        // must then read the post-extend addressOf(0), not a stale base, when it
        // wires the seal. Every other parquet var-size test stays within a few KB
        // (single page, no extend) and the large var-size tests are native (a
        // different memory class), so this is the only coverage of the extend.
        //
        // 8000 rows x 200-char STRING is ~3.2 MB of .d data; a 2000-row parquet
        // row group is ~0.8 MB, so the running total crosses 2 MB on the third
        // group and the remaining groups append past the extend.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vs_extend (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_vs_extend
                    SELECT
                        dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        rnd_str(200, 200, 0)
                    FROM long_sequence(8000)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_vs_extend CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t_vs_extend ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (s)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_vs_extend'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // The covering scan reads s from the sidecar built off the extended
            // scratch mmap; it must match the no_covering fallback for every row.
            // WHERE sym = 'A0' selects the covering factory (a bare ORDER BY ts
            // would not). 2000 of the 8000 rows have sym = 'A0'.
            assertQuery("SELECT count() FROM t_vs_extend WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n2000\n");
            assertSqlCursors(
                    "SELECT ts, sym, s FROM t_vs_extend WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, s FROM t_vs_extend WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexSkipsScratchForAllNullParquetWal() throws Exception {
        // Regression guard for the all-null covered-column skip in
        // prepareCoveredColumnMmaps. Two parquet partitions get a new VARCHAR
        // column 'tag' AFTER conversion, leaving columnTop == partitionSize
        // on both. ALTER ADD INDEX must NOT open scratch tag.d / tag.i files
        // under those partitions: the indexer's rowId < colTop fast path emits
        // null sentinels straight from the colTop value handed to it by
        // configureCoveringFromMmaps, with no data address ever dereferenced.
        // A regression that reintroduces materialisation (writing the null
        // pattern into a scratch mmap, or accumulating per row group through
        // accumulateAllNullVarSizeChunk) would create those files via the
        // ff.openRW path below and flip the counter.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final AtomicInteger badOpens = new AtomicInteger(0);
        final StringBuilder badNames = new StringBuilder();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && name != null
                        && (Utf8s.containsAscii(name, "2024-01-01") || Utf8s.containsAscii(name, "2024-01-02"))
                        && (Utf8s.containsAscii(name, "tag.d") || Utf8s.containsAscii(name, "tag.i"))) {
                    badOpens.incrementAndGet();
                    badNames.append(name).append(';');
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_null_parquet_skip (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_null_parquet_skip
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    UNION ALL
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_null_parquet_skip CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            // ADD COLUMN after every partition is parquet: 'tag' lives in NO
            // parquet schema and gets columnTop == partition_size on both
            // existing parquet partitions.
            execute("ALTER TABLE t_null_parquet_skip ADD COLUMN tag VARCHAR");
            drainWalQueue();

            // A native partition where 'tag' has real values, so the index
            // build has at least one partition that DOES need to materialise
            // and we exercise the per-partition code path twice with the
            // tripwire only catching the parquet cases.
            execute("""
                    INSERT INTO t_null_parquet_skip
                    SELECT
                        dateadd('m', x::INT, '2024-01-03T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            failArmed.set(true);
            try {
                execute("ALTER TABLE t_null_parquet_skip ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (tag)");
                drainWalQueue();
            } finally {
                failArmed.set(false);
            }

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_null_parquet_skip'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // The actual regression guard: no scratch tag.d / tag.i opens in
            // the parquet partition directories during ADD INDEX. The
            // covering scan below verifies the indexer still returns the
            // correct null sentinels through the rowId < colTop fast path.
            assertEquals("openRW(tag.d|tag.i) under 2024-01-01 or 2024-01-02 "
                            + "must be 0 -- the all-null covered-column slot must skip "
                            + "scratch materialisation. Opens recorded: " + badNames,
                    0, badOpens.get());

            // Sanity: 25 sym='A0' rows per partition. tag is NULL on the two
            // parquet partitions (50 rows) and 'V0' on the native one (25
            // rows). The covering scan must agree row-for-row with the
            // no_covering fallback, which means the indexer emitted the
            // correct null pattern for the parquet legs without reading
            // any scratch file.
            assertQuery("SELECT count(*) rows, "
                    + "sum(CASE WHEN tag IS NULL THEN 1 ELSE 0 END) null_tag, "
                    + "sum(CASE WHEN tag = 'V0' THEN 1 ELSE 0 END) v0_tag "
                    + "FROM t_null_parquet_skip WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("rows\tnull_tag\tv0_tag\n75\t50\t25\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_null_parquet_skip WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_null_parquet_skip WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexWhenLastPartitionIsNativeWal() throws Exception {
        // Native-partition parity for the Parquet fix below: same data, same
        // INCLUDE list, same assertions. The point is to show the covered
        // values returned by a covering scan are identical between native and
        // Parquet partitions on a WAL table -- so the Parquet provider path
        // really is a drop-in for the native path, not a separately-tuned
        // alternative.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro_native (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro_native
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        x::DOUBLE,
                        'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            // No CONVERT TO PARQUET -- partition stays native.

            execute("ALTER TABLE t_repro_native ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro_native'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT indexed FROM table_columns('t_repro_native') WHERE \"column\" = 'sym'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");
            // Mirror the Parquet test's assertion verbatim: x % 4 == 0 for
            // x in {4, 8, ..., 100}, so sum(price) = 4*(1+2+...+25) = 1300
            // and first(tag) = 'V0'.
            assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_repro_native WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexWhenLastPartitionIsParquetWal() throws Exception {
        // ADD INDEX TYPE POSTING ... INCLUDE (...) crashed with an NPE
        // ("Cannot invoke FilesFacade.read(...) because ff is null") when the
        // table's LAST partition is a Parquet partition, and -- separately --
        // covering source data was not being read for ANY Parquet partition
        // because mapColumnFile silently no-op'd on missing native .d/.i files.
        // The result was empty covering sidecars; queries returned null for
        // every INCLUDE column.
        //
        // Fix: indexLastPartition() routes Parquet last partitions through the
        // Parquet-aware indexParquetPartition(), and indexParquetPartition()
        // installs a CoveringSourceProvider on the posting writer so
        // mapCoveredColumn can decode each INCLUDE column from Parquet into
        // temp .d/.i files at the standard paths; unmapCoveredColumn deletes
        // them after the per-column seal iteration.
        //
        // A single-partition table is the minimal trigger: that partition is
        // the last partition, so indexLastPartition() handles it. On a WAL
        // table the failed apply suspends the table.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_repro (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_repro
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        x::DOUBLE,
                        'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_repro CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // INCLUDE both a fixed-width (DOUBLE) and a varsize (VARCHAR)
            // column to exercise both .d and .i materialisation paths.
            execute("ALTER TABLE t_repro ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            // The ADD INDEX must have applied cleanly: table not suspended,
            // column indexed.
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_repro'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT indexed FROM table_columns('t_repro') WHERE \"column\" = 'sym'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");
            // Covered columns must be populated. With the bug, the covering
            // sidecars are zero-filled and these reads return 0 / null.
            // x % 4 == 0 for x in {4, 8, ..., 100}: sum = 4*(1+2+...+25) = 1300;
            // tag = 'V0' on every one of those rows.
            assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_repro WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
        });
    }

    @Test
    public void testAddPostingCoveringIndexWithPendingLazyConversionAcrossParquetRowGroupsWal() throws Exception {
        // Same pending-lazy-conversion case as the single-row-group test, but with a
        // tiny Parquet row group size so a 100-row partition spans several row groups.
        // The converted var-size covered columns (SYMBOL->LONG, INT->VARCHAR,
        // INT->STRING, VARCHAR->LONG) must have their aux offsets rebased across row
        // groups via ColumnTypeDriver.shiftCopyAuxVector on the converted buffers, not
        // on the raw decode buffers.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lazy_conv_rg (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c_sym_long SYMBOL,
                        c_int_vc INT,
                        c_int_str INT,
                        c_vc_long VARCHAR,
                        c_keep DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_lazy_conv_rg
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        (x % 5)::STRING,
                        x::INT,
                        x::INT,
                        (x * 10)::VARCHAR,
                        x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_lazy_conv_rg CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_rg ALTER COLUMN c_sym_long TYPE LONG");
            execute("ALTER TABLE t_lazy_conv_rg ALTER COLUMN c_int_vc TYPE VARCHAR");
            execute("ALTER TABLE t_lazy_conv_rg ALTER COLUMN c_int_str TYPE STRING");
            execute("ALTER TABLE t_lazy_conv_rg ALTER COLUMN c_vc_long TYPE LONG");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_rg ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_lazy_conv_rg'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertSqlCursors(
                    "SELECT ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_rg WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_rg WHERE sym = 'A0' ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexWithPendingLazyConversionAllNullColumnWal() throws Exception {
        // Every converting covered column is fully NULL across the whole partition, so each
        // parquet row group decodes to an all-null chunk. This exercises the all-null routing
        // in accumulateCoveredColumnsFromRowGroup for converting columns: the size-based
        // detection (srcDataSize == 0 && srcAuxSize == 0) must route SYMBOL->LONG and
        // VARCHAR->LONG to accumulateAllNullFixedChunk and INT->VARCHAR / INT->STRING to
        // accumulateAllNullVarSizeChunk instead of into the conversion arms. Tiny row groups
        // place several all-null row groups back to back, covering the cross-row-group
        // all-null aux rebase as well.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lazy_conv_allnull (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c_sym_long SYMBOL,
                        c_int_vc INT,
                        c_int_str INT,
                        c_vc_long VARCHAR,
                        c_keep DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_lazy_conv_allnull
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        NULL::STRING,
                        NULL::INT,
                        NULL::INT,
                        NULL::VARCHAR,
                        x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_lazy_conv_allnull CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_allnull ALTER COLUMN c_sym_long TYPE LONG");
            execute("ALTER TABLE t_lazy_conv_allnull ALTER COLUMN c_int_vc TYPE VARCHAR");
            execute("ALTER TABLE t_lazy_conv_allnull ALTER COLUMN c_int_str TYPE STRING");
            execute("ALTER TABLE t_lazy_conv_allnull ALTER COLUMN c_vc_long TYPE LONG");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_allnull ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_lazy_conv_allnull'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertSqlCursors(
                    "SELECT ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_allnull WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_allnull WHERE sym = 'A0' ORDER BY ts"
            );
            assertSqlCursors(
                    "SELECT ts, sym, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_allnull ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv_allnull ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexWithPendingLazyConversionNullsAndVarcharSpillWal() throws Exception {
        // Stresses the pending-lazy-conversion covered columns on their hardest inputs:
        // NULLs in every converting column (exercises the null branch of each converter --
        // writeFixedNull for var/symbol->fixed, the VARCHAR null header and STRING -1
        // length prefix for fixed->var), and a LONG->VARCHAR column whose values exceed 9
        // bytes so the converted varchar spills into the data buffer (the inline-only case
        // never touches accumulateFixedToVarChunk's data append). Tiny row groups add the
        // cross-row-group aux rebase on top of both.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lazy_conv_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c_sym_long SYMBOL,
                        c_int_str INT,
                        c_long_vc LONG,
                        c_vc_long VARCHAR,
                        c_keep DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_lazy_conv_nulls
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        CASE WHEN x % 9 = 0 THEN NULL ELSE (x % 5)::STRING END,
                        CASE WHEN x % 7 = 0 THEN NULL ELSE x::INT END,
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x * 1_000_000_000)::LONG END,
                        CASE WHEN x % 8 = 0 THEN NULL ELSE (x * 10)::VARCHAR END,
                        x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_lazy_conv_nulls CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_nulls ALTER COLUMN c_sym_long TYPE LONG");
            execute("ALTER TABLE t_lazy_conv_nulls ALTER COLUMN c_int_str TYPE STRING");
            execute("ALTER TABLE t_lazy_conv_nulls ALTER COLUMN c_long_vc TYPE VARCHAR");
            execute("ALTER TABLE t_lazy_conv_nulls ALTER COLUMN c_vc_long TYPE LONG");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv_nulls ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c_sym_long, c_int_str, c_long_vc, c_vc_long, c_keep)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_lazy_conv_nulls'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertSqlCursors(
                    "SELECT ts, c_sym_long, c_int_str, c_long_vc, c_vc_long, c_keep FROM t_lazy_conv_nulls WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, c_sym_long, c_int_str, c_long_vc, c_vc_long, c_keep FROM t_lazy_conv_nulls WHERE sym = 'A0' ORDER BY ts"
            );
            assertSqlCursors(
                    "SELECT ts, sym, c_sym_long, c_int_str, c_long_vc, c_vc_long, c_keep FROM t_lazy_conv_nulls ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, c_sym_long, c_int_str, c_long_vc, c_vc_long, c_keep FROM t_lazy_conv_nulls ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddPostingCoveringIndexWithPendingLazyConversionParquetWal() throws Exception {
        // A covered column carries a pending lazy ALTER COLUMN TYPE: the partition was
        // converted to Parquet while the column had its source type, then the type was
        // changed without re-encoding Parquet, so the Parquet file still stores the
        // source type while metadata holds the target type. Building the covering index
        // must decode each such column in its Parquet-stored type and convert to the
        // current type, rather than asking the decoder for a type the file does not
        // contain (which suspended the table). Covers every crossing conversion arm:
        // SYMBOL->LONG and VARCHAR->LONG (var/symbol->fixed), INT->VARCHAR and
        // INT->STRING (fixed->var), plus a same-type pass-through (DOUBLE).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lazy_conv (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c_sym_long SYMBOL,
                        c_int_vc INT,
                        c_int_str INT,
                        c_vc_long VARCHAR,
                        c_keep DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_lazy_conv
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        (x % 5)::STRING,
                        x::INT,
                        x::INT,
                        (x * 10)::VARCHAR,
                        x::DOUBLE
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_lazy_conv CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv ALTER COLUMN c_sym_long TYPE LONG");
            execute("ALTER TABLE t_lazy_conv ALTER COLUMN c_int_vc TYPE VARCHAR");
            execute("ALTER TABLE t_lazy_conv ALTER COLUMN c_int_str TYPE STRING");
            execute("ALTER TABLE t_lazy_conv ALTER COLUMN c_vc_long TYPE LONG");
            drainWalQueue();

            execute("ALTER TABLE t_lazy_conv ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_lazy_conv'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT indexed FROM table_columns('t_lazy_conv') WHERE \"column\" = 'sym'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");
            // Covering read of the converted covered columns must match the no_covering
            // base read (the mature lazy-conversion read path) for every row of each key.
            assertSqlCursors(
                    "SELECT ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv WHERE sym = 'A0' ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv WHERE sym = 'A0' ORDER BY ts"
            );
            assertSqlCursors(
                    "SELECT ts, sym, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, c_sym_long, c_int_vc, c_int_str, c_vc_long, c_keep FROM t_lazy_conv ORDER BY ts"
            );
        });
    }

    @Test
    public void testAlterAddIndexAuthorizesIndexedColumnOnly() throws Exception {
        // SecurityContext.authorizeAlterTableAddIndex must receive only the indexed
        // column name. Passing covering column names through the same hook would let
        // an enterprise ACL implementation that authorizes per-column treat covering
        // columns as if they were being indexed, blurring the distinction between
        // "may add an index on column X" and "may read column Y".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_acl (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty INT) TIMESTAMP(ts)");
            ObjList<CharSequence> captured = new ObjList<>();
            AllowAllSecurityContext recording = new AllowAllSecurityContext() {
                @Override
                public void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    captured.clear();
                    for (int i = 0, n = columnNames.size(); i < n; i++) {
                        captured.add(columnNames.get(i).toString());
                    }
                }
            };
            try (SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(recording)) {
                execute("ALTER TABLE t_acl ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)", ctx);
            }
            assertEquals("authorize must receive only the indexed column", 1, captured.size());
            assertEquals("sym", captured.getQuick(0).toString());
        });
    }

    @Test
    public void testAlterAddIndexAuthorizesIndexedColumnOnlyWal() throws Exception {
        // Same contract as the non-WAL path, but exercises AlterOperation.authorize()
        // on the WAL-replay side, which builds the column list from extraStrInfo.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_acl_wal (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            ObjList<CharSequence> captured = new ObjList<>();
            AllowAllSecurityContext recording = new AllowAllSecurityContext() {
                @Override
                public void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    captured.clear();
                    for (int i = 0, n = columnNames.size(); i < n; i++) {
                        captured.add(columnNames.get(i).toString());
                    }
                }
            };
            try (SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(recording)) {
                execute("ALTER TABLE t_acl_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)", ctx);
                drainWalQueue();
            }
            assertEquals("authorize must receive only the indexed column", 1, captured.size());
            assertEquals("sym", captured.getQuick(0).toString());
        });
    }

    @Test
    public void testAlterAddIndexBuildOnMapFailedSurfacesError() throws Exception {
        // When ff.mmap fails for a covering .d file during ALTER ADD INDEX, the
        // build must surface a CairoException identifying the file rather than
        // silently storing MAP_FAILED into coveredColReadAddrs. This exercises
        // PostingIndexWriter.mapColumnFile, which opens its own fd via openRO
        // and mmaps the whole file with MMAP_INDEX_WRITER.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final long[] targetFd = {-1};
        final AtomicInteger badMunmaps = new AtomicInteger(0);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failArmed.get() && fd == targetFd[0] && memoryTag == MemoryTag.MMAP_INDEX_WRITER) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public void munmap(long address, long size, int memoryTag) {
                if (address == FilesFacade.MAP_FAILED) {
                    badMunmaps.incrementAndGet();
                    return; // skip the syscall: munmap(-1, ...) is undefined
                }
                super.munmap(address, size, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (failArmed.get() && fd != -1 && name != null
                        && Utf8s.endsWithAscii(name, "price.d")) {
                    targetFd[0] = fd;
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_build_fail (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_build_fail VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            CairoException caught = null;
            try {
                execute("ALTER TABLE t_build_fail ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            } catch (CairoException e) {
                caught = e;
            } catch (Throwable other) {
                fail("expected CairoException from mapColumnFile, got "
                        + other.getClass().getName() + ": " + other.getMessage());
            }
            failArmed.set(false);

            assertNotNull("expected CairoException from mapColumnFile", caught);
            String msg = caught.getMessage();
            assertTrue("error must identify the mmap failure: " + msg,
                    msg != null && msg.contains("could not mmap covered column"));
            assertTrue("error must reference price.d: " + msg, msg.contains("price.d"));
            assertEquals("munmap must never be called with MAP_FAILED address",
                    0, badMunmaps.get());
        });
    }

    @Test
    public void testAlterAddIndexFreesIndexerOnPostBuildMetadataFailure() throws Exception {
        // writeIndex() has its own try/catch that frees the indexer on a build
        // failure. addIndex()'s outer try/catch only does meaningful work when
        // the build SUCCEEDS but a later step fails -- before the indexer is
        // stored in the dense `indexers` list. Here the metadata swap
        // (rewriteAndSwapMetadata -> openMetaSwapFile) fails AFTER writeIndex
        // built the index, so the freshly built indexer is orphaned: not freed
        // by writeIndex's catch, not yet tracked in `indexers`. Only addIndex's
        // catch frees it; assertMemoryLeak fails if that catch is removed.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && name != null && Utf8s.containsAscii(name, "_meta.swp")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_post_build_fail (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_post_build_fail VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try {
                execute("ALTER TABLE t_post_build_fail ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                fail("expected exception from _meta.swp open failure after index build");
            } catch (CairoException e) {
                assertNotNull(e.getMessage());
            } finally {
                failArmed.set(false);
            }
        });
    }

    @Test
    public void testAlterAddIndexIncludeDuplicatePosition() throws Exception {
        // The SqlException must point at the duplicated column name, not
        // position 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_dup (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_dup ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, price)";
            int expected = sql.indexOf("price, price") + "price, ".length();
            assertQuery(sql)
                    .fails(expected, "duplicate column in INCLUDE");
        });
    }

    @Test
    public void testAlterAddIndexIncludeMissingColumnPosition() throws Exception {
        // The SqlException must point at the missing column name, not 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_miss (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_miss ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ghost)";
            int expected = sql.indexOf("ghost");
            assertQuery(sql)
                    .fails(expected, "INCLUDE column does not exist");
        });
    }

    @Test
    public void testAlterAddIndexIncludeSelfReferencePosition() throws Exception {
        // The SqlException must point at the offending sym name in the
        // INCLUDE list, not 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_self (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)";
            int expected = sql.lastIndexOf("sym");
            assertQuery(sql)
                    .fails(expected, "INCLUDE must not contain the indexed column");
        });
    }

    @Test
    public void testAlterAddIndexParquetCoveredMmapLeakOnAuxOpenFailure() throws Exception {
        // When prepareCoveredColumnMmaps opens the .d file for a var-size
        // covered column but the subsequent .i file open fails, the already-
        // opened .d MemoryMARW must not leak. Both are added to covMmaps only
        // after both opens succeed, so a failure on .i leaves .d unreachable
        // by the finally cleanup. assertMemoryLeak catches the leaked mmap.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && Utf8s.endsWithAscii(name, ".i")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_leak (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_leak VALUES
                    ('2024-01-01T00:00:00', 'A', 'v1'),
                    ('2024-01-01T01:00:00', 'B', 'v2'),
                    ('2024-01-02T00:00:00', 'A', 'v3')
                    """);
            engine.releaseAllWriters();
            execute("ALTER TABLE t_leak CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertQuery("SELECT name, isParquet FROM table_partitions('t_leak') WHERE name = '2024-01-01'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");

            failArmed.set(true);
            try {
                execute("ALTER TABLE t_leak ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (tag)");
                fail("expected CairoException from .i open failure");
            } catch (CairoException e) {
                assertTrue(e.getMessage().contains("could not open read-write"));
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testAlterAddIndexParquetCoveredMmapMultiSlotCleanupOnLaterSlotFailure() throws Exception {
        // Multi-slot variant of ...MmapLeakOnAuxOpenFailure. With TWO var-size
        // covered columns, prepareCoveredColumnMmaps fully opens slot 0's .d +
        // .i (both added to covMmaps) before slot 1's .i open fails. The
        // exception must leave slot 0's two mmaps AND slot 1's already-opened .d
        // reachable by the caller's finally (Misc.freeObjListIfCloseable);
        // assertMemoryLeak catches any leak. The single-slot test iterates the
        // loop once and never exercises cleanup of an earlier, fully-opened slot.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // Fail only the SECOND covered column's aux (.i) open, after the
                // first column's .d and .i have both opened.
                if (failArmed.get() && name != null
                        && Utf8s.containsAscii(name, "tag2")
                        && Utf8s.endsWithAscii(name, ".i")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_leak_multi (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        tag1 VARCHAR,
                        tag2 VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_leak_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 'a1', 'b1'),
                    ('2024-01-01T01:00:00', 'B', 'a2', 'b2'),
                    ('2024-01-02T00:00:00', 'A', 'a3', 'b3')
                    """);
            engine.releaseAllWriters();
            execute("ALTER TABLE t_leak_multi CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertQuery("SELECT name, isParquet FROM table_partitions('t_leak_multi') WHERE name = '2024-01-01'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");

            failArmed.set(true);
            try {
                execute("ALTER TABLE t_leak_multi ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (tag1, tag2)");
                fail("expected CairoException from tag2 .i open failure");
            } catch (CairoException e) {
                assertTrue(e.getMessage().contains("could not open read-write"));
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testAlterTableAddIndexIncludesColumnWithColumnTop() throws Exception {
        // Regression test: when an INCLUDE column was added via ALTER TABLE
        // ADD COLUMN after some rows already existed, the column file starts
        // at offset 0 mapping to row colTop in that partition. Reading
        // rowId * size without subtracting colTop reads past the mapped
        // region or returns garbage for rows below colTop. The covering
        // index sidecar must represent those rows as NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ct (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2 rows before the new column exists — colTop will be 2 on this partition.
            execute("""
                    INSERT INTO t_ct VALUES
                    ('2024-01-01T00:00:00', 'A', 10),
                    ('2024-01-01T01:00:00', 'B', 20)
                    """);

            execute("ALTER TABLE t_ct ADD COLUMN price DOUBLE");

            // 3 rows after ADD COLUMN — these have price values in the file.
            execute("""
                    INSERT INTO t_ct VALUES
                    ('2024-01-01T02:00:00', 'A', 30, 100.5),
                    ('2024-01-01T03:00:00', 'B', 40, 200.5),
                    ('2024-01-01T04:00:00', 'A', 50, 300.5)
                    """);

            // Build a covering index that INCLUDEs the newly-added column.
            execute("ALTER TABLE t_ct ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // 'A' rows land at rowIds 0, 2, 4.
            //   rowId 0 — below colTop=2, price must be NULL.
            //   rowId 2 — price = 100.5.
            //   rowId 4 — price = 300.5.
            // Confirm the covering factory is actually on the plan.
            assertQuery("SELECT sym, qty, price FROM t_ct WHERE sym = 'A' ORDER BY ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            sym\tqty\tprice
                            A\t10\tnull
                            A\t30\t100.5
                            A\t50\t300.5
                            """);
        });
    }

    @Test
    public void testAlterTableAddIndexIncludesGeoIpv4ColumnsWithColumnTop() throws Exception {
        // Regression: CoveringPageFrameCursor.writeColumnRow uses Long.MIN_VALUE,
        // Integer.MIN_VALUE, and zeros as the "null" sentinel for rows below
        // columnTop, but the canonical NULLs are GeoHashes.NULL = -1L (GEOLONG),
        // GeoHashes.INT_NULL = -1 (GEOINT), GeoHashes.SHORT_NULL = -1 (GEOSHORT),
        // GeoHashes.BYTE_NULL = -1 (GEOBYTE), and Numbers.IPv4_NULL = 0 (IPv4).
        // When a GEO/IPv4 column is added via ALTER TABLE ADD COLUMN, rows
        // pre-dating the column must read back as NULL (text rendering: empty
        // for GEO, "" for IPv4).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ct_geo (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2 rows before any of the GEO/IPv4 columns exist — those rows
            // must surface as NULL once the columns are added.
            execute("""
                    INSERT INTO t_ct_geo VALUES
                    ('2024-01-01T00:00:00', 'A', 10),
                    ('2024-01-01T01:00:00', 'A', 20)
                    """);

            // Bit widths map to: 5b=GEOBYTE, 10b=GEOSHORT, 20b=GEOINT, 40b=GEOLONG.
            execute("ALTER TABLE t_ct_geo ADD COLUMN gb GEOHASH(5b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gs GEOHASH(10b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gi GEOHASH(20b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gl GEOHASH(40b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN ip IPv4");

            // 2 rows after ADD COLUMN with real values. Geohash base-32 omits a/i/l/o.
            execute("""
                    INSERT INTO t_ct_geo VALUES
                    ('2024-01-01T02:00:00', 'A', 30, #y, #yz, #yzbc, #yzbc1234, '10.0.0.1'),
                    ('2024-01-01T03:00:00', 'A', 40, #b, #bc, #bcde, #bcde1234, '10.0.0.2')
                    """);

            execute("ALTER TABLE t_ct_geo ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (gb, gs, gi, gl, ip)");
            engine.releaseAllWriters();

            // Rows 0-1 (below columnTop for every added column) MUST be NULL.
            // The buggy writeColumnRow fallback writes Long.MIN_VALUE for
            // GEOLONG/gl, Integer.MIN_VALUE for GEOINT/gi and IPv4/ip,
            // (short) 0 for GEOSHORT/gs, and (byte) 0 for GEOBYTE/gb.
            // None of those round-trip as NULL — text rendering shows them
            // as garbage geohash strings or 0.0.0.0 for IPv4.
            assertQuery("SELECT sym, qty, gb, gs, gi, gl, ip FROM t_ct_geo WHERE sym = 'A' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            sym\tqty\tgb\tgs\tgi\tgl\tip
                            A\t10\t\t\t\t\t
                            A\t20\t\t\t\t\t
                            A\t30\ty\tyz\tyzbc\tyzbc1234\t10.0.0.1
                            A\t40\tb\tbc\tbcde\tbcde1234\t10.0.0.2
                            """);
        });
    }

    @Test
    public void testAlterTableAddIndexO3DuplicateInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_o3dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30),
                    ('2024-01-02T09:00:00', 'C', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50),
                    ('2024-01-02T11:00:00', 'A', 6.5, 60)
                    """);
            // Duplicate insert triggers O3 merge — tests sealFull/reencodeAllGenerations
            execute("""
                    INSERT INTO t_o3dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30),
                    ('2024-01-02T09:00:00', 'C', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50),
                    ('2024-01-02T11:00:00', 'A', 6.5, 60)
                    """);
            engine.releaseAllWriters();

            // Non-covering query verifies data survived O3 merge
            assertQuery("SELECT count() FROM t_o3dup WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            6
                            """);

            // Covering LATEST ON after O3 — sidecar rebuilt by rebuildSidecars()
            assertQuery("SELECT price, qty FROM t_o3dup WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            6.5\t60
                            """);
        });
    }

    @Test
    public void testAlterTableAddIndexO3DuplicateInsertWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3dup_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3dup_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-02T09:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            // Duplicate insert triggers O3 merge through WAL apply
            execute("""
                    INSERT INTO t_o3dup_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-02T09:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            // Non-covering query (SELECT * includes ts) — verifies data integrity after O3
            assertQuery("SELECT * FROM t_o3dup_wal WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice\tqty
                            2024-01-01T09:00:00.000000Z\tA\t1.5\t10
                            2024-01-01T09:00:00.000000Z\tA\t1.5\t10
                            2024-01-02T09:00:00.000000Z\tA\t3.5\t30
                            2024-01-02T09:00:00.000000Z\tA\t3.5\t30
                            """);
        });
    }

    @Test
    public void testAlterTableAddIndexWithInclude() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_alter ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_alter")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
                assertEquals(metadata.getColumnIndex("price"), coveringCols.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), coveringCols.getQuick(1));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeBinaryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_binary (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        data BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // BINARY columns are supported in INCLUDE — the writer stores them in
            // the var-width sidecar and the covering query path reads them directly.
            execute("ALTER TABLE t_binary ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (data)");
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeCoveringQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_q (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_alter_q ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            // Insert AFTER adding index so sidecar data is written
            execute("""
                    INSERT INTO t_alter_q VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20),
                    ('2024-01-01T02:00:00', 'A', 3.5, 30)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_alter_q WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            """);
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeDuplicateColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dup (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_dup ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, price)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("duplicate column in INCLUDE"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_empty (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_empty ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE ()");
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("at least one column name expected in INCLUDE"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeNonExistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noexist (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_noexist ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ghost)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE column does not exist"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeOnBitmapFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bitmap (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_bitmap ALTER COLUMN sym ADD INDEX TYPE BITMAP INCLUDE (price)");
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE is only supported for POSTING index type"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeSelfReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_self (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("ALTER TABLE t_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            // Verify metadata has covering index configuration
            try (TableReader r = engine.getReader("t_wal")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
            }

            // Non-covering query (SELECT * includes ts, which is not in INCLUDE)
            execute("""
                    INSERT INTO t_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t_wal WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice\tqty
                            2024-01-01T00:00:00.000000Z\tA\t1.5\t10
                            """);
        });
    }

    @Test
    public void testAlterTableWalIncludeNonExistentRejects() throws Exception {
        // Validation happens in SqlCompilerImpl.alterTableColumnAddIndex() — works for both WAL and non-WAL
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal_bad (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("ALTER TABLE t_wal_bad ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (nonexistent)");
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE column does not exist"));
            }
        });
    }

    @Test
    public void testAlterTableWalIncludeSelfReferenceRejects() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal_self (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("ALTER TABLE t_wal_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)");
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testAlterTypeCoveredColumnFallsBackToTableScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_cov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_alter_cov WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            execute("ALTER TABLE t_alter_cov ALTER COLUMN qty TYPE LONG");

            assertQuery("SELECT price, qty FROM t_alter_cov WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            execute("""
                    INSERT INTO t_alter_cov VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5, 400)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_alter_cov WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            40.5\t400
                            """);
        });
    }

    @Test
    public void testAsyncFilterPageFrameAllTypesParquetWal() throws Exception {
        // Parquet variant of testAsyncFilterPageFrameAllTypesWalUnsealed below:
        // CREATE without index, INSERT across every column type, CONVERT to
        // Parquet, then ALTER ADD INDEX TYPE POSTING INCLUDE (all 15 cols).
        // This exercises the Parquet covering provider for every QuestDB type
        // -- including the three varsize types (STRING, VARCHAR, BINARY) that
        // route through ColumnTypeDriver.shiftCopyAuxVector for offset
        // rewriting across row groups.
        //
        // Asserts the covering scan agrees with the no_covering scan over the
        // same data: any mismatch on any column proves that column's sidecar
        // is wrong.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_alltypes_pq (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_char CHAR,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_l256 LONG256,
                        v_str STRING,
                        v_vc VARCHAR,
                        v_bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_alltypes_pq VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1000, 10_000, 1.5, 2.5, true, 'x',
                     '2024-06-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.2.3.4',
                     cast('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef' as LONG256),
                     'str_a', 'vc_a', rnd_bin(8, 16, 0)),
                    ('2024-01-01T01:00:00', 'B', 9, 900, 9000, 90_000, 9.5, 8.5, false, 'y',
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('2024-01-01T02:00:00', 'A', 2, 200, 2000, 20_000, 2.5, 3.5, false, 'z',
                     '2024-07-01T00:00:00.000Z', '22222222-2222-2222-2222-222222222222', '5.6.7.8',
                     cast('0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890' as LONG256),
                     'str_a2', 'vc_a2', rnd_bin(8, 16, 0))
                    """);
            drainWalQueue();

            // Convert to Parquet THEN add the covering index -- the path
            // the provider hooks. After this, the covering sidecars must be
            // populated from Parquet bytes for every INCLUDE column.
            execute("ALTER TABLE t_pf_alltypes_pq CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("""
                    ALTER TABLE t_pf_alltypes_pq ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (
                        v_byte, v_short, v_int, v_long, v_float, v_double,
                        v_bool, v_char, v_date, v_uuid, v_ipv4, v_l256,
                        v_str, v_vc, v_bin
                    )
                    """);
            drainWalQueue();

            assertSqlCursors(
                    "SELECT v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, v_char, " +
                            "v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin " +
                            "FROM t_pf_alltypes_pq WHERE sym = 'A' AND v_int > 0",
                    "SELECT /*+ no_covering */ v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, v_char, " +
                            "v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin " +
                            "FROM t_pf_alltypes_pq WHERE sym = 'A' AND v_int > 0"
            );
        });
    }

    @Test
    public void testAsyncFilterPageFrameAllTypesWalUnsealed() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_alltypes (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            v_byte, v_short, v_int, v_long, v_float, v_double,
                            v_bool, v_char, v_date, v_uuid, v_ipv4, v_l256,
                            v_str, v_vc, v_bin
                        ),
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_char CHAR,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_l256 LONG256,
                        v_str STRING,
                        v_vc VARCHAR,
                        v_bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_alltypes VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1000, 10_000, 1.5, 2.5, true, 'x',
                     '2024-06-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.2.3.4',
                     cast('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef' as LONG256),
                     'str_a', 'vc_a', rnd_bin(8, 16, 0)),
                    ('2024-01-01T01:00:00', 'B', 9, 900, 9000, 90_000, 9.5, 8.5, false, 'y',
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('2024-01-01T02:00:00', 'A', 2, 200, 2000, 20_000, 2.5, 3.5, false, 'z',
                     '2024-07-01T00:00:00.000Z', '22222222-2222-2222-2222-222222222222', '5.6.7.8',
                     cast('0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890' as LONG256),
                     'str_a2', 'vc_a2', rnd_bin(8, 16, 0))
                    """);
            drainWalQueue();

            assertSqlCursors(
                    "SELECT v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, v_char, " +
                            "v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin " +
                            "FROM t_pf_alltypes WHERE sym = 'A' AND v_int > 0",
                    "SELECT /*+ no_covering */ v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, v_char, " +
                            "v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin " +
                            "FROM t_pf_alltypes WHERE sym = 'A' AND v_int > 0"
            );
        });
    }

    @Test
    public void testAsyncFilterPageFrameFallback() throws Exception {
        // Test that the page frame cursor fallback (column file reads) works
        // when covering data may not be in sidecar form. Uses WAL table where
        // the active partition is not yet sealed.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_fallback (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_fallback VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'A', 70.0),
                    ('2024-01-01T04:00:00', 'B', 80.0)
                    """);
            drainWalQueue();

            // Async filter with residual — must work via fallback or covering path
            assertSqlCursors(
                    "SELECT price FROM t_pf_fallback WHERE sym = 'A' AND price > 25",
                    "SELECT /*+ no_covering */ price FROM t_pf_fallback WHERE sym = 'A' AND price > 25"
            );

            // Aggregation with filter
            assertSqlCursors(
                    "SELECT sym, sum(price) FROM t_pf_fallback WHERE sym = 'A' AND price > 25 GROUP BY sym",
                    "SELECT /*+ no_covering */ sym, sum(price) FROM t_pf_fallback WHERE sym = 'A' AND price > 25 GROUP BY sym"
            );
        });
    }

    @Test
    public void testAsyncFilterPageFrameLargeRowGrowth() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_grow (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, label, payload),
                        price DOUBLE,
                        label VARCHAR,
                        payload STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_grow
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           CASE WHEN x % 4 = 0 THEN 'B' ELSE 'A' END,
                           x * 1.5,
                           ('lbl_' || x)::VARCHAR,
                           'pay_' || x || '_payload_text'
                    FROM long_sequence(5_000)
                    """);
            drainWalQueue();

            assertQuery("SELECT sym, count() count, sum(price) sum_p FROM t_pf_grow WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tcount\tsum_p
                            A\t3750\t1.40625E7
                            """);
        });
    }

    @Test
    public void testAsyncFilterWithAlterTableIndex() throws Exception {
        // ALTER TABLE ADD INDEX path: index is rebuilt for existing data.
        // The async filter wraps CoveringIndex and must produce correct results
        // whether isCoveredAvailable(0) is true (sealed sidecar) or false (fallback to columns).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_async (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_async VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 70.0, 5),
                    ('2024-01-01T05:00:00', 'C', 80.0, 6)
                    """);
            engine.releaseAllWriters();

            // Add covering index after data exists
            execute("ALTER TABLE t_alter_async ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Row cursor path (no filter)
            assertSqlCursors(
                    "SELECT price, qty FROM t_alter_async WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ price, qty FROM t_alter_async WHERE sym = 'A'"
            );

            // Async filter over covering index
            assertSqlCursors(
                    "SELECT price, qty FROM t_alter_async WHERE sym = 'A' AND price > 25",
                    "SELECT /*+ no_covering */ price, qty FROM t_alter_async WHERE sym = 'A' AND price > 25"
            );

            // GROUP BY with filter
            assertSqlCursors(
                    "SELECT sym, count() FROM t_alter_async WHERE sym = 'A' AND price > 25 GROUP BY sym",
                    "SELECT /*+ no_covering */ sym, count() FROM t_alter_async WHERE sym = 'A' AND price > 25 GROUP BY sym"
            );

            // IN-list with filter (ORDER BY for deterministic comparison —
            // covering returns A-then-B, non-covering returns timestamp order)
            assertSqlCursors(
                    "SELECT price FROM t_alter_async WHERE sym IN ('A', 'B') AND price > 25 ORDER BY price",
                    "SELECT /*+ no_covering */ price FROM t_alter_async WHERE sym IN ('A', 'B') AND price > 25 ORDER BY price"
            );
        });
    }

    @Test
    public void testAutoIncludeTimestampAlreadyPresent() throws Exception {
        // User explicitly includes timestamp — no duplicate
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, ts),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: ts should appear exactly once (no duplicate from auto-include)
            try (TableReader reader = getReader("t_ts_dup")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertEquals(2, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("price"), coveringIndices.getQuick(0));
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(1));
            }

            execute("""
                    INSERT INTO t_ts_dup VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT ts, price FROM t_ts_dup WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tprice
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t20.0
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampAlterNoInclude() throws Exception {
        // ALTER TABLE ... ADD INDEX TYPE POSTING with no INCLUDE clause is a
        // plain, non-covering posting index. The timestamp auto-include only
        // rounds out an explicit INCLUDE set, so a bare ALTER must NOT produce
        // a covering index.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_alter_noinc (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_ts_alter_noinc ALTER COLUMN sym ADD INDEX TYPE POSTING");

            try (TableReader reader = getReader("t_ts_alter_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertFalse(metadata.getColumnMetadata(symIdx).isCovering());
                assertNull(metadata.getColumnMetadata(symIdx).getCoveringColumnIndices());
            }

            execute("""
                    INSERT INTO t_ts_alter_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            // bare ALTER POSTING (no INCLUDE) must not use a CoveringIndex plan.
            assertQuery("SELECT ts FROM t_ts_alter_noinc WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
        });
    }

    @Test
    public void testAutoIncludeTimestampAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_alter (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Add index BEFORE inserting data so covering sidecars are populated
            execute("ALTER TABLE t_ts_alter ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            execute("""
                    INSERT INTO t_ts_alter VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Timestamp should be auto-included via ALTER TABLE path
            assertQuery("SELECT ts, price FROM t_ts_alter WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tprice
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t30.0
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampConfigDefaultIsTrue() {
        assertTrue("Default should be true",
                engine.getConfiguration().isPostingIndexAutoIncludeTimestamp());
    }

    @Test
    public void testAutoIncludeTimestampCoversLatestOn() throws Exception {
        // The LATEST ON special path must use the covering index for the
        // AUTO-INCLUDED timestamp, not just an explicitly listed column. With
        // POSTING INCLUDE (price) the designated ts is auto-appended to the
        // covering set, so a LATEST ON query that projects ts is served from
        // the covering index instead of the column files. Guards the auto-
        // include feature's payoff on the latest-on path after the bare-POSTING
        // auto-cover was removed.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_cov_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ts_cov_latest VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-02T00:00:00', 'B', 20.0),
                        ('2024-01-03T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Projecting the auto-included ts through LATEST ON must stay covering.
            assertQuery("SELECT ts FROM t_ts_cov_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampCoversWhereFilter() throws Exception {
        // Positive twin of testAutoIncludeTimestampPostingNoInclude (which now
        // asserts the bare, non-covering case): with an explicit INCLUDE the
        // designated ts is auto-appended, so projecting ONLY the auto-included
        // ts (not the explicitly covered price) is still served from the
        // covering index. This is the auto-include feature's whole point -- the
        // common SELECT ts ... WHERE sym = ... pattern stays covering.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_cov_where (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Metadata: covering = {price, ts}, with ts auto-appended.
            try (TableReader reader = getReader("t_ts_cov_where")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringIndices);
                assertEquals(2, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("price"), coveringIndices.getQuick(0));
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(1));
            }

            execute("""
                    INSERT INTO t_ts_cov_where VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Projecting only the auto-included ts must use CoveringIndex.
            assertQuery("SELECT ts FROM t_ts_cov_where WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_auto (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: covering indices should include timestamp
            try (TableReader reader = getReader("t_ts_auto")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertEquals(2, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("price"), coveringIndices.getQuick(0));
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(1));
            }

            execute("""
                    INSERT INTO t_ts_auto VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Query selecting ts should use CoveringIndex because ts was auto-included
            assertQuery("SELECT ts, price FROM t_ts_auto WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tprice
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t30.0
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampCreateTableAsSelectNoInclude() throws Exception {
        // CREATE TABLE AS SELECT with an out-of-line INDEX(... TYPE POSTING)
        // clause goes through resolveCoveringFromAugmented. The out-of-line
        // parser does not accept INCLUDE today, so a CTAS posting index has no
        // INCLUDE columns and must therefore stay non-covering: the timestamp
        // auto-include only rounds out an explicit INCLUDE set.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_src_noinc (ts TIMESTAMP, sym SYMBOL, price DOUBLE)
                        TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_src_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            execute("""
                    CREATE TABLE t_ctas_noinc AS (SELECT * FROM t_src_noinc),
                        INDEX(sym TYPE POSTING)
                        TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            try (TableReader reader = getReader("t_ctas_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertFalse(metadata.getColumnMetadata(symIdx).isCovering());
                assertNull(metadata.getColumnMetadata(symIdx).getCoveringColumnIndices());
            }
        });
    }

    @Test
    public void testAutoIncludeTimestampCreateTableWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_ts_wal VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT ts, price FROM t_ts_wal WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tprice
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t30.0
                            """);
        });
    }

    @Test
    public void testAutoIncludeTimestampNoDesignatedTimestamp() throws Exception {
        // Table without designated timestamp — auto-include should be a no-op
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_none (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    )
                    """);
            execute("""
                    INSERT INTO t_ts_none VALUES ('A', 10.0), ('A', 20.0)
                    """);
            engine.releaseAllWriters();

            // price-only query should still use CoveringIndex
            assertQuery("SELECT price FROM t_ts_none WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex");
        });
    }

    @Test
    public void testAutoIncludeTimestampPostingNoInclude() throws Exception {
        // POSTING index without INCLUDE clause -- auto-include must NOT trigger.
        // A bare INDEX TYPE POSTING is a plain, non-covering posting index; the
        // timestamp auto-include only rounds out an explicit INCLUDE set.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_noinc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: no covering list at all.
            try (TableReader reader = getReader("t_ts_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertFalse(metadata.getColumnMetadata(symIdx).isCovering());
                assertNull(metadata.getColumnMetadata(symIdx).getCoveringColumnIndices());
            }

            execute("""
                    INSERT INTO t_ts_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            // No INCLUDE at all -- CoveringIndex must not be used.
            assertQuery("SELECT ts, price FROM t_ts_noinc WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
        });
    }

    @Test
    public void testBulkInsertSelectWithPostingIndex() throws Exception {
        // INSERT...SELECT into table with POSTING INCLUDE — must handle multi-partition
        // data at scale. Tests 1M rows across ~12 partitions with 200 symbols.
        assertMemoryLeak(() -> {
            StringBuilder symArgs = new StringBuilder();
            for (int k = 0; k < 200; k++) {
                if (k > 0) symArgs.append(',');
                symArgs.append("'K").append(k).append("'");
            }
            execute("""
                    CREATE TABLE t_bulk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute(
                    "INSERT INTO t_bulk"
                            + " SELECT dateadd('s', x::INT, '2024-01-01'),"
                            + "   rnd_symbol(" + symArgs + "),"
                            + "   rnd_double() * 100,"
                            + "   rnd_int(1, 1000, 0)"
                            + " FROM long_sequence(1_000_000)");
            engine.releaseAllWriters();

            // Verify all rows committed
            assertQuery("SELECT count() FROM t_bulk")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            1000000
                            """);

            // Verify index is usable — covering and non-covering must agree
            assertSqlCursors(
                    "SELECT count() FROM t_bulk WHERE sym = 'K0'",
                    "SELECT /*+ no_covering */ count() FROM t_bulk WHERE sym = 'K0'"
            );

            // Verify covered data with filter
            assertSqlCursors(
                    "SELECT price, qty FROM t_bulk WHERE sym = 'K0' AND price > 50 LIMIT 10",
                    "SELECT /*+ no_covering */ price, qty FROM t_bulk WHERE sym = 'K0' AND price > 50 LIMIT 10"
            );
        });
    }

    @Test
    public void testCollectDistinctKeysAcrossManyGens() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "many_gens";
                int plen = path.size();
                int genCount = 8;
                int keysPerGen = 6;
                int rowsPerGen = keysPerGen * 4;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int row = 0;
                    for (int g = 0; g < genCount; g++) {
                        for (int j = 0; j < rowsPerGen; j++) {
                            writer.add(j % keysPerGen, row++);
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                    }
                    writer.seal();
                }

                try (DirectBitSet found = new DirectBitSet(64);
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int total = reader.collectDistinctKeysInRange(found, 0, (long) genCount * rowsPerGen - 1);
                    assertEquals(keysPerGen, total);

                    found.clear();
                    int firstGen = reader.collectDistinctKeysInRange(found, 0, rowsPerGen - 1);
                    assertEquals(keysPerGen, firstGen);

                    found.clear();
                    int singleRow = reader.collectDistinctKeysInRange(found, 0, 0);
                    assertEquals(1, singleRow);
                }
            }
        });
    }

    @Test
    public void testCollectDistinctKeysInRangeDeltaStrideFew() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "dist_delta";
                int plen = path.size();
                int keyCount = 5;
                int rowsPerKey = 200;
                int totalRows = keyCount * rowsPerKey;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int half = totalRows / 2;
                    for (int row = 0; row < half; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(half - 1);
                    writer.commit();
                    for (int row = half; row < totalRows; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(totalRows - 1);
                    writer.commit();
                    writer.seal();
                }

                try (DirectBitSet found = new DirectBitSet(keyCount);
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int distinctAll = reader.collectDistinctKeysInRange(found, 0, totalRows - 1);
                    assertEquals(keyCount, distinctAll);

                    found.clear();
                    int distinctNarrow = reader.collectDistinctKeysInRange(found, 0, keyCount - 1);
                    assertEquals(keyCount, distinctNarrow);

                    found.clear();
                    int distinctSingle = reader.collectDistinctKeysInRange(found, 0, 0);
                    assertEquals(1, distinctSingle);
                }
            }
        });
    }

    @Test
    public void testCollectDistinctKeysInRangeFlatStrideMany() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "dist_flat";
                int plen = path.size();
                int keyCount = 300;
                int rowsPerKey = 2;
                int totalRows = keyCount * rowsPerKey;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int half = totalRows / 2;
                    for (int row = 0; row < half; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(half - 1);
                    writer.commit();
                    for (int row = half; row < totalRows; row++) {
                        writer.add(row % keyCount, row);
                    }
                    writer.setMaxValue(totalRows - 1);
                    writer.commit();
                    writer.seal();
                }

                try (DirectBitSet found = new DirectBitSet(keyCount);
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int distinctAll = reader.collectDistinctKeysInRange(found, 0, totalRows - 1);
                    assertEquals(keyCount, distinctAll);

                    found.clear();
                    int distinctMid = reader.collectDistinctKeysInRange(found, totalRows / 2, totalRows / 2 + 100);
                    assertTrue(distinctMid > 0 && distinctMid <= keyCount);

                    found.clear();
                    int distinctEmpty = reader.collectDistinctKeysInRange(found, totalRows + 100, totalRows + 200);
                    assertEquals(0, distinctEmpty);
                }
            }
        });
    }

    @Test
    public void testCollectDistinctKeysInRangeManyValuesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "dist_ef";
                int plen = path.size();
                int totalRows = 4_000;
                long[] rowIds = new long[totalRows];
                long pos = 0;
                for (int i = 0; i < totalRows; i++) {
                    pos += 1 + ((i * 0x9E3779B1L) & 0x7F);
                    rowIds[i] = pos;
                }
                long maxRow = rowIds[totalRows - 1];
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    int half = totalRows / 2;
                    for (int row = 0; row < half; row++) {
                        writer.add(0, rowIds[row]);
                    }
                    writer.setMaxValue(rowIds[half - 1]);
                    writer.commit();
                    for (int row = half; row < totalRows; row++) {
                        writer.add(0, rowIds[row]);
                    }
                    writer.setMaxValue(maxRow);
                    writer.commit();
                    writer.seal();
                }

                try (DirectBitSet found = new DirectBitSet(8);
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int hit = reader.collectDistinctKeysInRange(found, 0, maxRow);
                    assertEquals(1, hit);

                    found.clear();
                    int high = reader.collectDistinctKeysInRange(found, rowIds[totalRows - 5], maxRow);
                    assertEquals(1, high);
                }
            }
        });
    }

    @Test
    public void testCollectDistinctKeysInRangeSparseGen() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "dist_sparse";
                int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int row = 0; row < 500; row++) {
                        writer.add(row % 50, row);
                    }
                    writer.setMaxValue(499);
                    writer.commit();
                    for (int row = 500; row < 600; row++) {
                        writer.add(row % 3, row);
                    }
                    writer.setMaxValue(599);
                    writer.commit();
                }

                try (DirectBitSet found = new DirectBitSet(64);
                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                             configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int total = reader.collectDistinctKeysInRange(found, 0, 599);
                    assertEquals(50, total);

                    found.clear();
                    int gen1Only = reader.collectDistinctKeysInRange(found, 500, 599);
                    assertEquals(3, gen1Only);
                }
            }
        });
    }

    @Test
    public void testCountPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            // COUNT(*) correctness: single partition
            assertQuery("SELECT COUNT(*) FROM t_count WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            Count
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """)
                    .returns("""
                            count
                            329
                            """);

            // Multi-partition
            execute("""
                    INSERT INTO t_count
                    SELECT dateadd('s', x::INT, '2024-01-02')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_count WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            668
                            """);
        });
    }

    @Test
    public void testCountPushdownEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertQuery("SELECT COUNT(*) FROM t_count_empty WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testCountPushdownNonExistentKey() throws Exception {
        // COUNT(*) for a key that doesn't exist should return 0
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_none (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_none VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_count_none WHERE sym = 'NONEXISTENT'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testCountPushdownVarcharOnly() throws Exception {
        // Regression test for count returning 0 for var-only INCLUDE.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_vc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_vc
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           rnd_str('alpha','beta','gamma')
                    FROM long_sequence(100)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_count_vc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            43
                            """);
        });
    }

    @Test
    public void testCountPushdownWithInList() throws Exception {
        // CoveringCursor.size() returns -1 for IN-list (multiKeys != null).
        // Verify COUNT(*) still works correctly via iteration fallback.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_in
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_count_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            654
                            """);
        });
    }

    @Test
    public void testCountStarMultiKeyVariations() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_multi
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           ('K' || (x % 5))::SYMBOL,
                           (x % 100) * 1.5
                    FROM long_sequence(2_000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM t_count_multi WHERE sym IN ('K0')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n400\n");
            assertQuery("SELECT count() FROM t_count_multi WHERE sym IN ('K0','K1')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n800\n");
            assertQuery("SELECT count() FROM t_count_multi WHERE sym IN ('K0','K1','K2')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n1200\n");

            assertQuery("SELECT DISTINCT sym FROM t_count_multi WHERE ts BETWEEN '2024-01-01' AND '2024-01-02' ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("sym\nK0\nK1\nK2\nK3\nK4\n");
            assertQuery("SELECT DISTINCT sym FROM t_count_multi ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("sym\nK0\nK1\nK2\nK3\nK4\n");
        });
    }

    @Test
    public void testCoveredBinLenDirectRead() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_binlen";
                int plen = path.size();
                int rowCount = 8;
                long binDataSize = (long) rowCount * 64;
                long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
                long binAuxSize = (long) (rowCount + 1) * Long.BYTES;
                long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    long off = 0;
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                        long binLen = 4L + i;
                        Unsafe.putLong(binDataAddr + off, binLen);
                        off += Long.BYTES;
                        for (long b = 0; b < binLen; b++) {
                            Unsafe.putByte(binDataAddr + off + b, (byte) (b + i));
                        }
                        off += binLen;
                    }
                    Unsafe.putLong(binAuxAddr + (long) rowCount * Long.BYTES, off);

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{binDataAddr},
                                new long[]{binAuxAddr},
                                new long[]{0},
                                new int[]{-1},
                                new int[]{2},
                                new int[]{ColumnType.BINARY},
                                1,
                                -1
                        );
                        int half = rowCount / 2;
                        for (int i = 0; i < half; i++) writer.add(0, i);
                        writer.setMaxValue(half - 1);
                        writer.commit();
                        for (int i = half; i < rowCount; i++) writer.add(0, i);
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                        writer.seal();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.BINARY}),
                            EMPTY_CVR, 0)) {
                        try (RowCursor c = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0})) {
                            assertTrue(c instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) c;
                            int seen = 0;
                            while (cc.hasNext()) {
                                cc.next();
                                assertEquals(4L + seen, cc.getCoveredBinLen(0));
                                seen++;
                            }
                            assertEquals(rowCount, seen);
                        }
                    }
                } finally {
                    Unsafe.free(binAuxAddr, binAuxSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(binDataAddr, binDataSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveredFloatAndBinLenDirectRead() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_float_bin";
                int plen = path.size();
                int rowCount = 10;
                long fAddr = Unsafe.malloc((long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                long binDataSize = (long) rowCount * 32; // 4 byte length + 28 byte payload per row
                long binDataAddr = Unsafe.malloc(binDataSize, MemoryTag.NATIVE_DEFAULT);
                long binAuxSize = (long) (rowCount + 1) * Long.BYTES;
                long binAuxAddr = Unsafe.malloc(binAuxSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    long off = 0;
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putFloat(fAddr + (long) i * Float.BYTES, 1.5f * i);
                        Unsafe.putLong(binAuxAddr + (long) i * Long.BYTES, off);
                        long binLen = 8L + i;
                        Unsafe.putLong(binDataAddr + off, binLen);
                        off += Long.BYTES;
                        for (long b = 0; b < binLen; b++) {
                            Unsafe.putByte(binDataAddr + off + b, (byte) (b + i));
                        }
                        off += binLen;
                    }
                    Unsafe.putLong(binAuxAddr + (long) rowCount * Long.BYTES, off);

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{fAddr},
                                new long[]{0},
                                new int[]{2},
                                new int[]{2},
                                new int[]{ColumnType.FLOAT},
                                1
                        );
                        int half = rowCount / 2;
                        for (int i = 0; i < half; i++) writer.add(0, i);
                        writer.setMaxValue(half - 1);
                        writer.commit();
                        for (int i = half; i < rowCount; i++) writer.add(0, i);
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                        writer.seal();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.FLOAT}),
                            EMPTY_CVR, 0)) {
                        try (RowCursor c = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0})) {
                            assertTrue(c instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) c;
                            int seen = 0;
                            while (cc.hasNext()) {
                                cc.next();
                                assertEquals(1.5f * seen, cc.getCoveredFloat(0), 1e-6);
                                seen++;
                            }
                            assertEquals(rowCount, seen);
                        }
                    }
                } finally {
                    Unsafe.free(binAuxAddr, binAuxSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(binDataAddr, binDataSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(fAddr, (long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveredLong256DenseGenDirectRead() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_long256";
                int plen = path.size();
                int rowCount = 8;
                long colAddr256 = Unsafe.malloc((long) rowCount * 32, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        long b256 = (long) i * 32;
                        Unsafe.putLong(colAddr256 + b256, 100L + i);
                        Unsafe.putLong(colAddr256 + b256 + 8, 200L + i);
                        Unsafe.putLong(colAddr256 + b256 + 16, 300L + i);
                        Unsafe.putLong(colAddr256 + b256 + 24, 400L + i);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr256},
                                new long[]{0},
                                new int[]{5},
                                new int[]{2},
                                new int[]{ColumnType.LONG256},
                                1
                        );
                        // Two commits + seal so the merged gen is stride-encoded
                        // dense and reads go through the dense-branch of
                        // getCoveredLong256_*.
                        int half = rowCount / 2;
                        for (int i = 0; i < half; i++) writer.add(0, i);
                        writer.setMaxValue(half - 1);
                        writer.commit();
                        for (int i = half; i < rowCount; i++) writer.add(0, i);
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                        writer.seal();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.LONG256}),
                            EMPTY_CVR, 0)) {
                        try (RowCursor c = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0})) {
                            assertTrue(c instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) c;
                            int seen = 0;
                            while (cc.hasNext()) {
                                cc.next();
                                assertEquals(100L + seen, cc.getCoveredLong256_0(0));
                                assertEquals(200L + seen, cc.getCoveredLong256_1(0));
                                assertEquals(300L + seen, cc.getCoveredLong256_2(0));
                                assertEquals(400L + seen, cc.getCoveredLong256_3(0));
                                seen++;
                            }
                            assertEquals(rowCount, seen);
                        }
                    }
                } finally {
                    Unsafe.free(colAddr256, (long) rowCount * 32, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveredStringFsstAndBinaryArray() throws Exception {
        // Covers the STRING / BINARY / ARRAY var-sidecar read paths via the
        // same O3 stride-merge route used by the varchar FSST test, hitting
        // decompressFsstStr / getVarSidecarStr / getVarSidecarBin /
        // getVarSidecarBinLen / getVarSidecarArray.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_str_bin_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (s, b, a),
                        s STRING,
                        b BINARY,
                        a DOUBLE[]
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_str_bin_arr
                    SELECT dateadd('s', (x * 2)::INT, '2024-01-01')::TIMESTAMP,
                           CASE x % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C'
                                      WHEN 3 THEN 'D' ELSE 'E' END,
                           ('abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_' || (x % 4)),
                           rnd_bin(8, 16, 0),
                           ARRAY[x::DOUBLE, (x + 1)::DOUBLE]
                    FROM long_sequence(2000)
                    """);
            execute("""
                    INSERT INTO t_str_bin_arr
                    SELECT dateadd('s', (x * 2 + 1)::INT, '2024-01-01')::TIMESTAMP,
                           CASE x % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C'
                                      WHEN 3 THEN 'D' ELSE 'E' END,
                           ('abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_' || (x % 4)),
                           rnd_bin(8, 16, 0),
                           ARRAY[x::DOUBLE, (x + 1)::DOUBLE]
                    FROM long_sequence(2000)
                    """);
            engine.releaseAllWriters();

            // Fetch all three covered columns to walk getVarSidecarStr,
            // getVarSidecarBin/getVarSidecarBinLen and getVarSidecarArray.
            assertQuery("SELECT count() c FROM t_str_bin_arr " +
                    "WHERE sym = 'A' AND s LIKE 'abcdefgh%' AND length(b) > 0 AND a[1] > 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("c\n800\n");
        });
    }

    @Test
    public void testCoveredUuidDenseGenDirectRead() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_uuid";
                int plen = path.size();
                int rowCount = 8;
                long colAddr = Unsafe.malloc((long) rowCount * 16, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(colAddr + (long) i * 16, 700L + i);
                        Unsafe.putLong(colAddr + (long) i * 16 + 8, 800L + i);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{4},
                                new int[]{2},
                                new int[]{ColumnType.UUID},
                                1
                        );
                        int half = rowCount / 2;
                        for (int i = 0; i < half; i++) writer.add(0, i);
                        writer.setMaxValue(half - 1);
                        writer.commit();
                        for (int i = half; i < rowCount; i++) writer.add(0, i);
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                        writer.seal();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.UUID}),
                            EMPTY_CVR, 0)) {
                        try (RowCursor c = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0})) {
                            assertTrue(c instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) c;
                            int seen = 0;
                            while (cc.hasNext()) {
                                cc.next();
                                assertEquals(700L + seen, cc.getCoveredLong128Lo(0));
                                assertEquals(800L + seen, cc.getCoveredLong128Hi(0));
                                seen++;
                            }
                            assertEquals(rowCount, seen);
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * 16, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveredVarcharFsstDecompress() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_dec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (msg),
                        msg VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_dec
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           'A',
                           ('order_confirmation_alpha_beta_gamma_delta_epsilon_zeta_eta_theta_iota_kappa_' || x)::VARCHAR
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM (SELECT msg FROM t_fsst_dec WHERE sym = 'A')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n200\n");

            assertQuery("SELECT msg FROM t_fsst_dec WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            msg
                            order_confirmation_alpha_beta_gamma_delta_epsilon_zeta_eta_theta_iota_kappa_200
                            """);
        });
    }

    @Test
    public void testCoveredVarcharFsstStrideMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_merge (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (msg),
                        msg VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_merge
                    SELECT dateadd('s', (x * 2)::INT, '2024-01-01')::TIMESTAMP,
                           CASE x % 7 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C'
                                      WHEN 3 THEN 'D' WHEN 4 THEN 'E' WHEN 5 THEN 'F' ELSE 'G' END,
                           ('abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_' || (x % 5))::VARCHAR
                    FROM long_sequence(3000)
                    """);
            execute("""
                    INSERT INTO t_fsst_merge
                    SELECT dateadd('s', (x * 2 + 1)::INT, '2024-01-01')::TIMESTAMP,
                           CASE x % 7 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C'
                                      WHEN 3 THEN 'D' WHEN 4 THEN 'E' WHEN 5 THEN 'F' ELSE 'G' END,
                           ('abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_' || (x % 5))::VARCHAR
                    FROM long_sequence(3000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM (SELECT msg FROM t_fsst_merge WHERE sym = 'A')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n856\n");
            // Actually FETCH varchar values from the FSST-flagged sidecar block
            // so the reader walks decompressFsstUtf8 / ensureFsstCacheCapacity.
            // x=2996 has x%7==0 (sym='A') and largest ts in batch 2 (5993).
            // 2996%5==1 so msg ends with _1.
            assertQuery("SELECT msg FROM t_fsst_merge WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            msg
                            abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_abcdefgh_1
                            """);
            assertQuery("SELECT count() c FROM t_fsst_merge WHERE sym = 'A' AND msg LIKE 'abcdefgh%'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("c\n856\n");
        });
    }

    @Test
    public void testCoveringAfterSealThenNewCommit() throws Exception {
        // After seal + new commit: genCount=2. With per-gen sidecars,
        // covering still works — gen0 has sealed sidecar, gen1 has raw sidecar.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_postseal";
                int plen = path.size();

                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    // First writer: write data and close (seal creates sidecar files)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 0; i < 20; i++) {
                            writer.add(i % 5, i);
                        }
                        writer.setMaxValue(19);
                        writer.commit();
                    } // seal + compact

                    // Verify sidecar files exist
                    FilesFacade ff = configuration.getFilesFacade();
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)));

                    // Second writer: reopen without reinit, add more data (creates gen1)
                    PostingIndexWriter writer2 = new PostingIndexWriter(configuration);
                    writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    writer2.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{2},
                            new int[]{ColumnType.DOUBLE},
                            1
                    );
                    for (int i = 20; i < 30; i++) {
                        writer2.add(i % 5, i);
                    }
                    writer2.setMaxValue(29);
                    writer2.commit();

                    // Reader: genCount=2, but per-gen sidecars exist
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        assertTrue(((CoveringRowCursor) cursor).isCoveredAvailable(0));

                        int count = 0;
                        while (cursor.hasNext()) {
                            cursor.next();
                            count++;
                        }
                        assertTrue(count > 0);
                        Misc.free(cursor);
                    }

                    writer2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringAfterSealThenNewCommitValues() throws Exception {
        // After seal + reopen + new commit: verify covered values from both gens.
        // Gen0 (sealed) has stride-indexed sidecar; gen1 (raw) appends after it.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_postseal_vals";
                int plen = path.size();
                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }
                    // Write 20 rows to key 0, close (seal creates dense gen0 + stride sidecar)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3},
                                new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                        );
                        for (int i = 0; i < 20; i++) writer.add(0, i);
                        writer.setMaxValue(19);
                        writer.commit();
                    }
                    // Reopen, add 10 more rows (gen1 raw sidecar appended after seal's stride data)
                    PostingIndexWriter w2 = new PostingIndexWriter(configuration);
                    w2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    w2.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    for (int i = 20; i < 30; i++) w2.add(0, i);
                    w2.setMaxValue(29);
                    w2.commit();
                    // Verify ALL 30 covered values across both gens
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.isCoveredAvailable(0));

                        for (int i = 0; i < 30; i++) {
                            assertTrue("row " + i, cc.hasNext());
                            assertEquals(i, cc.next());
                            assertEquals("value at row " + i, 10.0 * (i + 1), cc.getCoveredDouble(0), 0.001);
                        }
                        assertFalse(cc.hasNext());
                        Misc.free(cc);
                    }
                    w2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringFileNaming() {
        try (Path path = new Path().of("/db/2024-01-01")) {
            int plen = path.size();
            String name = "sym";

            // .pci remains single-version (per postingColumnNameTxn).
            PostingIndexUtils.coverInfoFileName(path, name, COLUMN_NAME_TXN_NONE);
            assertTrue(path.toString().contains("sym.pci"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 0);
            assertTrue(path.toString().contains("sym.pc0.0.0"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 1, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 0);
            assertTrue(path.toString().contains("sym.pc1.0.0"));

            PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, 5);
            assertTrue(path.toString().contains("sym.pci.5"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, 5, 3, 7);
            assertTrue(path.toString().contains("sym.pc0.5.3.7"));
        }
    }

    @Test
    public void testCoveringFlagInMeta() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE flagtest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("flagtest")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                // val column should NOT be covering
                int valIdx = metadata.getColumnIndex("val");
                assertFalse(metadata.getColumnMetadata(valIdx).isCovering());
            }
        });
    }

    @Test
    public void testCoveringHighCardinalityCrossesStrideBoundary() throws Exception {
        // Keys >= 256 cross the stride boundary (DENSE_STRIDE=256).
        // Verify correct sidecar offset computation for second stride.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_stride";
                int plen = path.size();

                int rowCount = 300;
                long colAddr = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},  // Long.BYTES = 8 = 2^3
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );

                        // One row per key, 300 keys (crosses 256 boundary)
                        for (int k = 0; k < rowCount; k++) {
                            writer.add(k, k);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    // Read back keys from second stride (key >= 256)
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG}), EMPTY_CVR, 0)) {
                        // Key 260 is in stride 1, local key 4
                        RowCursor cursor = reader.getCursor(260, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));


                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 299 is in stride 1, local key 43
                        cursor = reader.getCursor(299, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(299, cc.next());
                        assertEquals(1299L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 0 is in stride 0 (control)
                        cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringInList5Keys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cov5 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cov5 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30),
                    ('2024-01-02T00:00:00', 'D', 4.0, 40),
                    ('2024-01-02T01:00:00', 'E', 5.0, 50)
                    """);
            engine.releaseAllWriters();

            // 5-key covering IN-list — all columns covered, uses CoveringIndex path
            assertQuery("SELECT price, qty FROM t_cov5 WHERE sym IN ('A', 'B', 'C', 'D', 'E')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.0\t10
                            2.0\t20
                            3.0\t30
                            4.0\t40
                            5.0\t50
                            """);
        });
    }

    @Test
    public void testCoveringInListString() throws Exception {
        // IN-list with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'Y', 'world'),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_in_string WHERE sym IN ('X', 'Y')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            label
                            hello
                            world
                            foo
                            """);
        });
    }

    @Test
    public void testCoveringInListVarchar() throws Exception {
        // IN-list with VARCHAR covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0),
                    ('2024-01-01T03:00:00', 'C', 'carol', 40.0)
                    """);
            engine.releaseAllWriters();

            // IN list with covering VARCHAR -- rows in ascending timestamp order.
            assertQuery("SELECT name, price FROM t_in_varchar WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            alice\t10.0
                            bob\t20.0
                            anna\t30.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cnt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cnt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            // Count optimization uses CoveringIndex as base — count() doesn't select
            // any covered columns so "with" is absent
            assertQuery("SELECT count() FROM t_cnt WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            Count
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """)
                    .returns("""
                            count
                            3
                            """);
        });
    }

    @Test
    public void testCoveringIndexDistinctPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-02T00:00:00', 'C', 40.0)
                    """);
            engine.releaseAllWriters();

            // Verify data correctness without ORDER BY — PostingIndexDistinct doesn't
            // support static symbol tables needed for ORDER BY
            assertQuery("SELECT DISTINCT sym FROM t_dist")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist
                            """)
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);
        });
    }

    @Test
    public void testCoveringIndexDistinctRespectsIntervalRowBounds() throws Exception {
        // Regression: PostingIndexDistinctRecordCursorFactory must honor the
        // row bounds carried by IntervalFwdPartitionFrameCursor when an
        // interval predicate restricts the query to a sub-range of the
        // partition. Without that, collectDistinctKeys scans every gen in
        // the partition and returns keys whose only matching rows fall
        // outside the requested interval.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_iv (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_iv VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T12:00:00', 'B'),
                    ('2024-01-01T23:00:00', 'C')
                    """);
            engine.releaseAllWriters();

            // Sanity check: the unbounded query returns every key.
            assertQuery("SELECT DISTINCT sym FROM t_dist_iv")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);

            // The interval [00:00, 01:00) carves out only row 0 (sym='A'),
            // so only 'A' is reachable. The bug returns A, B, C because the
            // posting fast path ignores rowLo/rowHi from the interval frame.
            assertQuery("SELECT DISTINCT sym FROM t_dist_iv WHERE ts IN '2024-01-01T00:00:00;1h'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            """);

            // Sanity: the bitmap fallback path (with /*+ no_index */)
            // produces the correct result, confirming the bug is specific
            // to the posting-index DISTINCT factory.
            assertQuery("SELECT /*+ no_index */ DISTINCT sym FROM t_dist_iv WHERE ts IN '2024-01-01T00:00:00;1h'")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            """);
        });
    }

    @Test
    public void testCoveringIndexDistinctRespectsSubqueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_lim (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_lim VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T01:00:00', 'B'),
                    ('2024-01-01T02:00:00', 'C'),
                    ('2024-01-01T03:00:00', 'D')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_dist_lim")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            D
                            """);

            assertQuery("SELECT DISTINCT sym FROM (SELECT sym FROM t_dist_lim LIMIT 2) order by sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    @Test
    public void testCoveringIndexFastLagAccumulatesAcrossWalCommits() throws Exception {
        // Each WAL transaction with sequential timestamps in the active
        // partition takes the fast-lag path through
        // TableWriter.publishPostingIndexesForLastPartitionFastLag, which
        // calls indexer.getWriter().commit(), appending a sparse generation
        // rather than rewriting the whole .pv. After many fast-lag commits the
        // posting chain has multiple pending generations on the active
        // partition; readers must walk all of them. A regression to
        // seal-per-commit would still pass functional checks but defeat the
        // perf goal; a regression that drops pending data would surface here
        // as missing rows in the covering query.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fastlag_acc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            final int commitCount = 30;
            // Two distinct symbols so covering reads exercise per-key cursors
            // across multiple sparse gens, not just key=0.
            String[] syms = {"A", "B"};
            for (int i = 0; i < commitCount; i++) {
                String sym = syms[i % syms.length];
                // 10-minute spacing keeps every row inside the 2024-01-01 partition.
                String ts = String.format("2024-01-01T%02d:%02d:00", i / 6, (i % 6) * 10);
                execute("INSERT INTO t_fastlag_acc VALUES ('" + ts + "', '"
                        + sym + "', " + (i + 0.5) + ", " + (i * 10) + ")");
                drainWalQueue();
            }

            // Total row count visible: proves no commit silently dropped data.
            assertQuery("SELECT count() FROM t_fastlag_acc")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n" + commitCount + "\n");

            // Covering plan + correct per-key results: proves multi-gen reads
            // resolve covering values from the right gen.
            assertQuery("SELECT price, qty FROM t_fastlag_acc WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price, qty
                                  filter: sym='A'
                            """);

            // 'A' is at even indices (0, 2, 4, ..., 28): 15 rows.
            // 'B' is at odd indices (1, 3, 5, ..., 29): 15 rows.
            assertQuery("SELECT count() FROM t_fastlag_acc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n15\n");
            assertQuery("SELECT count() FROM t_fastlag_acc WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n15\n");

            // Aggregation across the covered columns: exercises the covering
            // sidecar reads on every gen that contributed to key 'A'.
            // sum(price) for A = 0.5 + 2.5 + 4.5 + ... + 28.5 = 15 * 14.5 = 217.5
            // sum(qty)   for A = 0 + 20 + 40 + ... + 280     = 15 * 140 = 2100
            assertQuery("SELECT sum(price) sum_price, sum(qty) sum_qty FROM t_fastlag_acc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tsum_qty\n217.5\t2100\n");
        });
    }

    @Test
    public void testCoveringIndexFastLagAutoSealAtMaxGenCount() throws Exception {
        // Drives MAX_GEN_COUNT+5 fast-lag commits to exercise the inline
        // auto-seal that PostingIndexWriter.flushAllPending fires when
        // genCount >= MAX_GEN_COUNT. The auto-seal runs from inside
        // commit() via the WAL fast-lag path; if it corrupts state during
        // the rewrite (e.g., truncates pending sidecar data, mishandles the
        // sealTxn bump), queries return stale or missing rows. Each insert
        // is its own transaction so the writer accumulates one sparse gen
        // per drain.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fastlag_max (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            final int commitCount = PostingIndexUtils.MAX_GEN_COUNT + 5;
            for (int i = 0; i < commitCount; i++) {
                // 1-second spacing fits commitCount rows comfortably inside
                // a single day partition (max 86_400 rows possible).
                long tsMicros = 1_704_067_200_000_000L + i * 1_000_000L; // 2024-01-01T00:00:00 + i sec
                execute("INSERT INTO t_fastlag_max VALUES (" + tsMicros + ", 'A', " + (i + 0.5) + ")");
                drainWalQueue();
            }

            assertQuery("SELECT count() FROM t_fastlag_max")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n" + commitCount + "\n");
            assertQuery("SELECT count() FROM t_fastlag_max WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n" + commitCount + "\n");

            // sum(price) = 0.5 + 1.5 + ... + (commitCount-0.5) = commitCount^2 / 2.
            double expectedSum = (double) commitCount * commitCount / 2.0;
            assertQuery("SELECT sum(price) sum_price FROM t_fastlag_max WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\n" + expectedSum + "\n");

            // Reopen the writer to verify chain state survives a clean cycle:
            // the chain must reload either as a single sealed gen (if
            // close() invoked seal on multi-gen) or as the post-auto-seal
            // sparse tail. Either way the data must remain intact.
            engine.releaseAllWriters();
            assertQuery("SELECT count() FROM t_fastlag_max")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n" + commitCount + "\n");
            assertQuery("SELECT sum(price) sum_price FROM t_fastlag_max WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\n" + expectedSum + "\n");
        });
    }

    @Test
    public void testCoveringIndexFastLagSealsOnPartitionSwitch() throws Exception {
        // Multiple fast-lag commits accumulate sparse generations on the
        // active partition. Switching to a new partition routes through
        // TableWriter.switchPartition, which calls
        // sealIfMultiGen(sealThreshold) on every indexer to compact the
        // retired partition's chain. After the switch, the previous
        // partition's data must remain readable, AND new fast-lag commits
        // on the new partition must work from a clean chain. Catches
        // regressions where switchPartition's seal corrupts the retired
        // partition's index, or where the new partition's writer inherits
        // stale state.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fastlag_switch (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            // Phase 1: 20 fast-lag commits on partition 2024-01-01 (above the
            // default sealThreshold=16, so the upcoming switch must seal).
            for (int i = 0; i < 20; i++) {
                String ts = String.format("2024-01-01T%02d:%02d:00", i / 4, (i % 4) * 15);
                execute("INSERT INTO t_fastlag_switch VALUES ('" + ts + "', 'A', " + (i + 0.5) + ")");
                drainWalQueue();
            }
            assertQuery("SELECT count() FROM t_fastlag_switch WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n20\n");

            // Phase 2: a row in the next day forces a partition switch.
            // switchPartition runs commit() + sealIfMultiGen(threshold) on
            // every indexer; the 2024-01-01 chain compacts to a single
            // sealed gen.
            execute("INSERT INTO t_fastlag_switch VALUES ('2024-01-02T00:00:00', 'A', 100.5)");
            drainWalQueue();

            // Retired partition's data must survive the seal.
            assertQuery("SELECT count() FROM t_fastlag_switch WHERE ts < '2024-01-02' AND sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n20\n");
            // sum(price) for partition 1 = 0.5 + 1.5 + ... + 19.5 = 200.
            assertQuery("SELECT sum(price) sum_price FROM t_fastlag_switch WHERE ts < '2024-01-02' AND sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\n200.0\n");

            // Phase 3: more fast-lag commits on the new partition. These
            // must take the same fast-lag path on a freshly opened writer
            // for partition 2024-01-02.
            for (int i = 1; i <= 5; i++) {
                String ts = String.format("2024-01-02T%02d:00:00", i);
                execute("INSERT INTO t_fastlag_switch VALUES ('" + ts + "', 'A', " + (200 + i) + ".0)");
                drainWalQueue();
            }

            assertQuery("SELECT count() FROM t_fastlag_switch WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n26\n");
            // partition 2024-01-02: 100.5 + 201 + 202 + 203 + 204 + 205 = 1115.5
            assertQuery("SELECT sum(price) sum_price FROM t_fastlag_switch WHERE ts >= '2024-01-02' AND sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\n1115.5\n");
        });
    }

    @Test
    public void testCoveringIndexGroupByAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_grp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_grp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 50.0, 5),
                    ('2024-01-01T05:00:00', 'C', 60.0, 6)
                    """);
            engine.releaseAllWriters();

            // Plan: GroupBy vectorized on top of CoveringIndex
            assertQuery("SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [sum(price),avg(qty)]
                                CoveringIndex on: sym with: price, qty
                                  filter: sym IN ['A','B']
                            """);

            // Single-key GROUP BY — covering and non-covering paths must agree
            String expected = """
                    sym\tsum\tavg
                    A\t90.0\t3.0
                    """;
            assertQuery("SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ sym, sum(price), avg(qty) FROM t_grp WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);

            // IN-list GROUP BY — covering path must match non-covering
            String expectedInList = """
                    sym\tsum\tavg
                    A\t90.0\t3.0
                    B\t60.0\t3.0
                    """;
            assertQuery("SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expectedInList);
            assertQuery("SELECT /*+ no_covering */ sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expectedInList);
        });
    }

    @Test
    public void testCoveringIndexGroupByMinMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_minmax (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_minmax VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'B', 40.0),
                    ('2024-01-01T04:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    sym\tmin\tmax
                    A\t10.0\t50.0
                    """;
            assertQuery("SELECT sym, min(price), max(price) FROM t_minmax WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [min(price),max(price)]
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ sym, min(price), max(price) FROM t_minmax WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testCoveringIndexGroupByMultiPartition() throws Exception {
        // Verify aggregation over covering index works across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_grp_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_grp_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T12:00:00', 'B', 20.0, 2),
                    ('2024-01-02T00:00:00', 'A', 30.0, 3),
                    ('2024-01-02T12:00:00', 'B', 40.0, 4),
                    ('2024-01-03T00:00:00', 'A', 50.0, 5)
                    """);
            engine.releaseAllWriters();

            // Both paths must agree across partitions
            String expected = """
                    sym\tsum\tcount
                    A\t90.0\t3
                    """;
            assertQuery("SELECT sym, sum(price), count() FROM t_grp_mp WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ sym, sum(price), count() FROM t_grp_mp WHERE sym = 'A' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testCoveringIndexLatestOnPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lat (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_lat VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'A', 20.0),
                    ('2024-01-01T02:00:00', 'B', 30.0),
                    ('2024-01-01T03:00:00', 'A', 40.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_lat WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price
                                  filter: sym='A'
                            """)
                    .returns("""
                            price
                            40.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexNextPartitionNullSymbolWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_null_part (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_null_part VALUES
                    ('2025-01-01T00:00:00', 'A', 1.0, 10),
                    ('2025-01-01T01:00:00', 'B', 2.0, 20),
                    ('2025-01-01T02:00:00', 'A', 3.0, 30)
                    """);
            // Second partition with NULL symbol
            execute("INSERT INTO t_null_part (ts) VALUES ('2025-01-02')");
            drainWalQueue();

            assertQuery("SELECT price, qty FROM t_null_part WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.0\t10
                            3.0\t30
                            """);

            // Nonexistent key should return empty across both partitions
            assertQuery("SELECT price, qty FROM t_null_part WHERE sym = 'nonexistent'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterO3Merge() throws Exception {
        // O3CopyJob.updateIndex calls rollbackConditionally before writing.
        // Before the fix, PostingIndexWriter.rollbackConditionally would trigger
        // a destructive in-place re-encode of sealed generations, corrupting
        // sidecar alignment and causing AIOOBE in ALP decode.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_cover (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_cover VALUES
                    ('2024-01-01T10:00:00', 'A', 10.5, 100),
                    ('2024-01-01T12:00:00', 'B', 20.5, 200),
                    ('2024-01-01T14:00:00', 'A', 30.5, 300)
                    """);
            drainWalQueue();

            // O3 insert into same partition triggers merge → rollbackConditionally
            execute("""
                    INSERT INTO t_o3_cover VALUES
                    ('2024-01-01T09:00:00', 'A', 5.5, 50),
                    ('2024-01-01T11:00:00', 'B', 15.5, 150),
                    ('2024-01-01T13:00:00', 'A', 25.5, 250)
                    """);
            drainWalQueue();

            // Verify covering data is readable (would crash with AIOOBE before the fix)
            assertQuery("SELECT price, qty FROM t_o3_cover WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            5.5\t50
                            10.5\t100
                            25.5\t250
                            30.5\t300
                            """);

            assertQuery("SELECT price, qty FROM t_o3_cover WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            15.5\t150
                            20.5\t200
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterO3MergeMultiPartition() throws Exception {
        // O3 merge across multiple partitions: each partition's posting index
        // must survive rollbackConditionally without sidecar corruption.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_mp VALUES
                    ('2024-01-01T12:00:00', 'X', 1.0),
                    ('2024-01-02T12:00:00', 'X', 2.0),
                    ('2024-01-03T12:00:00', 'Y', 3.0)
                    """);
            drainWalQueue();

            // O3: insert rows before existing ones in every partition
            execute("""
                    INSERT INTO t_o3_mp VALUES
                    ('2024-01-01T06:00:00', 'X', 0.5),
                    ('2024-01-02T06:00:00', 'Y', 1.5),
                    ('2024-01-03T06:00:00', 'X', 2.5)
                    """);
            drainWalQueue();

            assertQuery("SELECT val FROM t_o3_mp WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val
                            0.5
                            1.0
                            2.0
                            2.5
                            """);

            assertQuery("SELECT val FROM t_o3_mp WHERE sym = 'Y'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val
                            1.5
                            3.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterRepeatedO3() throws Exception {
        // Multiple rounds of O3 merges on the same partition.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_repeat (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T10:00:00', 'A', 100.0),
                    ('2024-01-01T20:00:00', 'B', 200.0)
                    """);
            drainWalQueue();

            // Round 1: O3 before existing data
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T05:00:00', 'A', 50.0)
                    """);
            drainWalQueue();

            // Round 2: O3 between existing rows
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T15:00:00', 'A', 150.0)
                    """);
            drainWalQueue();

            // Round 3: O3 at the very start
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T01:00:00', 'B', 10.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT price FROM t_o3_repeat WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            50.0
                            100.0
                            150.0
                            """);

            assertQuery("SELECT price FROM t_o3_repeat WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            200.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionDuplicateO3() throws Exception {
        // Duplicate rows via O3 — exercises rollbackConditionally with row > 0
        // where the posting index already has entries at those positions.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            // Exact duplicate insert triggers O3 merge
            execute("""
                    INSERT INTO t_o3_dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            assertQuery("SELECT price, qty FROM t_o3_dup WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            1.5\t10
                            3.5\t30
                            3.5\t30
                            """);

            assertQuery("SELECT price, qty FROM t_o3_dup WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            2.5\t20
                            2.5\t20
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionO3ManyCoveredColumns() throws Exception {
        // Many covered columns of different types — exercises all CoveringCompressor
        // read methods (readDoubleAt, readLongAt, readIntAt) on sidecar data that
        // would have been corrupted by in-place rollback.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_many_cols (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d1, d2, d3, i1, l1),
                        d1 DOUBLE,
                        d2 DOUBLE,
                        d3 DOUBLE,
                        i1 INT,
                        l1 LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_many_cols VALUES
                    ('2024-01-01T10:00:00', 'A', 1.1, 2.2, 3.3, 100, 1000),
                    ('2024-01-01T12:00:00', 'B', 4.4, 5.5, 6.6, 200, 2000),
                    ('2024-01-01T14:00:00', 'A', 7.7, 8.8, 9.9, 300, 3000)
                    """);
            drainWalQueue();

            // O3 merge
            execute("""
                    INSERT INTO t_many_cols VALUES
                    ('2024-01-01T09:00:00', 'A', 0.1, 0.2, 0.3, 10, 100),
                    ('2024-01-01T11:00:00', 'B', 3.3, 3.4, 3.5, 150, 1500)
                    """);
            drainWalQueue();

            assertQuery("SELECT d1, d2, d3, i1, l1 FROM t_many_cols WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d1\td2\td3\ti1\tl1
                            0.1\t0.2\t0.3\t10\t100
                            1.1\t2.2\t3.3\t100\t1000
                            7.7\t8.8\t9.9\t300\t3000
                            """);

            assertQuery("SELECT d1, d2, d3, i1, l1 FROM t_many_cols WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d1\td2\td3\ti1\tl1
                            3.3\t3.4\t3.5\t150\t1500
                            4.4\t5.5\t6.6\t200\t2000
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionO3WithNulls() throws Exception {
        // O3 merge where some rows have NULL symbol values.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T10:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', null, 20.0),
                    ('2024-01-01T14:00:00', 'A', 30.0)
                    """);
            drainWalQueue();

            // O3 insert with NULLs
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T09:00:00', null, 5.0),
                    ('2024-01-01T11:00:00', 'A', 15.0),
                    ('2024-01-01T13:00:00', null, 25.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT val FROM t_o3_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0
                            15.0
                            30.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionWalMultiPartitionThenO3() throws Exception {
        // WAL creates multiple partitions in order, then O3 merges into a prior partition.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mp_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_mp_o3 VALUES
                    ('2024-01-01T12:00:00', 'A', 1.0),
                    ('2024-01-02T12:00:00', 'A', 2.0),
                    ('2024-01-03T12:00:00', 'A', 3.0)
                    """);
            drainWalQueue();

            // O3 into first partition
            execute("""
                    INSERT INTO t_mp_o3 VALUES
                    ('2024-01-01T06:00:00', 'A', 0.5)
                    """);
            drainWalQueue();

            // Verify all partitions are intact
            assertQuery("SELECT price FROM t_mp_o3 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            0.5
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ord (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ord VALUES
                    ('2024-01-01T00:00:00', 'A', 30.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 10.0),
                    ('2024-01-01T03:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_ord WHERE sym = 'A' ORDER BY price")
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [price]
                                SelectedRecord
                                    CoveringIndex on: sym with: price
                                      filter: sym='A'
                            """)
                    .returns("""
                            price
                            10.0
                            30.0
                            50.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexOrderByNoMatchOnSymbolKey() throws Exception {
        // CoveringIndexRecordCursorFactory.ofEmpty leaves frameCursor null when
        // the WHERE symbol value is not in the symbol map. An outer ORDER BY on
        // a SYMBOL column triggers SortKeyEncoder.init -> getSymbolTable on the
        // empty inner cursor; getSymbolTable must return EmptySymbolMapReader
        // rather than NPE on frameCursor.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ord_no_match (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ord_no_match VALUES
                    ('2024-01-01T00:00:00', 'A', 30.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT sym FROM t_ord_no_match WHERE sym = 'NONEXISTENT' ORDER BY sym")
                    .noLeakCheck()
                    .returns("sym\n");
            assertQuery("(SELECT sym FROM t_ord_no_match WHERE sym = 'NONEXISTENT' ORDER BY price) ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\n");
        });
    }

    @Test
    public void testCoveringIndexResidualFilterMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', 'A', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0),
                    ('2024-01-02T12:00:00', 'B', 40.0),
                    ('2024-01-03T00:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            // Cross-partition with residual filter
            String expected = """
                    price
                    30.0
                    50.0
                    """;
            assertQuery("SELECT price FROM t_resid_mp WHERE sym = 'A' AND price >= 30")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ price FROM t_resid_mp WHERE sym = 'A' AND price >= 30")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testCoveringIndexResidualFilterRandomAccessAcrossFrames() throws Exception {
        // Regression for the async-filter-over-covering random-access contract.
        //
        // SELECT name, price ... WHERE sym = 'A' AND price > N compiles to
        // Async Filter -> CoveringIndex. AsyncFilteredRecordCursor services
        // recordAt()/getRecordB() through its own page-frame memory pool, so the
        // factory reports recordCursorSupportsRandomAccess() == true even though
        // the covering base's row cursor does not support random access.
        //
        // The builder's random-access path re-reads every result row via
        // recordAt() AFTER full forward iteration (so earlier frames' async queue
        // items have already been released) and asserts the values match. Forcing
        // a 2-row covering frame cap spreads the 7 matching rows across 6 frames
        // and 3 partitions, so a stale or garbled cross-frame recordAt would
        // surface here as a VARCHAR or price mismatch.
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
        try {
            assertMemoryLeak(() -> {
                execute("""
                        CREATE TABLE t_cov_ra (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                            name VARCHAR,
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                execute("""
                        INSERT INTO t_cov_ra VALUES
                        ('2024-01-01T00:00:00', 'A', 'a1', 10.0),
                        ('2024-01-01T01:00:00', 'A', 'a2', 20.0),
                        ('2024-01-01T02:00:00', 'A', 'a3', 30.0),
                        ('2024-01-01T03:00:00', 'B', 'bx', 99.0),
                        ('2024-01-02T00:00:00', 'A', 'b1', 40.0),
                        ('2024-01-02T01:00:00', 'A', 'b2', 50.0),
                        ('2024-01-02T02:00:00', 'A', 'b3', 60.0),
                        ('2024-01-03T00:00:00', 'A', 'c1', 70.0),
                        ('2024-01-03T01:00:00', 'A', 'c2', 80.0),
                        ('2024-01-03T02:00:00', 'A', 'c3', 90.0)
                        """);
                engine.releaseAllWriters();

                // Confirm the plan really is async-filter-over-covering.
                // Random access enabled (no .noRandomAccess()): the builder re-reads
                // every matching row via recordAt() across the 6 covering frames.
                assertQuery("SELECT name, price FROM t_cov_ra WHERE sym = 'A' AND price > 25")
                        .noLeakCheck()
                        .withPlanContaining("Async Filter", "CoveringIndex on: sym")
                        .returns("""
                                name\tprice
                                a3\t30.0
                                b1\t40.0
                                b2\t50.0
                                b3\t60.0
                                c1\t70.0
                                c2\t80.0
                                c3\t90.0
                                """);
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testCoveringIndexResidualFilterUncoveredColumn() throws Exception {
        // When the filter references an uncovered column, covering index should NOT be used
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_uncov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_uncov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 100),
                    ('2024-01-01T01:00:00', 'A', 30.0, 200)
                    """);
            engine.releaseAllWriters();

            // Filter on uncovered column 'extra' — can't use covering index; data must still be correct.
            assertQuery("SELECT price FROM t_resid_uncov WHERE sym = 'A' AND extra > 150")
                    .noLeakCheck()
                    .withPlanNotContaining("CoveringIndex")
                    .returns("""
                            price
                            30.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexSidecarAlterAddIndexMultiPartition() throws Exception {
        // ALTER TABLE ADD INDEX INCLUDE on a table with multiple existing partitions.
        // All partitions must get sidecar files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_mp (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_mp VALUES
                    ('2024-01-01T09:00:00', 'A', 1.0, 10),
                    ('2024-01-01T10:00:00', 'B', 2.0, 20),
                    ('2024-01-02T09:00:00', 'A', 3.0, 30),
                    ('2024-01-03T09:00:00', 'B', 4.0, 40)
                    """);
            execute("ALTER TABLE t_alter_mp ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_alter_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.0\t10
                            3.0\t30
                            """);

            assertQuery("SELECT price, qty FROM t_alter_mp WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            2.0\t20
                            4.0\t40
                            """);
        });
    }

    @Test
    public void testCoveringIndexSidecarSurvivesPartitionSwitch() throws Exception {
        // After ALTER TABLE ADD INDEX INCLUDE, inserting into the next partition
        // must not corrupt the prior partition's sidecar files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_switch (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_switch VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            execute("ALTER TABLE t_switch ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");

            // Insert into next partition — triggers partition switch and seal
            execute("""
                    INSERT INTO t_switch VALUES
                    ('2024-01-02T09:00:00', 'A', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50)
                    """);
            engine.releaseAllWriters();

            // Both partitions should return correct covered data
            assertQuery("SELECT price, qty FROM t_switch WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            4.5\t40
                            """);

            assertQuery("SELECT price, qty FROM t_switch WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            2.5\t20
                            5.5\t50
                            """);
        });
    }

    @Test
    public void testCoveringIndexSidecarSurvivesPartitionSwitchWal() throws Exception {
        // Same partition-switch scenario through WAL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_switch_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_switch_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_switch_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            // Insert into next partition
            execute("""
                    INSERT INTO t_switch_wal VALUES
                    ('2024-01-02T09:00:00', 'A', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50)
                    """);
            drainWalQueue();

            assertQuery("SELECT price, qty FROM t_switch_wal WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            4.5\t40
                            """);
        });
    }

    @Test
    public void testCoveringIndexSidecarWrittenOnAlterAddIndex() throws Exception {
        // ALTER TABLE ADD INDEX INCLUDE on a table with existing data must
        // seal the posting index for the last partition so that sidecar files
        // exist before any query uses the covering scan plan.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_cover (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_cover VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            execute("ALTER TABLE t_alter_cover ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Covering query — all selected columns are in INCLUDE
            assertQuery("SELECT price, qty FROM t_alter_cover WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            """);
        });
    }

    @Test
    public void testCoveringIndexSidecarWrittenOnAlterAddIndexWal() throws Exception {
        // Same as above but through the WAL path.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_alter_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_alter_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            assertQuery("SELECT price, qty FROM t_alter_wal WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            """);
        });
    }

    @Test
    public void testCoveringIndexWithResidualFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 50.0, 5)
                    """);
            engine.releaseAllWriters();

            // Plan: Async Filter wrapping CoveringIndex (parallel filter enabled in tests)
            // Data correctness: only A rows with price > 15
            assertQuery("SELECT price FROM t_resid WHERE sym = 'A' AND price > 15")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                Async Filter workers: 1
                                  filter: 15<price
                                    CoveringIndex on: sym with: price
                                      filter: sym='A'
                            """)
                    .returns("""
                            price
                            30.0
                            50.0
                            """);

            // Must match non-covering path
            assertQuery("SELECT /*+ no_covering */ price FROM t_resid WHERE sym = 'A' AND price > 15")
                    .noLeakCheck()
                    .returns("""
                            price
                            30.0
                            50.0
                            """);
        });
    }

    @Test
    public void testCoveringIndexWithResidualFilterInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'B', 40.0),
                    ('2024-01-01T04:00:00', 'C', 50.0)
                    """);
            engine.releaseAllWriters();

            // IN-list with residual filter
            assertQuery("SELECT price FROM t_resid_in WHERE sym IN ('A', 'B') AND price > 25")
                    .noLeakCheck()
                    .returns("""
                            price
                            30.0
                            40.0
                            """);
            assertQuery("SELECT /*+ no_covering */ price FROM t_resid_in WHERE sym IN ('A', 'B') AND price > 25")
                    .noLeakCheck()
                    .returns("""
                            price
                            30.0
                            40.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'A', 2.0),
                    ('2024-01-02T12:00:00', 'B', 3.0)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            assertQuery("SELECT price FROM t_latest_bind WHERE sym = :sym LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            2.0
                            """);

            // Re-execute with different value
            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");
            assertQuery("SELECT price FROM t_latest_bind WHERE sym = :sym LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            3.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_latest_plan WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price
                                  filter: sym='A'
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnMultiKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mk VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T12:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T12:00:00', 'C', 4.0),
                    ('2024-01-03T00:00:00', 'B', 5.0),
                    ('2024-01-03T12:00:00', 'A', 6.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON with IN list: latest A is 6.0 (day 3), latest B is 5.0 (day 3)
            assertQuery("SELECT price FROM t_latest_mk WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            6.0
                            5.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnMultiKeyWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mk_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mk_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 1.0),
                    ('2024-01-01T12:00:00', 'B', 'hot', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'warm', 3.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT sym, tag, price FROM t_latest_mk_sym WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\ttag\tprice
                            A\twarm\t3.0
                            B\thot\t2.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 100),
                    ('2024-01-01T12:00:00', 'B', 20.0, 200),
                    ('2024-01-02T00:00:00', 'A', 30.0, 300),
                    ('2024-01-03T00:00:00', 'B', 40.0, 400)
                    """);
            engine.releaseAllWriters();

            // Latest A is in partition 2024-01-02
            assertQuery("SELECT price, qty FROM t_latest_mp WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            30.0\t300
                            """);

            // Latest B is in partition 2024-01-03
            assertQuery("SELECT price, qty FROM t_latest_mp WHERE sym = 'B' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            40.0\t400
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_latest_ne WHERE sym = 'MISSING' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnSingleKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T12:00:00', 'A', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T12:00:00', 'B', 4.0),
                    ('2024-01-03T00:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON should return the latest row for sym='A' (from last partition)
            assertQuery("SELECT price FROM t_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            5.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnString() throws Exception {
        // LATEST ON with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-02T00:00:00', 'X', 'world'),
                    ('2024-01-02T01:00:00', 'Y', 'bar')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_latest_string WHERE sym = 'X' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            label
                            world
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnVarchar() throws Exception {
        // LATEST ON with VARCHAR covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'anna', 30.0),
                    ('2024-01-02T01:00:00', 'B', 'bea', 40.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_latest_varchar WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            anna\t30.0
                            """);

            assertQuery("SELECT name, price FROM t_latest_varchar WHERE sym = 'B' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            bea\t40.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_latest_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0)
                    """);
            drainWalQueue();

            // Non-covering LATEST ON (SELECT * includes ts, not in INCLUDE list)
            assertQuery("SELECT * FROM t_latest_wal WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-02T00:00:00.000000Z\tA\t3.0
                            """);
        });
    }

    @Test
    public void testCoveringLatestOnWithSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 1.0),
                    ('2024-01-02T00:00:00', 'A', 'hot', 2.0),
                    ('2024-01-03T00:00:00', 'A', 'warm', 3.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT sym, tag, price FROM t_latest_sym WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\ttag\tprice
                            A\twarm\t3.0
                            """);
        });
    }

    @Test
    public void testCoveringMetadataSurvivesAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("""
                    INSERT INTO t_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_alter WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            11.5
                            """);

            execute("ALTER TABLE t_alter ADD COLUMN extra INT");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_alter")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue("covering flag lost after ALTER", r.getMetadata().getColumnMetadata(symIdx).isCovering());
                IntList indices = r.getMetadata().getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering indices lost after ALTER", indices);
                assertEquals(2, indices.size());
            }

            assertQuery("SELECT price FROM t_alter WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            11.5
                            """);
        });
    }

    @Test
    public void testCoveringMultiColumnMultiCommit() throws Exception {
        // Multi-column covering (DOUBLE + INT) with multiple commits.
        // Each column's sidecar file grows at different rates (8B vs 4B per value).
        // Verifies per-column sidecar offset computation is correct.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mc_offset (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i),
                        d DOUBLE,
                        i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T00:00:00', 'A', 1.5, 10), ('2024-01-01T01:00:00', 'B', 2.5, 20)");
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T02:00:00', 'A', 3.5, 30), ('2024-01-01T03:00:00', 'B', 4.5, 40)");
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T04:00:00', 'A', 5.5, 50)");
            engine.releaseAllWriters();

            assertQuery("SELECT d, i FROM t_mc_offset WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d\ti
                            1.5\t10
                            3.5\t30
                            5.5\t50
                            """);

            assertQuery("SELECT d, i FROM t_mc_offset WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d\ti
                            2.5\t20
                            4.5\t40
                            """);
        });
    }

    @Test
    public void testCoveringMultiCommitMixedTypes() throws Exception {
        // Multiple commits with LONG, INT, SHORT covered columns.
        // Tests per-gen raw sidecar reading for non-DOUBLE types.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mc_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (l, i, s),
                        l LONG,
                        i INT,
                        s SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T00:00:00', 'A', 100_000, 10, 1), ('2024-01-01T01:00:00', 'B', 200_000, 20, 2)");
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T02:00:00', 'A', 300_000, 30, 3), ('2024-01-01T03:00:00', 'B', 400_000, 40, 4)");
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T04:00:00', 'A', 500_000, 50, 5)");
            engine.releaseAllWriters();

            assertQuery("SELECT l, i, s FROM t_mc_mixed WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            l\ti\ts
                            100000\t10\t1
                            300000\t30\t3
                            500000\t50\t5
                            """);
        });
    }

    @Test
    public void testCoveringMultiCommitSamePartition() throws Exception {
        // Multiple commits to the same partition, then seal.
        // Verifies covering data is correct after multi-gen seal.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_commit (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T00:00:00', 'A', 10.0)");
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T01:00:00', 'A', 11.0)");
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T02:00:00', 'B', 20.0)");
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_multi_commit WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            11.0
                            """);
        });
    }

    @Test
    public void testCoveringMultiGenFallback() throws Exception {
        // With per-gen sidecars, isCoveredAvailable(0) returns true even with genCount > 1.
        // Covering data is written during each commit's flushAllPending().
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_multigen";
                int plen = path.size();

                int rowCount = 20;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }

                    // Write index with covering, two commits (two gens), do NOT close (no seal)
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{2},
                            new int[]{ColumnType.DOUBLE},
                            1
                    );

                    // Gen 0
                    for (int i = 0; i < 10; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(9);
                    writer.commit();

                    // Gen 1 — index now has genCount=2 with per-gen sidecar data
                    for (int i = 10; i < 20; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    // Reader sees genCount=2 with per-gen sidecar data
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        // Per-gen sidecars: isCoveredAvailable(0) returns true
                        assertTrue(cc.isCoveredAvailable(0));


                        // Row IDs and covered values are correct
                        int count = 0;
                        while (cursor.hasNext()) {
                            long rowId = cursor.next();
                            assertEquals(count, rowId);
                            double covered = cc.getCoveredDouble(0);
                            assertEquals("covered value at row " + count, 100.0 + count, covered, 0.001);
                            count++;
                        }
                        assertEquals(20, count);
                        Misc.free(cursor);
                    }

                    writer.close(); // seal happens here
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringMultiGenKeyAbsentInOneGen() throws Exception {
        // Key 0 has data only in gen 0, key 1 in both gens.
        // Verifies per-gen sidecar reads skip empty key ranges correctly.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_absent_gen";
                int plen = path.size();
                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    // Gen 0: key 0 = rows 0,2; key 1 = rows 1,3
                    writer.add(0, 0);
                    writer.add(0, 2);
                    writer.add(1, 1);
                    writer.add(1, 3);
                    writer.setMaxValue(3);
                    writer.commit();
                    // Gen 1: ONLY key 1 = rows 4,5
                    writer.add(1, 4);
                    writer.add(1, 5);
                    writer.setMaxValue(5);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // Key 0: only gen 0 data (rows 0,2)
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(10.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(2, cc.next());
                        assertEquals(30.0, cc.getCoveredDouble(0), 0.001);
                        assertFalse(cc.hasNext());

                        // Key 1: gen 0 + gen 1 (rows 1,3,4,5)
                        cc = (CoveringRowCursor) reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(20.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(40.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(4, cc.next());
                        assertEquals(50.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(5, cc.next());
                        assertEquals(60.0, cc.getCoveredDouble(0), 0.001);
                        assertFalse(cc.hasNext());
                    }
                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringMultiGenMultiKey() throws Exception {
        // Per-gen sidecar reads with multiple keys, verifying the sidecarBase
        // offset computation is correct (key > 0 must skip earlier keys' values).
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_multigen_mk";
                int plen = path.size();
                int rowCount = 20;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    // Gen 0: key 0 = rows 0,2,4,6,8; key 1 = rows 1,3,5,7,9
                    for (int i = 0; i < 10; i++) {
                        writer.add(i % 2, i);
                    }
                    writer.setMaxValue(9);
                    writer.commit();
                    // Gen 1: key 0 = rows 10,12,14,16,18; key 1 = rows 11,13,15,17,19
                    for (int i = 10; i < 20; i++) {
                        writer.add(i % 2, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // Key 1: rows 1,3,5,7,9,11,13,15,17,19
                        RowCursor cursor = reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));

                        int count = 0;
                        while (cc.hasNext()) {
                            long rowId = cc.next();
                            assertEquals(count * 2L + 1, rowId);
                            assertEquals(100.0 + count * 2 + 1, cc.getCoveredDouble(0), 0.001);
                            count++;
                        }
                        assertEquals(10, count);
                        Misc.free(cursor);
                    }
                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringNullSentinelGeoShort() throws Exception {
        // Verify GeoHashes.SHORT_NULL (-1) is used for GEOSHORT covering columns
        // below columnTop, not (short) 0
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_geo";
                int plen = path.size();

                int rowCount = 4;
                long colTop = 2; // first 2 rows below column top
                long colAddr = Unsafe.malloc((long) rowCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putShort(colAddr + (long) i * Short.BYTES, (short) (100 + i));
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{colTop},
                                new int[]{1},  // Short.BYTES = 2 = 2^1
                                new int[]{1},
                                new int[]{ColumnType.GEOSHORT},
                                1
                        );

                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.GEOSHORT}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));


                        // Rows 0-1: below columnTop, must get GeoHashes.SHORT_NULL (-1)
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(-1, cc.getCoveredShort(0));

                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(-1, cc.getCoveredShort(0));

                        // Rows 2-3: above columnTop, actual values
                        assertTrue(cc.hasNext());
                        assertEquals(2, cc.next());
                        assertEquals(100, cc.getCoveredShort(0));

                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(101, cc.getCoveredShort(0));

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringQueryAllNullCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_allnull (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_allnull VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', NULL, NULL),
                    ('2024-01-01T02:00:00', 'B', 10.5, 100)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_allnull WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            null\tnull
                            null\tnull
                            """);
        });
    }

    @Test
    public void testCoveringQueryArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3),
                        x::INT
                    FROM long_sequence(8)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen coverage
            execute("""
                    INSERT INTO t_arr
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (8 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3),
                        (8 + x)::INT
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify array data matches
            assertQuery("SELECT vals, extra FROM t_arr WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            vals\textra
                            [0.19202208853547864,0.5093827001617407,0.11427984775756228,0.5243722859289777,0.8072372233384567,null,0.6276954028373309,0.6778564558839208,0.8756771741121929,0.8799634725391621,0.5249321062686694]\t2
                            [0.9038068796506872,null]\t4
                            [0.16474369169931913,0.931192737286751]\t6
                            [0.7588175403454873,0.5778947915182423,0.9269068519549879,0.5449155021518948,0.1202416087573498,0.9640289041849747,null,null,0.6359144993891355,null,0.4971342426836798,0.48558682958070665,0.9047642416961028,0.03167026265669903,0.14830552335848957]\t8
                            [null,0.053594208204197136]\t10
                            [null,0.021189232728939578,0.7777024823107295]\t12
                            """);
        });
    }

    @Test
    public void testCoveringQueryArrayColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_arr_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr_null
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3, 3),
                        x::INT
                    FROM long_sequence(12)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT vals, extra FROM t_arr_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            vals\textra
                            [0.19202208853547864,0.5093827001617407,0.11427984775756228]\t2
                            [null,0.1985581797355932,null]\t4
                            [0.12503042190293423,0.9038068796506872,null]\t6
                            [0.022965637512889825,null]\t8
                            [0.18769708157331322,null,null]\t10
                            [0.45659895188239796,0.9566236549439661,0.5406709846540508]\t12
                            """);
        });
    }

    @Test
    public void testCoveringQueryArrayDimLenAndElement() throws Exception {
        // Reading an array column back whole goes through CoveringRecord.getArray(). Reading only a
        // dimension or a single element does not: dim_length() and arr[i] take Record's
        // getArrayDimLen()/getArrayDouble1d2d() defaults, which call getArray() and then dereference
        // what comes back. A NULL array has to survive that route as a NULL answer rather than an
        // NPE, which is what pins CoveringRecord on the no-Java-null side of the getArray() contract.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_arr_dim (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr_dim VALUES
                    ('2024-01-01T00:00:00', 'A', ARRAY[1.0, 2.0, 3.0], 1),
                    ('2024-01-01T01:00:00', 'B', ARRAY[9.0], 2),
                    ('2024-01-01T02:00:00', 'A', NULL, 3),
                    ('2024-01-01T03:00:00', 'A', ARRAY[4.0, 5.0], 4)
                    """);
            engine.releaseAllWriters();

            // Pin the route: read off the covering index, not the table. Both accessors reach
            // CoveringRecord only from here, and a query that quietly fell back to a frame scan
            // would still return these rows while covering none of it.
            assertQuery("SELECT extra, dim_length(vals, 1) len, vals[1] first FROM t_arr_dim WHERE sym = 'A'")
                    .withPlanContaining("CoveringIndex on: sym with: extra, vals")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            extra\tlen\tfirst
                            1\t3\t1.0
                            3\tnull\tnull
                            4\t2\t4.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryArrayDimLenAndElementOverUnavailableSidecar() throws Exception {
        // A sidecar the reader cannot map makes getVarSidecarArray() hand back a Java null for a row
        // whose array is not null at all. This drives the missing-file guard: ensureSidecarOpen()
        // returns quietly and leaves the slot at size 0 when the .pc file is not there to be mapped.
        // The sibling guard reaches the identical reader state from a published zero end offset,
        // which PostingIndexWriter's in-place reseal window can produce over a sidecar that exists.
        // CoveringRecord.getArray() has to turn that into a NULL ArrayView: getArrayDimLen() and
        // getArrayDouble1d2d() dereference whatever getArray() hands them, so a Java null takes the
        // whole query down.
        final AtomicBoolean hideSidecar = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ name) {
                // Arm only after the write side has published the sidecar, so the index is built
                // normally and only the covered read finds the .pc missing.
                if (hideSidecar.get() && name != null && Utf8s.containsAscii(name, ".pc0.")) {
                    return false;
                }
                return super.exists(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_arr_sidecar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr_sidecar VALUES
                    ('2024-01-01T00:00:00', 'A', ARRAY[1.0, 2.0, 3.0], 1),
                    ('2024-01-01T01:00:00', 'A', ARRAY[4.0, 5.0], 2)
                    """);
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            hideSidecar.set(true);
            // Every array stored here is non-null, so a null length or element can only have come
            // from the unmappable sidecar, never from the data.
            assertQuery("SELECT extra, dim_length(vals, 1) len, vals[1] first FROM t_arr_sidecar WHERE sym = 'A'")
                    .withPlanContaining("CoveringIndex on: sym with: extra, vals")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            extra\tlen\tfirst
                            1\tnull\tnull
                            2\tnull\tnull
                            """);
        });
    }

    @Test
    public void testCoveringQueryBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_basic (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_basic VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150),
                    ('2024-01-01T03:00:00', 'C', 30.5, 300),
                    ('2024-01-01T04:00:00', 'B', 21.5, 250),
                    ('2024-01-01T05:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_basic WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            11.5\t150
                            12.5\t120
                            """);
        });
    }

    @Test
    public void testCoveringQueryBinaryColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data, extra),
                        data BINARY,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two commits to exercise both sparse (gen1) and sealed (gen0) paths
            execute("""
                    INSERT INTO t_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 2),
                        x::INT
                    FROM long_sequence(6)
                    """);
            engine.releaseAllWriters();
            execute("""
                    INSERT INTO t_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (6 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 2),
                        (6 + x)::INT
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify BINARY data matches
            assertQuery("SELECT data, extra FROM t_bin WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            data\textra
                            \t2
                            \t4
                            00000000 1b c7 88 de a0 79 3c\t6
                            \t8
                            00000000 49 b4 59 7e 3b 08\t10
                            """);
        });
    }

    @Test
    public void testCoveringQueryBinaryColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bin_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data, extra),
                        data BINARY,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bin_null
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 3 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 3),
                        x::INT
                    FROM long_sequence(12)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT data, extra FROM t_bin_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            data\textra
                            00000000 3b 72 db f3 04 1b c7 88\t3
                            00000000 08 a1 1e 38 8d 1b 9e f4\t6
                            \t9
                            00000000 f0 2d 40 e2 4b\t12
                            """);
        });
    }

    @Test
    public void testCoveringQueryBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            assertQuery("SELECT price FROM t_bind WHERE sym = :sym")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            11.5
                            """);

            // Re-execute with different bind variable value
            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");
            assertQuery("SELECT price FROM t_bind WHERE sym = :sym")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            20.5
                            """);
        });
    }

    @Test
    public void testCoveringQueryBindVariableNonExistent() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "NONEXISTENT");
            assertQuery("SELECT price FROM t_bind_ne WHERE sym = :sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            """);
        });
    }

    @Test
    public void testCoveringQueryBindVariableRebindPageFrame() throws Exception {
        // Regression test for the page-frame path (exercised by count()
        // pushdown, GROUP BY, etc.). Rebinding the symbol must pick up the
        // new bind value on re-execution.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_pf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_pf VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'B', 21.5),
                    ('2024-01-01T04:00:00', 'B', 22.5)
                    """);
            engine.releaseAllWriters();

            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "sym=A",
                    """
                            count
                            2
                            """,
                    bindVariableService -> bindVariableService.setStr("sym", "A")
            ));
            cases.add(BindVarTuple.ok(
                    "sym=B",
                    """
                            count
                            3
                            """,
                    bindVariableService -> bindVariableService.setStr("sym", "B")
            ));
            assertQuery("SELECT count() FROM t_bind_pf WHERE sym = :sym")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testCoveringQueryBindVariableRebindReusesFactory() throws Exception {
        // Regression test: re-executing a prepared covering-index query with a
        // different bind value must return rows for the new value. The
        // single-key path used to cache the resolved symbol key in the cursor
        // across executions.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_rebind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_rebind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'B', 21.5)
                    """);
            engine.releaseAllWriters();

            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "sym=A",
                    """
                            price
                            10.5
                            11.5
                            """,
                    bindVariableService -> bindVariableService.setStr("sym", "A")
            ));
            cases.add(BindVarTuple.ok(
                    "sym=B",
                    """
                            price
                            20.5
                            21.5
                            """,
                    bindVariableService -> bindVariableService.setStr("sym", "B")
            ));
            // And back to A to cover both directions of the transition.
            cases.add(BindVarTuple.ok(
                    "sym=A again",
                    """
                            price
                            10.5
                            11.5
                            """,
                    bindVariableService -> bindVariableService.setStr("sym", "A")
            ));
            assertQuery("SELECT price FROM t_bind_rebind WHERE sym = :sym")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testCoveringQueryBooleanColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bool (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (flag, extra),
                        flag BOOLEAN,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bool VALUES
                    ('2024-01-01T00:00:00', 'A', true, 1),
                    ('2024-01-01T01:00:00', 'B', false, 2),
                    ('2024-01-01T02:00:00', 'A', false, 3),
                    ('2024-01-01T03:00:00', 'A', true, 4)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT flag, extra FROM t_bool WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            flag\textra
                            true\t1
                            false\t3
                            true\t4
                            """);
        });
    }

    @Test
    public void testCoveringQueryBoundaryValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_boundary (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i),
                        d DOUBLE,
                        i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_boundary VALUES
                    ('2024-01-01T00:00:00', 'A', 1.7976931348623157E308, 2147483647),
                    ('2024-01-01T01:00:00', 'A', -1.7976931348623157E308, -2147483648),
                    ('2024-01-01T02:00:00', 'A', 0.0, 0)
                    """);
            engine.releaseAllWriters();

            // INT MIN_VALUE is the NULL sentinel; displays as "null" via covering path
            assertQuery("SELECT d, i FROM t_boundary WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d\ti
                            1.7976931348623157E308\t2147483647
                            -1.7976931348623157E308\tnull
                            0.0\t0
                            """);
        });
    }

    @Test
    public void testCoveringQueryByteColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_byte (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (b, extra),
                        b BYTE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_byte VALUES
                    ('2024-01-01T00:00:00', 'X', 42, 1),
                    ('2024-01-01T01:00:00', 'Y', 0, 2),
                    ('2024-01-01T02:00:00', 'X', -1, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT b, extra FROM t_byte WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            b\textra
                            42\t1
                            -1\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryDateColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_date (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, extra),
                        d DATE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_date VALUES
                    ('2024-01-01T00:00:00', 'A', '2020-06-15T00:00:00.000Z', 1),
                    ('2024-01-01T01:00:00', 'B', '2021-12-25T00:00:00.000Z', 2),
                    ('2024-01-01T02:00:00', 'A', '2022-03-01T00:00:00.000Z', 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT d, extra FROM t_date WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d\textra
                            2020-06-15T00:00:00.000Z\t1
                            2022-03-01T00:00:00.000Z\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryDecimal128ByteOrder() throws Exception {
        // Verifies that DECIMAL128 byte order (high word at offset 0, low word
        // at offset 8) survives the covering index round-trip. Inserts known
        // values and checks exact output rather than relying on random data.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_d128 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d128),
                        d128 DECIMAL(38, 10)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_d128 VALUES
                        ('2024-01-01T00:00:00', 'A', '1234567890.1234567890'::DECIMAL(38, 10)),
                        ('2024-01-01T01:00:00', 'A', '-9999999999.9999999999'::DECIMAL(38, 10)),
                        ('2024-01-01T02:00:00', 'A', '0.0000000001'::DECIMAL(38, 10)),
                        ('2024-01-01T03:00:00', 'B', '99999999999999999999999999.9999999999'::DECIMAL(38, 10)),
                        ('2024-01-01T04:00:00', 'A', NULL)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen
            execute("INSERT INTO t_d128 VALUES ('2024-01-01T05:00:00', 'A', '42'::DECIMAL(38, 10))");
            engine.releaseAllWriters();

            assertQuery("SELECT d128 FROM t_d128 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d128
                            1234567890.1234567890
                            -9999999999.9999999999
                            0.0000000001
                            
                            42.0000000000
                            """);
            assertQuery("SELECT d128 FROM t_d128 WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d128
                            99999999999999999999999999.9999999999
                            """);
        });
    }

    @Test
    public void testCoveringQueryDecimalAllSizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d8, d16, d32, d64, d128, d256),
                        d8 DECIMAL(2, 1),
                        d16 DECIMAL(4, 2),
                        d32 DECIMAL(9, 2),
                        d64 DECIMAL(18, 4),
                        d128 DECIMAL(38, 10),
                        d256 DECIMAL(40, 5)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dec
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_decimal(2, 1, 3),
                        rnd_decimal(4, 2, 3),
                        rnd_decimal(9, 2, 3),
                        rnd_decimal(18, 4, 3),
                        rnd_decimal(38, 10, 3),
                        rnd_decimal(40, 5, 3)
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen coverage
            execute("""
                    INSERT INTO t_dec
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (10 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_decimal(2, 1, 3),
                        rnd_decimal(4, 2, 3),
                        rnd_decimal(9, 2, 3),
                        rnd_decimal(18, 4, 3),
                        rnd_decimal(38, 10, 3),
                        rnd_decimal(40, 5, 3)
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify all decimal sizes match
            assertQuery("SELECT d8, d16, d32, d64, d128, d256 FROM t_dec WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d8\td16\td32\td64\td128\td256
                            \t70.80\t\t40863994239165.1703\t1787280113583629108167793234.9373449732\t18888715180791238225928221362458460.92766
                            7.0\t\t2538903.64\t16664082490389.7958\t1641632316967050245515388573.3440205453\t14875755943028931385865395098236153.96808
                            3.1\t78.26\t5751353.94\t23804269374864.1413\t810081242630250895698426776.6927411277\t8583077020202156956218481394266599.02389
                            7.5\t\t\t86605238667466.9519\t956563306327071686987346799.6830377222\t22724816918678007437269485083139146.44429
                            1.7\t92.66\t8823714.73\t13188254446200.8274\t1207097311830897670430779224.2024504775\t29150523193729163716100989803355305.47666
                            6.5\t66.70\t5446956.70\t14417228720079.2492\t1418531814025018706708158328.3816287698\t5732543185464093840813081895232786.18866
                            1.8\t\t8892248.06\t39899107525936.1297\t1016253333977104912056699053.5900116824\t1732803907904724146465147103771127.97855
                            """);

            assertQuery("SELECT d8, d16, d32, d64, d128, d256 FROM t_dec WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d8\td16\td32\td64\td128\td256
                            3.9\t\t735757.01\t61184357814108.3005\t1205323264613915778464177818.0211586008\t1386075460267063369330913776166802.31153
                            5.8\t57.62\t1018221.05\t47578458180646.1665\t693332620165057781283181359.2820666308\t15151479461332606562822538694713052.93139
                            3.9\t5.57\t1379694.58\t20559518411576.0695\t601247527511781476692712681.1373876190\t1948200433546889932354865222990931.08533
                            3.4\t37.66\t6273933.81\t31612360735939.2493\t315864032070159436143241665.0969926445\t29659946948645594828374741791005525.18833
                            2.4\t12.38\t9505528.43\t21423064588439.9731\t645239438434308363262983650.6793302617\t7927075818711980216168116892213920.99887
                            0.9\t\t7953593.56\t91590662830857.7949\t751262867641396953441858009.1969477517\t13167451642297942276784652339229868.67664
                            8.4\t\t9429993.84\t62429987870713.5916\t822752991025940024709620990.3877240957\t28031524510998509335616827333341238.08766
                            """);
        });
    }

    @Test
    public void testCoveringQueryDecimalNullSentinel() throws Exception {
        // Red test for PostingIndexWriter.writeNullSentinel
        // (PostingIndexWriter.java:1340 and :1348) — both overloads write
        // zero bytes for DECIMAL8 / DECIMAL16 NULL sentinels instead of
        // Decimals.DECIMAL8_NULL (Byte.MIN_VALUE) / DECIMAL16_NULL
        // (Short.MIN_VALUE). The bug only fires for rows where the cover
        // column did not exist (rowId < colTop, e.g., column added later
        // via ALTER), since rows that have data are copied byte-for-byte
        // via Unsafe.copyMemory. CursorPrinter (CursorPrinter.java:301,
        // :330) compares against the proper sentinel — when the sidecar
        // holds zero, the covering path renders "0.0"/"0.00" instead of
        // NULL, while the no_covering fallback correctly renders NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dec_null (
                        ts TIMESTAMP,
                        sym SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two pre-ALTER rows: d8 and d16 will not yet exist for these.
            execute("""
                    INSERT INTO t_dec_null VALUES
                        ('2024-01-01T00:00:00', 'A'),
                        ('2024-01-01T01:00:00', 'A')
                    """);
            execute("ALTER TABLE t_dec_null ADD COLUMN d8 DECIMAL(2,1)");
            execute("ALTER TABLE t_dec_null ADD COLUMN d16 DECIMAL(4,2)");
            // One post-ALTER row with real values: d8 and d16 exist for it.
            execute("""
                    INSERT INTO t_dec_null VALUES
                        ('2024-01-01T02:00:00', 'A', 1.5::DECIMAL(2,1), 12.34::DECIMAL(4,2))
                    """);
            // Add the POSTING+covering index. Seal walks rowId 0,1
            // (rowId < colTop) for d8/d16 and writes the buggy zero
            // sentinel into the sidecar.
            execute("ALTER TABLE t_dec_null ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (d8, d16)");
            engine.releaseAllWriters();

            // Both covering and no_covering paths must produce identical
            // output. Capture the fallback first; if covering disagrees
            // (because NULL was sealed into the sidecar as zero), the
            // assertion fails.
            sink.clear();
            printSql("SELECT /*+ no_covering */ d8, d16 FROM t_dec_null WHERE sym = 'A' ORDER BY ts");
            String fallback = sink.toString();

            // Covering must agree with fallback for the same rows.
            assertQuery("SELECT d8, d16 FROM t_dec_null WHERE sym = 'A' ORDER BY ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns(fallback);
        });
    }

    @Test
    public void testCoveringQueryEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertQuery("SELECT price, qty FROM t_empty WHERE sym = 'A'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            """);
        });
    }

    @Test
    public void testCoveringQueryExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Query plan should show CoveringIndex when all selected columns are covered
            assertQuery("SELECT price FROM t_plan WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """);
        });
    }

    @Test
    public void testCoveringQueryFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_float (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (f, extra),
                        f FLOAT,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_float VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 1),
                    ('2024-01-01T01:00:00', 'B', 2.5, 2),
                    ('2024-01-01T02:00:00', 'A', 3.5, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT f, extra FROM t_float WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            f\textra
                            1.5\t1
                            3.5\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            // IN list uses CoveringIndex -- rows in ascending timestamp order.
            assertQuery("SELECT price FROM t_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym IN ['A','B']
                            """)
                    .returns("""
                            price
                            10.5
                            20.5
                            11.5
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedAllUnresolvedPageFrame() throws Exception {
        // All IN-list keys are unresolved; the factory wires the page-frame
        // cursor with an empty multiKeys list, and nextImpl() must hit
        // its multiKeys.size() == 0 early return rather than touching the
        // partition iterator. count() goes through the page-frame path
        // via CountRecordCursorFactory, so a zero count proves the early
        // return fires cleanly.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_unres_pf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_unres_pf VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM t_in_unres_pf WHERE sym IN ('XX', 'YY')")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedSampleByEquivalent() throws Exception {
        // Regression: multi-key heap-merge must yield the same SAMPLE BY
        // result as the FilterOnValues control. Before the fix the
        // CoveringIndex multi-key cursor advertised SCAN_DIRECTION_FORWARD
        // while emitting (partition, key, row-id) order, so SAMPLE BY's
        // monotonic-ts assumption mis-bucketed rows whose row-ids
        // interleaved in time.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_sb (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_sb VALUES
                    ('2024-01-01T00:30:00', 'A', 1.0),
                    ('2024-01-01T01:30:00', 'B', 2.0),
                    ('2024-01-01T02:30:00', 'A', 3.0),
                    ('2024-01-01T03:30:00', 'B', 4.0),
                    ('2024-01-01T04:30:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertSqlCursors(
                    "SELECT ts, sum(price) AS s FROM t_in_sb WHERE sym IN ('A', 'B') "
                            + "SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    "SELECT ts, sum(price) AS s FROM t_in_sb WHERE /*+ no_covering */ sym IN ('A', 'B') "
                            + "SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedSampleByMultiPartition() throws Exception {
        // Stresses the page-frame multi-partition resume path: SAMPLE BY
        // consumes via supportsPageFrameCursor() == true, so the heap-merge
        // page-frame variant must also produce ts-ascending output and
        // match the FilterOnValues control. Multiple partitions exercise
        // the partition advance + heap re-population logic.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_sb_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_sb_mp
                    SELECT
                        dateadd('m', (x * 30)::INT, '2024-01-01T00:00:00.000000Z')::TIMESTAMP,
                        rnd_symbol('A', 'B', 'C', 'D'),
                        x::DOUBLE
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            assertSqlCursors(
                    "SELECT ts, count() AS c, sum(price) AS s FROM t_in_sb_mp "
                            + "WHERE sym IN ('A', 'B') "
                            + "SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    "SELECT ts, count() AS c, sum(price) AS s FROM t_in_sb_mp "
                            + "WHERE /*+ no_covering */ sym IN ('A', 'B') "
                            + "SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedSkipsEmptyPartition() throws Exception {
        // A partition in the middle of the time range has no rows for any
        // IN-list key. The multi-key cursor must drop its empty heap,
        // advance to the next partition, and resume emit there. This
        // exercises the openPartitionCursors-returned-false branch of
        // advancePartition (cursor path) and the matching branch of
        // nextImpl (page-frame path).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_skip (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_skip VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'C', 99.0),
                    ('2024-01-02T01:00:00', 'D', 88.0),
                    ('2024-01-03T00:00:00', 'A', 3.0),
                    ('2024-01-03T01:00:00', 'B', 4.0)
                    """);
            engine.releaseAllWriters();

            // Cursor path: ts-ascending across partitions 1 and 3, with
            // partition 2 silently skipped.
            assertQuery("SELECT ts, sym, price FROM t_in_skip WHERE sym IN ('A', 'B') ORDER BY ts")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            2024-01-03T00:00:00.000000Z\tA\t3.0
                            2024-01-03T01:00:00.000000Z\tB\t4.0
                            """);

            // Page-frame path via vectorized count + sum: correct totals
            // prove the middle partition is skipped without producing a
            // bogus frame.
            assertQuery("SELECT count() c, sum(price) s FROM t_in_skip WHERE sym IN ('A', 'B')")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("c\ts\n4\t10.0\n");
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedTsAscending() throws Exception {
        // Multi-key IN where the keys' row-ids interleave in time. Before
        // the fix the CoveringIndex multi-key cursor concatenated per-key
        // blocks while still advertising SCAN_DIRECTION_FORWARD, so the
        // optimizer elided ORDER BY ts and the output came back grouped
        // by key. After the heap-merge fix the stream is ts-ascending and
        // matches the FilterOnValues control without an explicit sort.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_asc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_asc VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'A', 3.0),
                    ('2024-01-01T03:00:00', 'B', 4.0),
                    ('2024-01-01T04:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT ts, sym, price FROM t_in_asc WHERE sym IN ('A', 'B') ORDER BY ts")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0
                            2024-01-01T03:00:00.000000Z\tB\t4.0
                            2024-01-01T04:00:00.000000Z\tA\t5.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListInterleavedWithUnresolvedKey() throws Exception {
        // An unresolved literal in the IN list keeps the multi-key code
        // path but contributes no rows. Output must still be ts-ascending
        // across the resolved keys.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_unres (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_unres VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT ts, sym, price FROM t_in_unres WHERE sym IN ('A', 'B', 'XQCE')")
                    .noLeakCheck()
                    .assertsPlan("""
                            CoveringIndex on: sym with: ts, price
                              filter: sym IN ['A','B','XQCE']
                            """);
            assertQuery("SELECT ts, sym, price FROM t_in_unres WHERE sym IN ('A', 'B', 'XQCE')")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T01:00:00', 'C', 4.0),
                    ('2024-01-03T00:00:00', 'B', 5.0),
                    ('2024-01-03T01:00:00', 'A', 6.0)
                    """);
            engine.releaseAllWriters();

            // IN list across 3 partitions -- rows in ascending timestamp order.
            assertQuery("SELECT price FROM t_in_mp WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            5.0
                            6.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // One existing key, one non-existent — only existing key returns
            assertQuery("SELECT price FROM t_in_ne WHERE sym IN ('A', 'NONEXISTENT')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            """);

            // All non-existent keys — empty result
            assertQuery("SELECT price FROM t_in_ne WHERE sym IN ('X', 'Y')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_single (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_single VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Single-value IN is equivalent to = query
            assertQuery("SELECT price FROM t_in_single WHERE sym IN ('A')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            """);
        });
    }

    @Test
    public void testCoveringQueryInListWithSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // IN list with sym in SELECT -- sym varies per row, in timestamp order.
            assertQuery("SELECT sym, price FROM t_in_sym WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice
                            A\t10.5
                            B\t20.5
                            A\t11.5
                            """);
        });
    }

    @Test
    public void testCoveringQueryKeyAbsentFromMiddlePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_absent (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_absent VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-02T00:00:00', 'B', 2.5, 20),
                    ('2024-01-03T00:00:00', 'A', 3.5, 30)
                    """);
            engine.releaseAllWriters();

            // Key A is absent from partition 2024-01-02
            assertQuery("SELECT price, qty FROM t_absent WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            """);
        });
    }

    @Test
    public void testCoveringQueryLong256Column() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_l256 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (hash, extra),
                        hash LONG256,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_l256 VALUES
                    ('2024-01-01T00:00:00', 'A', '0x0100000000000000020000000000000003000000000000000400000000000000', 1),
                    ('2024-01-01T01:00:00', 'B', '0x0500000000000000060000000000000007000000000000000800000000000000', 2),
                    ('2024-01-01T02:00:00', 'A', '0x090000000000000000000000000000000b000000000000000c00000000000000', 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT hash, extra FROM t_l256 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            hash\textra
                            0x0100000000000000020000000000000003000000000000000400000000000000\t1
                            0x090000000000000000000000000000000b000000000000000c00000000000000\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_long (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (big_val, extra),
                        big_val LONG,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_long VALUES
                    ('2024-01-01T00:00:00', 'X', 100_000_000, 1),
                    ('2024-01-01T01:00:00', 'Y', 200_000_000, 2),
                    ('2024-01-01T02:00:00', 'X', 300_000_000, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT big_val, extra FROM t_long WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            big_val\textra
                            100000000\t1
                            300000000\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryManyRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_many (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val, extra),
                        val DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            StringBuilder sb = new StringBuilder("INSERT INTO t_many VALUES ");
            int totalRows = 300;
            for (int i = 0; i < totalRows; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                String sym = switch (i % 3) {
                    case 0 -> "A";
                    case 1 -> "B";
                    default -> "C";
                };
                // All rows on same day, each second apart
                sb.append(String.format(
                        "('2024-01-01T%02d:%02d:%02d', '%s', %d.5, %d)",
                        0, (i % 3600) / 60, i % 60, sym, i, i * 10
                ));
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Key A has rows at i=0,3,6,...,297 → 100 rows
            // First 3 values for A: 0.5, 3.5, 6.5
            assertQuery("SELECT val, extra FROM t_many WHERE sym = 'A' LIMIT 3")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\textra
                            0.5\t0
                            3.5\t30
                            6.5\t60
                            """);
        });
    }

    @Test
    public void testCoveringQueryMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i, l, s),
                        d DOUBLE,
                        i INT,
                        l LONG,
                        s SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_mixed VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10, 100000, 1),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20, 200000, 2),
                    ('2024-01-01T02:00:00', 'A', 3.5, 30, 300000, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT d, i, l, s FROM t_mixed WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            d\ti\tl\ts
                            1.5\t10\t100000\t1
                            3.5\t30\t300000\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryMultipleKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_keys (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_keys VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150),
                    ('2024-01-01T03:00:00', 'C', 30.5, 300),
                    ('2024-01-01T04:00:00', 'B', 21.5, 250),
                    ('2024-01-01T05:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_keys WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            11.5\t150
                            12.5\t120
                            """);

            assertQuery("SELECT price, qty FROM t_keys WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            20.5\t200
                            21.5\t250
                            """);

            assertQuery("SELECT price, qty FROM t_keys WHERE sym = 'C'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            30.5\t300
                            """);
        });
    }

    @Test
    public void testCoveringQueryMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_parts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Data across 3 partitions: Jan 1, 2, 3
            execute("""
                    INSERT INTO t_parts VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T12:00:00', 'B', 2.5, 20),
                    ('2024-01-02T00:00:00', 'A', 3.5, 30),
                    ('2024-01-02T12:00:00', 'C', 4.5, 40),
                    ('2024-01-03T00:00:00', 'A', 5.5, 50),
                    ('2024-01-03T12:00:00', 'B', 6.5, 60)
                    """);
            engine.releaseAllWriters();

            // Key A has data in all 3 partitions
            assertQuery("SELECT price, qty FROM t_parts WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            1.5\t10
                            3.5\t30
                            5.5\t50
                            """);

            // Key B has data in partitions 1 and 3
            assertQuery("SELECT price, qty FROM t_parts WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            2.5\t20
                            6.5\t60
                            """);
        });
    }

    @Test
    public void testCoveringQueryNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_nokey (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_nokey VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            // Non-existent symbol returns empty result
            assertQuery("SELECT price FROM t_nokey WHERE sym = 'Z'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            """);
        });
    }

    @Test
    public void testCoveringQueryNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'A', NULL, NULL),
                    ('2024-01-01T02:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            // NULL sentinel values in covered columns are stored in the sidecar files.
            // The CoveringRecord returns NaN/MIN_VALUE, displayed as empty/null.
            assertQuery("SELECT price, qty FROM t_nulls WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            null\tnull
                            12.5\t120
                            """);
        });
    }

    @Test
    public void testCoveringQueryO3() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Insert day 2 first, then day 1 (out of order)
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-02T00:00:00', 'A', 20.5, 200),
                    ('2024-01-02T12:00:00', 'B', 21.5, 210)
                    """);
            drainWalQueue();
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T12:00:00', 'B', 11.5, 110)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // O3 partitions have their covering sidecars rebuilt after the merge
            assertQuery("SELECT price, qty FROM t_o3 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            20.5\t200
                            """);
        });
    }

    @Test
    public void testCoveringQueryReversedColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rev (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rev VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Reversed column order vs table definition
            assertQuery("SELECT qty, price FROM t_rev WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            qty\tprice
                            100\t10.5
                            150\t11.5
                            """);
        });
    }

    @Test
    public void testCoveringQuerySelectOnlySymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_symonly (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_symonly VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Selecting only the indexed symbol column
            assertQuery("SELECT sym FROM t_symonly WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            A
                            """);
        });
    }

    @Test
    public void testCoveringQueryShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_short (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (small_val, extra),
                        small_val SHORT,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_short VALUES
                    ('2024-01-01T00:00:00', 'X', 100, 1),
                    ('2024-01-01T01:00:00', 'Y', 200, 2),
                    ('2024-01-01T02:00:00', 'X', 300, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT small_val, extra FROM t_short WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            small_val\textra
                            100\t1
                            300\t3
                            """);
        });
    }

    @Test
    public void testCoveringQuerySingleRowPerKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_single (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_single VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'C', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_single WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            """);

            assertQuery("SELECT price, qty FROM t_single WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            20.5\t200
                            """);

            assertQuery("SELECT price, qty FROM t_single WHERE sym = 'C'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            30.5\t300
                            """);
        });
    }

    @Test
    public void testCoveringQueryString() throws Exception {
        // STRING column in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'Y', 'world'),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_cover_string WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            label
                            hello
                            foo
                            """);
        });
    }

    @Test
    public void testCoveringQueryStringMultiCommit() throws Exception {
        // Multi-commit (multi-gen) with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string_mc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T00:00:00', 'X', 'hello')");
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T01:00:00', 'X', 'world')");
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T02:00:00', 'Y', 'foo')");
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_cover_string_mc WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            label
                            hello
                            world
                            """);
        });
    }

    @Test
    public void testCoveringQueryStringNull() throws Exception {
        // STRING column in INCLUDE with NULL values
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_string_null VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'X', NULL),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_cover_string_null WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            label
                            hello
                            \n\
                            foo
                            """);
        });
    }

    @Test
    public void testCoveringQuerySymbolInSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_sel (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_sel VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // Selecting sym + covered column should return correct sym values
            assertQuery("SELECT sym, price FROM t_sym_sel WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice
                            A\t10.5
                            A\t11.5
                            """);
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot', 10.5),
                    ('2024-01-01T01:00:00', 'B', 'cold', 20.5),
                    ('2024-01-01T02:00:00', 'A', 'warm', 11.5)
                    """);
            engine.releaseAllWriters();

            // tag is a SYMBOL column in INCLUDE — covering index should resolve it
            assertQuery("SELECT tag, price FROM t_sym_incl WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            tag\tprice
                            hot\t10.5
                            warm\t11.5
                            """);

            assertQuery("SELECT tag, price FROM t_sym_incl WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            tag\tprice
                            cold\t20.5
                            """);
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot', 1.0),
                    ('2024-01-01T01:00:00', 'B', 'cold', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'warm', 3.0),
                    ('2024-01-02T01:00:00', 'B', 'cool', 4.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT sym, tag, price FROM t_sym_incl_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\ttag\tprice
                            A\thot\t1.0
                            A\twarm\t3.0
                            """);
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag),
                        tag SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_null VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot'),
                    ('2024-01-01T01:00:00', 'A', NULL),
                    ('2024-01-01T02:00:00', 'A', 'cold')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT tag FROM t_sym_incl_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            tag
                            hot
                            \n\
                            cold
                            """);
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeWithInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag),
                        tag SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_in VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot'),
                    ('2024-01-01T01:00:00', 'B', 'cold'),
                    ('2024-01-01T02:00:00', 'A', 'warm'),
                    ('2024-01-01T03:00:00', 'C', 'cool')
                    """);
            engine.releaseAllWriters();

            // SYMBOL include with IN list -- rows in ascending timestamp order.
            assertQuery("SELECT sym, tag FROM t_sym_incl_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\ttag
                            A\thot
                            B\tcold
                            A\twarm
                            """);
        });
    }

    @Test
    public void testCoveringQueryTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (event_time, extra),
                        event_time TIMESTAMP,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ts VALUES
                    ('2024-01-01T00:00:00', 'A', '2024-06-15T12:30:00.000000Z', 1),
                    ('2024-01-01T01:00:00', 'B', '2024-07-20T08:00:00.000000Z', 2),
                    ('2024-01-01T02:00:00', 'A', '2024-08-25T16:45:00.000000Z', 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT event_time, extra FROM t_ts WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            event_time\textra
                            2024-06-15T12:30:00.000000Z\t1
                            2024-08-25T16:45:00.000000Z\t3
                            """);
        });
    }

    @Test
    public void testCoveringQueryUuidColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_uuid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, extra),
                        id UUID,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_uuid VALUES
                    ('2024-01-01T00:00:00', 'A', '11111111-1111-1111-1111-111111111111', 1),
                    ('2024-01-01T01:00:00', 'B', '22222222-2222-2222-2222-222222222222', 2),
                    ('2024-01-01T02:00:00', 'A', '33333333-3333-3333-3333-333333333333', 3),
                    ('2024-01-01T03:00:00', 'A', NULL, 4)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT id, extra FROM t_uuid WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            id\textra
                            11111111-1111-1111-1111-111111111111\t1
                            33333333-3333-3333-3333-333333333333\t3
                            \t4
                            """);
        });
    }

    @Test
    public void testCoveringQueryVarchar() throws Exception {
        // VARCHAR column in INCLUDE — end-to-end via FallbackRecord (column reads)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0),
                    ('2024-01-01T03:00:00', 'A', null, 40.0)
                    """);
            engine.releaseAllWriters();

            // Query with covered varchar + double columns
            assertQuery("SELECT name, price FROM t_cover_varchar WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            alice\t10.0
                            anna\t30.0
                            \t40.0
                            """);

            assertQuery("SELECT name, price FROM t_cover_varchar WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            bob\t20.0
                            """);

            // Non-covered column forces fallback — verify no crash
            assertQuery("SELECT ts, name FROM t_cover_varchar WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tname
                            2024-01-01T00:00:00.000000Z\talice
                            2024-01-01T02:00:00.000000Z\tanna
                            2024-01-01T03:00:00.000000Z\t
                            """);
        });
    }

    @Test
    public void testCoveringQueryVarcharEmpty() throws Exception {
        // Empty string (zero-length VARCHAR) in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'A', '', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_cover_varchar_empty WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            alice\t10.0
                            \t20.0
                            anna\t30.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryVarcharMultiCommit() throws Exception {
        // Multi-commit (multi-gen) with var-sized covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_mc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T00:00:00', 'A', 'alice', 10.0)");
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T01:00:00', 'A', 'anna', 20.0)");
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T02:00:00', 'B', 'bob', 30.0)");
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_cover_varchar_mc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            alice\t10.0
                            anna\t20.0
                            """);

            assertQuery("SELECT name, price FROM t_cover_varchar_mc WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            bob\t30.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryVarcharMultiPartition() throws Exception {
        // VARCHAR covering across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'anna', 30.0),
                    ('2024-01-03T00:00:00', 'A', 'amy', 50.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_cover_varchar_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            alice\t10.0
                            anna\t30.0
                            amy\t50.0
                            """);
        });
    }

    @Test
    public void testCoveringQueryWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_wal WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            11.5\t150
                            """);
        });
    }

    @Test
    public void testCoveringRecord_AllColumnTypes() throws Exception {
        // Exercise every column type the covering writer supports
        // (writeCoveredRow's switch enumerates them). assertQueryNoLeakCheck
        // drives the full RecordCursorFactory/RecordCursor API:
        //   - CursorPrinter calls every type's primary accessor
        //   - testStringsLong256AndBinary covers STRING getStrA+B+Len,
        //     BINARY getBin+Len, LONG256 getLong256A+B
        //   - assertVariableColumns covers VARCHAR getVarcharA+B+Size
        //   - testSymbolAPI covers SYMBOL getInt+getSymA+symbol table API
        //     including the indexed sym column whose getInt must return the
        //     resolved key.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_all_types (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            b, sh, i, l, ts_inc, dt, f, d, c, bo, ip,
                            uuid_col, l256,
                            d8, d16, d32, d64, d128, d256,
                            gb, gs, gi, gl,
                            label, name, payload, kind
                        ),
                        b BYTE,
                        sh SHORT,
                        i INT,
                        l LONG,
                        ts_inc TIMESTAMP,
                        dt DATE,
                        f FLOAT,
                        d DOUBLE,
                        c CHAR,
                        bo BOOLEAN,
                        ip IPv4,
                        uuid_col UUID,
                        l256 LONG256,
                        d8 DECIMAL(2, 1),
                        d16 DECIMAL(4, 2),
                        d32 DECIMAL(9, 2),
                        d64 DECIMAL(18, 4),
                        d128 DECIMAL(38, 10),
                        d256 DECIMAL(40, 5),
                        gb GEOHASH(5b),
                        gs GEOHASH(10b),
                        gi GEOHASH(20b),
                        gl GEOHASH(40b),
                        label STRING,
                        name VARCHAR,
                        payload BINARY,
                        kind SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two rows: row 1 is fully populated, row 2 is all-null. Each
            // accessor is exercised in both branches of its null-handling
            // logic.
            execute("""
                    INSERT INTO t_all_types VALUES
                    (
                        '2024-01-01T00:00:00', 'A',
                        7, 1234, 99, 1000000,
                        '2024-06-15T10:30:00', '2024-06-15',
                        1.5, 2.25, 'X', true,
                        '10.0.0.1',
                        '00000000-0000-0000-0000-000000000001',
                        0x0a,
                        '1.5'::DECIMAL(2, 1),
                        '12.34'::DECIMAL(4, 2),
                        '1234567.89'::DECIMAL(9, 2),
                        '12345678901234.5678'::DECIMAL(18, 4),
                        '1234567890123456789012345678.1234567890'::DECIMAL(38, 10),
                        '12345678901234567890123456789012345.67890'::DECIMAL(40, 5),
                        #y, #yz, #yzbc, #yzbc1234,
                        'alice', 'va', null, 'k1'
                    ),
                    (
                        '2024-01-01T01:00:00', 'A',
                        null, null, null, null,
                        null, null,
                        null, null, null, null,
                        null,
                        null,
                        null,
                        null, null, null, null, null, null,
                        null, null, null, null,
                        null, null, null, null
                    )
                    """);
            engine.releaseAllWriters();

            // Selecting only the indexed sym column plus all INCLUDE'd
            // columns keeps the optimizer on the CoveringIndex factory.
            // Including ts (uncovered) here would fall back to a regular scan.
            String allTypesSelect = "SELECT sym, b, sh, i, l, ts_inc, dt, f, d, c, bo, ip, uuid_col, l256, d8, d16, d32, d64, d128, d256, gb, gs, gi, gl, label, name, payload, kind FROM t_all_types WHERE sym = 'A'";
            assertQuery(allTypesSelect)
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tb\tsh\ti\tl\tts_inc\tdt\tf\td\tc\tbo\tip\tuuid_col\tl256\td8\td16\td32\td64\td128\td256\tgb\tgs\tgi\tgl\tlabel\tname\tpayload\tkind
                            A\t7\t1234\t99\t1000000\t2024-06-15T10:30:00.000000Z\t2024-06-15T00:00:00.000Z\t1.5\t2.25\tX\ttrue\t10.0.0.1\t00000000-0000-0000-0000-000000000001\t0x0a\t1.5\t12.34\t1234567.89\t12345678901234.5678\t1234567890123456789012345678.1234567890\t12345678901234567890123456789012345.67890\ty\tyz\tyzbc\tyzbc1234\talice\tva\t\tk1
                            A\t0\t0\tnull\tnull\t\t\tnull\tnull\t\tfalse\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            """);

            // Manual pass for accessors not exercised by assertQueryNoLeakCheck:
            //   - SYMBOL getSymB on the indexed col + INCLUDE'd col
            //   - getRowId() (Record metadata, not column data)
            try (var factory = select(allTypesSelect);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record r = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals("A", r.getSymA(0).toString());
                assertEquals("A", r.getSymB(0).toString());
                // kind is the last column (index 27)
                assertEquals("k1", r.getSymA(27).toString());
                assertEquals("k1", r.getSymB(27).toString());
                // getRowId returns the file row id stored on the record
                assertTrue(r.getRowId() >= 0);
            }

            // Page-frame cursor pass: exercises writeCoveredRow's full type
            // switch including STRING/VARCHAR/BINARY/LONG256/DECIMAL128+256,
            // plus writeStringToFrame and writeBinaryToFrame paths that the
            // record cursor does not visit.
            try (var factory = select(allTypesSelect);
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                long rows = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    rows += f.getPartitionHi() - f.getPartitionLo();
                }
                assertEquals(2, rows);
            }
        });
    }

    @Test
    public void testCoveringReloadDoesNotRefreshStaleSidecarMmap() throws Exception {
        // Regression test for: long-lived posting-index readers must refresh
        // covering sidecar mmaps when reloadConditionally() picks up a new
        // chain entry produced by a gen flush that extended .pcN past the
        // reader's previous extent.
        //
        // The default 1 MiB append-page pre-allocation would hide the bug,
        // because both gens would land within one writer-side chunk and the
        // reader's initial mmap (sized to the file's pre-allocated length)
        // would happen to cover the new gen's bytes too. The OS-page override
        // forces the writer to extend the file between gens so the bug
        // manifests reliably.
        //
        // The fix publishes each .pcN's authoritative valid byte extent in
        // the chain entry (one long per cover column, in the cover end-offset
        // footer). reloadConditionally() resizes the sidecar mapping to the
        // newly-published extent, so reads in the new gen stay in bounds and
        // return the writer's actual values.
        final long osPage = io.questdb.std.Files.PAGE_SIZE;
        CairoConfiguration smallPageCfg = new CairoConfigurationWrapper(configuration) {
            @Override
            public long getDataIndexValueAppendPageSize() {
                return osPage;
            }
        };
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "stale_sidecar_red";
                int plen = path.size();
                // Enough rows that gen0 + gen1 sidecar bytes overflow one OS
                // page (header 1144 + per-gen raw blocks of 4 + V*8 each).
                int rowsPerGen = (int) (osPage / 8);
                int totalRows = rowsPerGen * 2;
                long colAddr = Unsafe.malloc((long) totalRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(smallPageCfg, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},               // shift for LONG (8 bytes = 2^3)
                                new int[]{1},               // covered column writer index
                                new int[]{ColumnType.LONG},
                                1
                        );

                        // Gen 0: first half of rows for key 0.
                        for (int i = 0; i < rowsPerGen; i++) writer.add(0, i);
                        writer.setMaxValue(rowsPerGen - 1);
                        writer.commit();

                        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                smallPageCfg, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                                coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG}), EMPTY_CVR, 0)) {
                            // First covering read populates sidecarMems[0] via
                            // ensureSidecarOpen(), mmaping to the chain-published
                            // gen-0 extent.
                            CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(
                                    0, 0, Long.MAX_VALUE, new int[]{0});
                            assertTrue(cc.isCoveredAvailable(0));
                            for (int i = 0; i < rowsPerGen; i++) {
                                assertTrue("gen0 row " + i, cc.hasNext());
                                assertEquals(i, cc.next());
                                assertEquals("gen0 covered value at row " + i,
                                        1000L + i, cc.getCoveredLong(0));
                            }
                            assertFalse(cc.hasNext());
                            Misc.free(cc);

                            long mmapSizeAfterGen0 = readSidecarMmapSize(reader, 0);
                            assertTrue("sanity: sidecar mmap was populated by first covering read",
                                    mmapSizeAfterGen0 > 0);

                            // Gen 1: second half of rows for key 0. Same writer,
                            // no seal between commits, so sealTxn stays at 0 and
                            // writeSidecarGenData() appends another block to the
                            // same .pc0 file. With the small append page size this
                            // forces the writer to allocate a fresh chunk past the
                            // gen-0 extent.
                            for (int i = rowsPerGen; i < totalRows; i++) writer.add(0, i);
                            writer.setMaxValue(totalRows - 1);
                            writer.commit();

                            long fileSizeAfterGen1 = sidecarFileLengthOnDisk(path.trimTo(plen), name, 0);
                            assertTrue(
                                    "sanity: writer must extend .pc0 past the gen-0 mmap" +
                                            " [gen0Mmap=" + mmapSizeAfterGen0 +
                                            ", fileLenGen1=" + fileSizeAfterGen1 + "]",
                                    fileSizeAfterGen1 > mmapSizeAfterGen0
                            );

                            // Trigger reloadConditionally() through getCursor and
                            // iterate every row across both gens. Without the
                            // chain-published cover end-offset, this would
                            // dereference past the stale gen-0 mapping (SIGBUS) or
                            // return zero-padded bytes. With the fix the mapping is
                            // resized to the new published extent and every value
                            // round-trips.
                            cc = (CoveringRowCursor) reader.getCursor(
                                    0, 0, Long.MAX_VALUE, new int[]{0});
                            assertTrue(cc.isCoveredAvailable(0));
                            for (int i = 0; i < totalRows; i++) {
                                assertTrue("post-reload row " + i, cc.hasNext());
                                assertEquals(i, cc.next());
                                assertEquals("post-reload covered value at row " + i,
                                        1000L + i, cc.getCoveredLong(0));
                            }
                            assertFalse(cc.hasNext());
                            Misc.free(cc);

                            long mmapSizeAfterReload = readSidecarMmapSize(reader, 0);
                            assertTrue(
                                    "reload must extend the sidecar mmap past the gen-0" +
                                            " extent [gen0Mmap=" + mmapSizeAfterGen0 +
                                            ", afterReload=" + mmapSizeAfterReload + "]",
                                    mmapSizeAfterReload > mmapSizeAfterGen0
                            );
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, (long) totalRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringReloadRefreshesAllSidecarMmapsMultiColumn() throws Exception {
        // Same scenario as the single-column reload test, but with three cover
        // columns of different fixed widths (Long / Int / Double). Exercises
        // includeIdx = 0, 1, 2 of sidecarMems / sidecarFileEndOffsets so the
        // chain entry's per-cover-column footer is verified slot by slot.
        //
        // Each cover column gets its own column buffer; gen 1 forces every
        // .pcN file past gen 0's mmap, and the post-reload reads must
        // round-trip every value through every slot.
        final long osPage = io.questdb.std.Files.PAGE_SIZE;
        CairoConfiguration smallPageCfg = new CairoConfigurationWrapper(configuration) {
            @Override
            public long getDataIndexValueAppendPageSize() {
                return osPage;
            }
        };
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "stale_sidecar_red_multi";
                int plen = path.size();
                int rowsPerGen = (int) (osPage / 8);
                int totalRows = rowsPerGen * 2;

                // Cover column 0: LONG (8B), values 1000 + i.
                // Cover column 1: INT  (4B), values 2_000_000 + i.
                // Cover column 2: DOUBLE (8B), values 3.5 * (i + 1).
                long longAddr = Unsafe.malloc((long) totalRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                long intAddr = Unsafe.malloc((long) totalRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                long doubleAddr = Unsafe.malloc((long) totalRows * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < totalRows; i++) {
                        Unsafe.putLong(longAddr + (long) i * Long.BYTES, 1000L + i);
                        Unsafe.putInt(intAddr + (long) i * Integer.BYTES, 2_000_000 + i);
                        Unsafe.putDouble(doubleAddr + (long) i * Double.BYTES, 3.5d * (i + 1));
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(smallPageCfg, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{longAddr, intAddr, doubleAddr},
                                new long[]{0, 0, 0},
                                new int[]{3, 2, 3},                              // shifts: LONG=8(2^3), INT=4(2^2), DOUBLE=8(2^3)
                                new int[]{1, 2, 3},                              // covered column writer indices
                                new int[]{ColumnType.LONG, ColumnType.INT, ColumnType.DOUBLE},
                                3
                        );

                        // Gen 0: first half of rows for key 0.
                        for (int i = 0; i < rowsPerGen; i++) writer.add(0, i);
                        writer.setMaxValue(rowsPerGen - 1);
                        writer.commit();

                        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                smallPageCfg, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                                coveringMetadata(
                                        new int[]{1, 2, 3},
                                        new int[]{ColumnType.LONG, ColumnType.INT, ColumnType.DOUBLE}
                                ),
                                EMPTY_CVR, 0
                        )) {
                            // First read populates sidecarMems[0..2] via
                            // ensureSidecarOpen(), each mmaped to the
                            // chain-published gen-0 extent for its slot.
                            CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(
                                    0, 0, Long.MAX_VALUE, new int[]{0, 1, 2});
                            assertTrue("slot 0 covered after gen0", cc.isCoveredAvailable(0));
                            assertTrue("slot 1 covered after gen0", cc.isCoveredAvailable(1));
                            assertTrue("slot 2 covered after gen0", cc.isCoveredAvailable(2));
                            for (int i = 0; i < rowsPerGen; i++) {
                                assertTrue("gen0 row " + i, cc.hasNext());
                                assertEquals(i, cc.next());
                                assertEquals("gen0 LONG @row " + i, 1000L + i, cc.getCoveredLong(0));
                                assertEquals("gen0 INT @row " + i, 2_000_000 + i, cc.getCoveredInt(1));
                                assertEquals("gen0 DOUBLE @row " + i, 3.5d * (i + 1), cc.getCoveredDouble(2), 1e-9);
                            }
                            assertFalse(cc.hasNext());
                            Misc.free(cc);

                            long[] gen0Sizes = new long[3];
                            for (int slot = 0; slot < 3; slot++) {
                                gen0Sizes[slot] = readSidecarMmapSize(reader, slot);
                                assertTrue(
                                        "sanity: slot " + slot + " mmap populated by first read",
                                        gen0Sizes[slot] > 0
                                );
                            }

                            // Gen 1: second half. Same writer, same sealTxn —
                            // every .pcN extends past gen-0's mmap.
                            for (int i = rowsPerGen; i < totalRows; i++) writer.add(0, i);
                            writer.setMaxValue(totalRows - 1);
                            writer.commit();

                            for (int slot = 0; slot < 3; slot++) {
                                long fileLen = sidecarFileLengthOnDisk(path.trimTo(plen), name, slot);
                                assertTrue(
                                        "sanity: slot " + slot + " .pcN must extend past gen-0 mmap" +
                                                " [gen0Mmap=" + gen0Sizes[slot] +
                                                ", fileLenGen1=" + fileLen + "]",
                                        fileLen > gen0Sizes[slot]
                                );
                            }

                            // Reload and read everything — every covered value
                            // for every slot must round-trip.
                            cc = (CoveringRowCursor) reader.getCursor(
                                    0, 0, Long.MAX_VALUE, new int[]{0, 1, 2});
                            assertTrue(cc.isCoveredAvailable(0));
                            assertTrue(cc.isCoveredAvailable(1));
                            assertTrue(cc.isCoveredAvailable(2));
                            for (int i = 0; i < totalRows; i++) {
                                assertTrue("post-reload row " + i, cc.hasNext());
                                assertEquals(i, cc.next());
                                assertEquals("post-reload LONG @row " + i, 1000L + i, cc.getCoveredLong(0));
                                assertEquals("post-reload INT @row " + i, 2_000_000 + i, cc.getCoveredInt(1));
                                assertEquals("post-reload DOUBLE @row " + i, 3.5d * (i + 1), cc.getCoveredDouble(2), 1e-9);
                            }
                            assertFalse(cc.hasNext());
                            Misc.free(cc);

                            for (int slot = 0; slot < 3; slot++) {
                                long postReload = readSidecarMmapSize(reader, slot);
                                assertTrue(
                                        "reload must extend slot " + slot + " mmap past the gen-0 extent" +
                                                " [gen0Mmap=" + gen0Sizes[slot] +
                                                ", afterReload=" + postReload + "]",
                                        postReload > gen0Sizes[slot]
                                );
                            }
                        }
                    }
                } finally {
                    Unsafe.free(longAddr, (long) totalRows * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(intAddr, (long) totalRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(doubleAddr, (long) totalRows * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringStringFsstCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_string
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('X','Y','Z'),
                           rnd_str('label_alpha','label_beta','label_gamma','label_delta')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_fsst_string WHERE sym = 'X'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: label
                                  filter: sym='X'
                            """);

            assertQuery("SELECT COUNT(*) FROM t_fsst_string WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n498\n");
        });
    }

    @Test
    public void testCoveringTimestampThroughSealDeltaCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_cover (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (event_ts, extra),
                        event_ts TIMESTAMP,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ts_cover VALUES
                    ('2024-01-01T01:00:00', 'A', '2024-06-15T12:00:01.000000', 1),
                    ('2024-01-01T02:00:00', 'B', '2024-06-15T12:00:02.000000', 2),
                    ('2024-01-01T03:00:00', 'A', '2024-06-15T12:00:03.000000', 3),
                    ('2024-01-01T04:00:00', 'B', '2024-06-15T12:00:04.000000', 4),
                    ('2024-01-01T05:00:00', 'A', '2024-06-15T12:00:05.000000', 5),
                    ('2024-01-01T06:00:00', 'A', NULL, 6)
                    """);
            engine.releaseAllWriters();
            // Second commit to exercise sealed + sparse gens
            execute("""
                    INSERT INTO t_ts_cover VALUES
                    ('2024-01-01T07:00:00', 'A', '2024-06-15T12:00:07.000000', 7),
                    ('2024-01-01T08:00:00', 'B', '2024-06-15T12:00:08.000000', 8)
                    """);
            engine.releaseAllWriters();

            // Verify covered TIMESTAMP values including NULLs
            assertQuery("SELECT event_ts, extra FROM t_ts_cover WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: event_ts, extra
                                  filter: sym='A'
                            """)
                    .returns("""
                            event_ts\textra
                            2024-06-15T12:00:01.000000Z\t1
                            2024-06-15T12:00:03.000000Z\t3
                            2024-06-15T12:00:05.000000Z\t5
                            \t6
                            2024-06-15T12:00:07.000000Z\t7
                            """);
        });
    }

    @Test
    public void testCoveringToTopReScan() throws Exception {
        // Gap 8: toTop() re-scan via CROSS JOIN
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_totop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_totop VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'A', 11.0)
                    """);
            engine.releaseAllWriters();

            // CROSS JOIN forces re-scan — each left row paired with all right rows
            assertQuery("""
                    SELECT t.price, x.x
                    FROM (SELECT price FROM t_totop WHERE sym = 'A') t
                    CROSS JOIN (SELECT x FROM long_sequence(2)) x
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tx
                            10.0\t1
                            10.0\t2
                            11.0\t1
                            11.0\t2
                            """);
        });
    }

    @Test
    public void testCoveringVarcharEmptyVsNullDistinction() throws Exception {
        // Regression test for #14: empty VARCHAR ('') must round-trip
        // through the covering sidecar as a present-but-empty value, NOT as
        // SQL NULL. The writer used to emit zero bytes for both NULL and
        // empty, leaving the reader unable to distinguish them.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vc_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        v VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vc_empty VALUES
                    ('2024-01-01T00:00:00', 'A', NULL),
                    ('2024-01-01T01:00:00', 'A', ''),
                    ('2024-01-01T02:00:00', 'A', 'data'),
                    ('2024-01-01T03:00:00', 'A', '')
                    """);
            engine.releaseAllWriters();

            // IS NULL must match exactly the literal NULL row, not the two
            // empty strings. Without the fix, all three were treated as
            // NULL and the count was 3.
            assertQuery("SELECT count() AS cnt FROM t_vc_empty WHERE sym = 'A' AND v IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            cnt
                            1
                            """);

            // And the empty filter must match exactly two rows.
            assertQuery("SELECT count() AS cnt FROM t_vc_empty WHERE sym = 'A' AND v = ''")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            cnt
                            2
                            """);
        });
    }

    @Test
    public void testCoveringVarcharFsstCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_varchar
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('K0','K1','K2'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta','order_epsilon')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            // Plan: CoveringIndex for varchar column
            assertQuery("SELECT name FROM t_fsst_varchar WHERE sym = 'K0'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym='K0'
                            """);

            // Data correctness: covering vs non-covering
            assertQuery("SELECT COUNT(*) FROM t_fsst_varchar WHERE sym = 'K0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n498\n");
        });
    }

    @Test
    public void testCoveringVarcharFsstEmptyStrings() throws Exception {
        // Edge case: empty strings (zero-length) mixed with regular strings
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_empty
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           CASE WHEN x % 3 = 0 THEN '' ELSE rnd_str('alpha_value','beta_value','gamma_value') END
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_fsst_empty WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n715\n");
        });
    }

    @Test
    public void testCoveringVarcharFsstInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_in
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name FROM t_fsst_in WHERE sym IN ('A', 'B')")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym IN ['A','B']
                            """);

            assertQuery("SELECT COUNT(*) FROM t_fsst_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n753\n");
        });
    }

    @Test
    public void testCoveringVarcharFsstMixedWithDouble() throws Exception {
        // VARCHAR + DOUBLE in INCLUDE — both columns covered, one FSST-compressed
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_mixed
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta'),
                           rnd_double() * 100
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_fsst_mixed WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name, price
                                  filter: sym='A'
                            """);

            assertQuery("SELECT COUNT(*) FROM t_fsst_mixed WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n528\n");
        });
    }

    @Test
    public void testCoveringVarcharFsstWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_null
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           CASE WHEN x % 5 = 0 THEN null ELSE rnd_str('value_one','value_two','value_three','value_four') END
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name FROM t_fsst_null WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym='A'
                            """);

            assertQuery("SELECT COUNT(*) FROM t_fsst_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n715\n");
        });
    }

    @Test
    public void testCoveringVarcharLargeInsert() throws Exception {
        // Reproducer for SIGSEGV in writeVarcharValue during intermediate commit.
        // MemoryPMARImpl maps one page at a time — addressOf() for a non-current page
        // returns 0, causing a crash. The fix uses ff.read() (pread) instead.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_large_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_large_varchar
                    SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_str('order_alpha_2024','order_beta_2024','order_gamma_2024','order_delta_2024')
                    FROM long_sequence(50000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_large_varchar WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n9976\n");
        });
    }

    @Test
    public void testCoveringVarcharLargeInsertFixedWidth() throws Exception {
        // Same MemoryPMARImpl bug but for fixed-width INCLUDE columns (DOUBLE).
        // writeSidecarValueSafe also used addressOf() — now uses ff.read().
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_large_double (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_large_double
                    SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_double() * 100
                    FROM long_sequence(50000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT COUNT(*) FROM t_large_double WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n9960\n");
        });
    }

    @Test
    public void testCoveringWithColumnTop() throws Exception {
        // Rows below columnTop get null sentinels in sidecar data
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_coltop";
                int plen = path.size();

                int rowCount = 10;
                long colTop = 4; // first 4 rows are below column top
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 50.0 + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{colTop},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));


                        // First 4 rows: below columnTop, null sentinel is NaN for DOUBLE
                        for (int i = 0; i < 4; i++) {
                            assertTrue(cc.hasNext());
                            assertEquals(i, cc.next());
                            assertTrue("row " + i + " should be NaN", Double.isNaN(cc.getCoveredDouble(0)));
                        }
                        // Rows 4-9: above columnTop, should get actual values
                        // Row 4 maps to colAddr offset (4-4)*8 = 0 -> value 50.0
                        for (int i = 4; i < rowCount; i++) {
                            assertTrue(cc.hasNext());
                            assertEquals(i, cc.next());
                            assertEquals(50.0 + (i - colTop), cc.getCoveredDouble(0), 0.001);
                        }
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringWithDesignatedTimestamp() throws Exception {
        // Selecting the designated timestamp column which is also in the INCLUDE list.
        // The covering index should serve this without a full table scan.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_ts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_ts VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 100.0),
                    ('2024-01-01T00:01:00', 'SILVER', 'LME', 200.0),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 300.0)
                    """);
            engine.releaseAllWriters();

            // ts + sym: both should be coverable
            assertQuery("SELECT ts, sym FROM t_cover_ts WHERE sym = 'GOLD'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """)
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000Z\tGOLD
                            2024-01-01T00:02:00.000000Z\tGOLD
                            """);
        });
    }

    @Test
    public void testCoveringWithDesignatedTimestampWal() throws Exception {
        // WAL table with designated timestamp in INCLUDE — same as user's production scenario
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_ts_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL
                    """);
            execute("""
                    INSERT INTO t_cover_ts_wal VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 100.0),
                    ('2024-01-01T00:01:00', 'SILVER', 'LME', 200.0),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 300.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT ts, sym FROM t_cover_ts_wal WHERE sym = 'GOLD'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """)
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000Z\tGOLD
                            2024-01-01T00:02:00.000000Z\tGOLD
                            """);
        });
    }

    @Test
    public void testCoveringWithTimestampNs() throws Exception {
        // Exact reproduction of user's DDL with TIMESTAMP_NS, SYMBOL, arrays, INCLUDE list
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_tsns (
                        ts TIMESTAMP_NS,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, best_bid, best_ask),
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_tsns VALUES
                    ('2024-01-01T00:00:00.000000000', 'GOLD', 'CME', 'metal', NULL, NULL, 2050.5, 2051.0),
                    ('2024-01-01T00:01:00.000000000', 'SILVER', 'LME', 'metal', NULL, NULL, 24.3, 24.5),
                    ('2024-01-01T00:02:00.000000000', 'GOLD', 'LME', 'metal', NULL, NULL, 2051.0, 2052.0)
                    """);
            engine.releaseAllWriters();

            // Covered columns only — should use CoveringIndex
            assertQuery("SELECT ts, sym, exchange FROM t_cover_tsns WHERE sym = 'GOLD'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\texchange
                            2024-01-01T00:00:00.000000000Z\tGOLD\tCME
                            2024-01-01T00:02:00.000000000Z\tGOLD\tLME
                            """);

            // ts + sym only — should use CoveringIndex
            assertQuery("SELECT ts, sym FROM t_cover_tsns WHERE sym = 'GOLD'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """)
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000000Z\tGOLD
                            2024-01-01T00:02:00.000000000Z\tGOLD
                            """);
        });
    }

    @Test
    public void testCoveringWithTimestampNsWal() throws Exception {
        // Reproduction of user's WAL table with TIMESTAMP_NS and INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_tsns_wal (
                        ts TIMESTAMP_NS,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, best_bid, best_ask),
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL
                    """);
            execute("""
                    INSERT INTO t_cover_tsns_wal VALUES
                    ('2024-01-01T00:00:00.000000000', 'GOLD', 'CME', 'metal', 2050.5, 2051.0),
                    ('2024-01-01T00:01:00.000000000', 'SILVER', 'LME', 'metal', 24.3, 24.5),
                    ('2024-01-01T00:02:00.000000000', 'GOLD', 'LME', 'metal', 2051.0, 2052.0)
                    """);
            drainWalQueue();

            // Plan should use CoveringIndex for covered columns on WAL table
            assertQuery("SELECT ts, sym FROM t_cover_tsns_wal WHERE sym = 'GOLD'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """)
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000000Z\tGOLD
                            2024-01-01T00:02:00.000000000Z\tGOLD
                            """);
        });
    }

    @Test
    public void testCreateTableIncludeDuplicatePosition() throws Exception {
        // The SqlException must point at the duplicated column name, not
        // position 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cdup (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price, price), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("price, price") + "price, ".length();
            assertQuery(sql)
                    .fails(expected, "duplicate column in INCLUDE");
        });
    }

    @Test
    public void testCreateTableIncludeDuplicateRejects() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_dup2 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (price, price),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("duplicate column in INCLUDE"));
            }
        });
    }

    @Test
    public void testCreateTableIncludeMissingColumnPosition() throws Exception {
        // The SqlException must point at the missing column name, not 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cmiss (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (ghost), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("ghost");
            assertQuery(sql)
                    .fails(expected, "INCLUDE column");
        });
    }

    @Test
    public void testCreateTableIncludeSelfReferencePosition() throws Exception {
        // The SqlException must point at the self-referencing column name in
        // the INCLUDE list, not 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cself (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (sym), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("INCLUDE (") + "INCLUDE (".length();
            assertQuery(sql)
                    .fails(expected, "INCLUDE must not contain the indexed column");
        });
    }

    @Test
    public void testCreateTableIncludeSelfReferenceRejects() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_selfref (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (sym),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testCreateTableWithIncludeSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("trades")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                // Verify covering column indices are stored in metadata
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
                assertEquals(metadata.getColumnIndex("price"), coveringCols.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), coveringCols.getQuick(1));
            }
        });
    }

    @Test
    public void testDistinctAfterDropAllPartitions() throws Exception {
        // Gap 12: DISTINCT when symbolCount>0 but 0 rows
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_dropall (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_dropall VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();
            execute("ALTER TABLE t_distinct_dropall DROP PARTITION LIST '2024-01-01', '2024-01-02'");
            engine.releaseAllWriters();

            // symbolCount=2 but no data anywhere — should return empty
            assertQuery("SELECT DISTINCT sym FROM t_distinct_dropall")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            """);
        });
    }

    @Test
    public void testDistinctHighCardinality() throws Exception {
        // Gap 13: DISTINCT with >256 keys (crosses stride boundary)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_hc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            StringBuilder sb = new StringBuilder("INSERT INTO t_distinct_hc VALUES ");
            for (int i = 0; i < 300; i++) {
                if (i > 0) sb.append(',');
                sb.append("('2024-01-01T").append(String.format("%02d", i / 60)).append(':')
                        .append(String.format("%02d", i % 60)).append(":00', 'K").append(i).append("', ").append(i).append(".0)");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Should return 300 distinct keys
            try (var factory = select("SELECT DISTINCT sym FROM t_distinct_hc");
                 var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                assertEquals(300, count);
            }
        });
    }

    @Test
    public void testDistinctMultiPartitionWithTimeFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 3 days, symbols S0-S4, only S0/S1 appear on day 2
            execute("""
                    INSERT INTO t_dist_mp VALUES
                    ('2024-01-01T01:00:00', 'S0', 1.0),
                    ('2024-01-01T02:00:00', 'S1', 2.0),
                    ('2024-01-01T03:00:00', 'S2', 3.0),
                    ('2024-01-02T01:00:00', 'S0', 4.0),
                    ('2024-01-02T02:00:00', 'S1', 5.0),
                    ('2024-01-03T01:00:00', 'S3', 6.0),
                    ('2024-01-03T02:00:00', 'S4', 7.0)
                    """);
            engine.releaseAllWriters();

            // DISTINCT with time filter — only symbols in day 2
            assertQuery("SELECT DISTINCT sym FROM t_dist_mp WHERE ts IN '2024-01-02' ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            S0
                            S1
                            """);
        });
    }

    @Test
    public void testDistinctNoWhereStillWorks() throws Exception {
        // Ensure the existing no-WHERE DISTINCT path still uses PostingIndex
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_nofilter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_nofilter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_dist_nofilter")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist_nofilter
                            """)
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    @Test
    public void testDistinctSymAfterDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_drop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_drop VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'C', 3.0)
                    """);
            engine.releaseAllWriters();

            // Drop partition containing B — B's symbol table key persists but has no rows
            execute("ALTER TABLE t_distinct_drop DROP PARTITION LIST '2024-01-01'");
            engine.releaseAllWriters();

            // B should NOT appear — only C has data
            assertQuery("SELECT DISTINCT sym FROM t_distinct_drop")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            C
                            """);
        });
    }

    @Test
    public void testDistinctSymBitmapFallback() throws Exception {
        assertMemoryLeak(() -> {
            // BITMAP index (not POSTING) should NOT use PostingIndex distinct
            execute("""
                    CREATE TABLE t_distinct_bmp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_bmp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // Should still return correct results via GROUP BY path
            assertQuery("SELECT DISTINCT sym FROM t_distinct_bmp ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            """);

            // Plan should NOT show PostingIndex distinct
            assertQuery("SELECT DISTINCT sym FROM t_distinct_bmp")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t_distinct_bmp
                            """);
        });
    }

    @Test
    public void testDistinctSymEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertQuery("SELECT DISTINCT sym FROM t_distinct_empty")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            """);
        });
    }

    @Test
    public void testDistinctSymExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            // Optimizer rewrites DISTINCT → GROUP BY + count(*), but we intercept in
            // generateSelectGroupBy and replace the entire chain with PostingIndex distinct
            assertQuery("SELECT DISTINCT sym FROM t_distinct_plan")
                    .noLeakCheck()
                    .assertsPlan("""
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_distinct_plan
                            """);
        });
    }

    @Test
    public void testDistinctSymFromPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'A', 3.0),
                    ('2024-01-02T00:00:00', 'C', 4.0),
                    ('2024-01-02T01:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_distinct")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);
        });
    }

    @Test
    public void testDistinctSymFromPostingIndexWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Use INSERT...SELECT with enough rows to trigger multi-stride sealing.
            // The bug was that close() seals without covered column data
            // (column memories already closed by TableWriter), leaving stale
            // per-gen sidecar files that the reader misinterprets as stride format.
            execute("""
                    INSERT INTO t_dist_cov
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                        rnd_symbol(100, 4, 6, 0),
                        rnd_double() * 100
                    FROM long_sequence(10000)
                    """);
            engine.releaseAllWriters();

            // Must not SIGSEGV — the covering sidecar files should be absent
            // (cleaned up by close) so the distinct cursor falls back to
            // per-key index scans without sidecar decoding.
            assertQuery("SELECT count() FROM (SELECT DISTINCT sym FROM t_dist_cov)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            100
                            """);
        });
    }

    @Test
    public void testDistinctSymKeyAliasPropagatesToMetadata() throws Exception {
        // Regression: when the DISTINCT->GROUP BY rewrite projects an aliased
        // symbol key (e.g. `sym AS k`), PostingIndexDistinctRecordCursorFactory
        // must name its output column after the alias, not the source column.
        // Naming it `sym` made the enclosing model fail to resolve `k`,
        // surfacing as "Invalid column: k" or an "wtf? k" assert depending on
        // the outer query shape.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_alias (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_alias VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            // Bare alias reference through a subquery.
            assertQuery("SELECT k FROM (SELECT sym AS k, count() AS cnt FROM t_dist_alias) ORDER BY k")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            k
                            A
                            B
                            """);

            // Qualified alias reference alongside a constant (select-virtual outer model).
            assertQuery("SELECT t0.k, -45 FROM (SELECT sym AS k, count() AS cnt FROM t_dist_alias) t0 ORDER BY t0.k")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            k\tcolumn
                            A\t-45
                            B\t-45
                            """);

            // Alias consumed by an outer expression.
            assertQuery("SELECT length(k) FROM (SELECT sym AS k, count() AS cnt FROM t_dist_alias) ORDER BY length(k)")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            length
                            1
                            1
                            """);
        });
    }

    @Test
    public void testDistinctSymKeyRangeScansAllPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_range (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_distinct_range
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00')::TIMESTAMP,
                           ('K' || (x % 50))::SYMBOL,
                           x * 1.0
                    FROM long_sequence(2_000)
                    """);
            drainWalQueue();

            execute("""
                    INSERT INTO t_distinct_range
                    SELECT dateadd('s', (x + 10000)::INT, '2024-01-01T00:00:00')::TIMESTAMP,
                           ('K' || (x % 3))::SYMBOL,
                           x * 1.0
                    FROM long_sequence(500)
                    """);
            drainWalQueue();

            execute("""
                    INSERT INTO t_distinct_range
                    SELECT dateadd('s', x::INT, '2024-01-02T00:00:00')::TIMESTAMP,
                           ('K' || (x % 100))::SYMBOL,
                           x * 1.0
                    FROM long_sequence(3_000)
                    """);
            drainWalQueue();

            assertQuery("SELECT count_distinct(sym) FROM t_distinct_range " +
                    "WHERE ts BETWEEN '2024-01-01T00:30:00' AND '2024-01-01T00:40:00'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count_distinct\n50\n");
            assertQuery("SELECT count_distinct(sym) FROM t_distinct_range " +
                    "WHERE ts BETWEEN '2024-01-01T02:00:00' AND '2024-01-01T03:00:00'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count_distinct\n3\n");
            assertQuery("SELECT count_distinct(sym) FROM t_distinct_range " +
                    "WHERE ts BETWEEN '2024-01-01T03:00:00' AND '2024-01-01T05:00:00'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count_distinct\n0\n");
            assertQuery("SELECT count_distinct(sym) FROM t_distinct_range " +
                    "WHERE ts BETWEEN '2024-01-02T00:00:00' AND '2024-01-02T01:00:00'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count_distinct\n100\n");
            assertQuery("SELECT count_distinct(sym) FROM t_distinct_range")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count_distinct\n100\n");
        });
    }

    @Test
    public void testDistinctSymMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0),
                    ('2024-01-03T00:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            // A appears in 2 partitions, B in 1 — should still return just A, B
            assertQuery("SELECT DISTINCT sym FROM t_distinct_mp")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    @Test
    public void testDistinctSymMultiPartitionHighCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_mp2 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert 1000 keys across 10 partitions, one partition at a time
            for (int p = 0; p < 10; p++) {
                execute("INSERT INTO t_dist_mp2"
                        + " SELECT dateadd('s', x::INT, dateadd('d', " + p + ", '2024-01-01'))::TIMESTAMP,"
                        + "     rnd_symbol(1000, 4, 8, 0),"
                        + "     rnd_double() * 100"
                        + " FROM long_sequence(100_000)");
                engine.releaseAllWriters();
            }
            engine.releaseAllReaders();

            // Each INSERT generates ~1000 unique random symbols independently.
            // Across 10 INSERTs the symbol table grows to ~10000 distinct keys.
            // Just verify it doesn't crash and returns a reasonable count.
            assertQuery("SELECT count() > 5000 AS hasKeys FROM (SELECT DISTINCT sym FROM t_dist_mp2)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            hasKeys
                            true
                            """);
        });
    }

    @Test
    public void testDistinctSymWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'C', 3.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_distinct_wal")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);
        });
    }

    @Test
    public void testDistinctWalO3() throws Exception {
        // Gap 14: DISTINCT on WAL table with O3 inserts
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_o3 VALUES
                    ('2024-01-02T00:00:00', 'B', 2.0),
                    ('2024-01-02T01:00:00', 'C', 3.0)
                    """);
            drainWalQueue();
            // O3: insert day 1
            execute("""
                    INSERT INTO t_distinct_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // Key order depends on symbol table insertion order: B=0, C=1, A=2
            // (B and C inserted first, then A via O3)
            assertQuery("SELECT DISTINCT sym FROM t_distinct_o3")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            B
                            C
                            A
                            """);
        });
    }

    @Test
    public void testDistinctWithIntervalAndNonIntervalFilter() throws Exception {
        // Regression test: DISTINCT with combined interval + non-interval WHERE
        // must not lose the interval predicate when the optimization is rejected.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_combined (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_combined VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 90.0),
                    ('2024-01-02T00:00:00', 'C', 5.0),
                    ('2024-01-02T01:00:00', 'D', 95.0)
                    """);
            engine.releaseAllWriters();

            // Only D matches: ts >= '2024-01-02' AND price > 50
            assertQuery("SELECT DISTINCT sym FROM t_dist_combined WHERE ts >= '2024-01-02' AND price > 50")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            D
                            """);
        });
    }

    @Test
    public void testDistinctWithNoIndexHintFallsBack() throws Exception {
        // no_index hint should prevent PostingIndex distinct
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_noidx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_noidx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // no_index: falls back to standard GroupBy, NOT PostingIndex
            assertQuery("SELECT /*+ no_index */ DISTINCT sym FROM t_dist_noidx")
                    .noLeakCheck()
                    .assertsPlanNotContaining("PostingIndex");

            // Data correctness — compare sorted results
            assertQuery("SELECT /*+ no_index */ DISTINCT sym FROM t_dist_noidx ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    @Test
    public void testDistinctWithNonIntervalFilterFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // Non-interval filter: falls back to standard GroupBy, NOT PostingIndex
            // Data correctness; a non-interval filter must not use PostingIndex.
            assertQuery("SELECT DISTINCT sym FROM t_dist_filt WHERE price > 15")
                    .expectSize()
                    .noLeakCheck()
                    .withPlanNotContaining("PostingIndex")
                    .returns("""
                            sym
                            B
                            """);
        });
    }

    @Test
    public void testDistinctWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        'S' || (x % 5),
                        rnd_double() * 100
                    FROM long_sequence(50)
                    """);
            engine.releaseAllWriters();

            // Verify all 5 symbols returned
            assertQuery("SELECT DISTINCT sym FROM t_dist ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            S0
                            S1
                            S2
                            S3
                            S4
                            """);

            // Verify plan uses PostingIndex distinct
            assertQuery("SELECT DISTINCT sym FROM t_dist")
                    .noLeakCheck()
                    .assertsPlan("""
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist
                            """);
        });
    }

    @Test
    public void testDistinctWithTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_where (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_where
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            // Second day — only keys A,B,C appear
            execute("""
                    INSERT INTO t_dist_where VALUES
                    ('2024-01-02T00:00:00', 'A', 1.0),
                    ('2024-01-02T01:00:00', 'B', 2.0),
                    ('2024-01-02T02:00:00', 'C', 3.0)
                    """);
            engine.releaseAllWriters();

            // Plan: PostingIndex distinct with interval partition frame
            assertQuery("SELECT DISTINCT sym FROM t_dist_where WHERE ts >= '2024-01-02'")
                    .noLeakCheck()
                    .assertsPlan("""
                            PostingIndex op: distinct on: sym
                                Interval forward scan on: t_dist_where
                                  intervals: [("2024-01-02T00:00:00.000000Z","MAX")]
                            """);

            // Data correctness: should only see A, B, C from the second day
            assertQuery("SELECT DISTINCT sym FROM t_dist_where WHERE ts >= '2024-01-02' ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);
        });
    }

    @Test
    public void testDistinctWithTimestampRangeNoMatches() throws Exception {
        // DISTINCT with a timestamp range that excludes all data → empty result
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_dist_empty WHERE ts >= '2025-01-01'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            """);
        });
    }

    @Test
    public void testDropCoveredColumn() throws Exception {
        // Drop a column that is INCLUDED in a covering index
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_col (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_col VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Drop a covered column
            execute("ALTER TABLE t_drop_col DROP COLUMN qty");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Index should still work (posting data is per-symbol, not per-covered-column)
            assertQuery("SELECT sym FROM t_drop_col WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            A
                            """);
        });
    }

    @Test
    public void testDropCoveredColumnFallsBackToTableScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_cov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_drop_cov WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            execute("ALTER TABLE t_drop_cov DROP COLUMN qty");

            assertQuery("SELECT price FROM t_drop_cov WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            30.5
                            """);

            execute("""
                    INSERT INTO t_drop_cov VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_drop_cov WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            30.5
                            40.5
                            """);
        });
    }

    // Regression: dropping an INCLUDE column tombstones its cover slot
    // (coveringColumnIndices entry -1), and the seal maps the slot as
    // type=-1/shift=0. A pure-append O3 commit keeps the dense gen0 plus
    // sparse O3 gens, so the reseal takes sealIncremental, whose
    // writeSidecarStrideData did not skip tombstoned slots and crashed with
    // "maxCompressedSize: unsupported column type -1". The setup needs more
    // than DENSE_STRIDE (256) symbol keys with the O3 batch touching only
    // the first stride: with every stride dirty, sealIncremental falls back
    // to the tombstone-safe sealFull and the bug stays hidden. Found by
    // WalWriterFuzzTest#testConvertPartitionToParquetWithCoveringIndex
    // (seeds 6069995328240L, 1781013513457L).
    @Test
    public void testDropCoveredColumnThenO3AppendIncrementalSeal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // More than DENSE_STRIDE (256) symbol keys, so the sealed index
            // has at least two strides.
            execute("""
                    INSERT INTO t_drop_o3
                    SELECT timestamp_sequence('2024-01-01', 1_000_000) ts,
                           ('sym_' || x)::SYMBOL sym,
                           x::DOUBLE price
                    FROM long_sequence(300)
                    """);

            // ALTER ADD INDEX on existing data builds a dense gen0 per
            // partition, the precondition for the incremental seal.
            execute("ALTER TABLE t_drop_o3 ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            // Tombstone the covering slot.
            execute("ALTER TABLE t_drop_o3 DROP COLUMN price");

            // Pure-append O3 into the last partition: the batch is internally
            // out of order so the writer takes the O3 commit path, but its
            // min ts is above the committed max, so the partition task runs
            // with append=true and the seal keeps the dense gen0 + sparse O3
            // gens. Existing symbol keys from the first stride only, so at
            // least one stride stays clean and the incremental seal does not
            // fall back to the (tombstone-safe) full seal.
            execute("""
                    INSERT INTO t_drop_o3 VALUES
                    ('2024-01-01T01:00:00', 'sym_1'),
                    ('2024-01-01T00:59:00', 'sym_2')
                    """);

            assertQuery("SELECT ts, sym FROM t_drop_o3 WHERE sym = 'sym_1'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000Z\tsym_1
                            2024-01-01T01:00:00.000000Z\tsym_1
                            """);
        });
    }

    @Test
    public void testDropIndexRemovesCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            try (TableReader r = engine.getReader("t_drop")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }

            execute("ALTER TABLE t_drop ALTER COLUMN sym DROP INDEX");

            try (TableReader r = engine.getReader("t_drop")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertFalse(r.getMetadata().isColumnIndexed(symIdx));
                assertFalse(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testDropUnrelatedColumnDoesNotBreakCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_unrel (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT,
                        unrelated VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_unrel VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 'x'),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 'y'),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 'z')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            execute("ALTER TABLE t_drop_unrel DROP COLUMN unrelated");

            assertQuery("SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            execute("""
                    INSERT INTO t_drop_unrel VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5, 400)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            40.5\t400
                            """);
        });
    }

    @Test
    public void testEqFilterOnUnknownSymbolWithOrderBy() throws Exception {
        // Regression: an ORDER BY on a SYMBOL column wraps the base cursor in
        // EncodedSortRecordCursor, whose init() phase probes baseCursor.getSymbolTable()
        // before iteration to build symbol-rank maps. When the WHERE literal is
        // unknown to the symbol map (no rows match), the covering cursor must still
        // expose a usable symbol table for that probe, even though iteration yields
        // nothing. Realistic shape: a populated table with a few known symbols, a
        // dashboard query for a value that has never been written. The fuzz hit
        // this via CREATE VIEW; assertQuery exercises the same API path directly.
        // The covering cursor under test only kicks in for a covering index, so
        // sym carries an explicit INCLUDE (ts) -- a bare INDEX TYPE POSTING is
        // non-covering and would take the plain posting-filter path instead.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_unknown_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (ts),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_unknown_sym
                    SELECT dateadd('h', x::INT, '2024-01-01')::TIMESTAMP,
                           ('K' || (x % 5))::SYMBOL,
                           (x % 100) * 1.5
                    FROM long_sequence(2_000)
                    """);
            engine.releaseAllWriters();

            // Sanity: known symbols return the expected row counts. Defends against
            // a future change accidentally short-circuiting the populated path too.
            // Hourly spacing over 2_000 rows spans ~83 day-partitions, exercising
            // the multi-partition advanceKey()/nextImpl() loops.
            assertQuery("SELECT count() FROM t_unknown_sym WHERE sym = 'K0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n400\n");

            // Positive baseline for the same factory chain that produced the
            // original NPE stack (LimitRecordCursorFactory + EncodedSortRecordCursor
            // + SortKeyEncoder). Confirms ORDER BY DESC + LIMIT works on a
            // populated key.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym = 'K0' ORDER BY 1 DESC LIMIT 5")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym
                            K0
                            K0
                            K0
                            K0
                            K0
                            """);

            // Empty result through the same LIMIT + ORDER BY chain.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym = 'qrsw' ORDER BY 1 DESC LIMIT 5")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("sym\n");

            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym = 'qrsw' ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\n");

            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym IN ('qrsw', 'zzz') ORDER BY 1")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("sym\n");

            // Mixed list: one known value, one unknown. Result must contain only
            // the known symbol's rows; the unknown literal must not poison the
            // multi-key path or short-circuit the cursor.
            assertQuery("SELECT count() FROM t_unknown_sym WHERE sym IN ('K0', 'qrsw')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n400\n");

            // Mixed list under ORDER BY: exercises the same EncodedSortRecordCursor
            // probe with partial key resolution. LIMIT samples the head of the
            // sorted stream to confirm only K0 rows are produced.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym IN ('K0', 'qrsw') ORDER BY 1 LIMIT 3")
                    .noLeakCheck()
                    .returns("""
                            sym
                            K0
                            K0
                            K0
                            """);

            // Bind-variable single-key path: getCursor() takes the
            // symbolFunctionRuntimeConstant branch, resolving the value at
            // runtime. Pre-fix, an unknown bind value hit the same NPE.
            bindVariableService.clear();
            bindVariableService.setStr("sym", "qrsw");
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym = :sym ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\n");

            // Bind-variable IN-list path: keyValueFuncs branch in the multi-key
            // code, both bind values unknown.
            bindVariableService.clear();
            bindVariableService.setStr(0, "qrsw");
            bindVariableService.setStr(1, "zzz");
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym IN ($1, $2) ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\n");

            // LATEST ON path: SingleKeyCoveringCursor.hasNextLatestBy() carries
            // its own VALUE_NOT_FOUND guard. Combine with an outer ORDER BY on
            // the SYMBOL column so the upstream sort still probes the empty
            // cursor's getSymbolTable() before iteration.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym = 'qrsw' LATEST ON ts PARTITION BY sym ORDER BY 1")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex op: latest on: sym")
                    .returns("sym\n");

            // LATEST ON multi-key, all unknown. MultiKeyCoveringCursor.hasNextLatestBy()
            // has no explicit guard; it relies on the empty multiKeys list keeping
            // the inner loop from running. The outer ORDER BY still triggers the
            // getSymbolTable() probe.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym IN ('qrsw', 'zzz') LATEST ON ts PARTITION BY sym ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\n");

            // LATEST ON multi-key, mixed: returns the latest K0 row; the
            // unknown 'qrsw' partition is naturally absent.
            assertQuery("SELECT sym FROM t_unknown_sym WHERE sym IN ('K0', 'qrsw') LATEST ON ts PARTITION BY sym ORDER BY 1")
                    .noLeakCheck()
                    .returns("sym\nK0\n");
        });
    }

    @Test
    public void testFallbackInList3Keys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in3 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30)
                    """);
            engine.releaseAllWriters();

            // 3-key IN-list with non-covered column — needs 3 independent cursors
            assertQuery("SELECT price, extra FROM t_in3 WHERE sym IN ('A', 'B', 'C')")
                    .noLeakCheck()
                    .returns("""
                            price\textra
                            1.0\t10
                            2.0\t20
                            3.0\t30
                            """);
        });
    }

    @Test
    public void testFallbackInList5KeysMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in5 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in5 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30),
                    ('2024-01-02T00:00:00', 'D', 4.0, 40),
                    ('2024-01-02T01:00:00', 'E', 5.0, 50),
                    ('2024-01-02T02:00:00', 'A', 6.0, 60),
                    ('2024-01-03T00:00:00', 'B', 7.0, 70),
                    ('2024-01-03T01:00:00', 'C', 8.0, 80)
                    """);
            engine.releaseAllWriters();

            // 5-key IN-list across 3 partitions with non-covered column
            assertQuery("SELECT price, extra FROM t_in5 WHERE sym IN ('A', 'B', 'C', 'D', 'E')")
                    .noLeakCheck()
                    .returns("""
                            price\textra
                            1.0\t10
                            2.0\t20
                            3.0\t30
                            4.0\t40
                            5.0\t50
                            6.0\t60
                            7.0\t70
                            8.0\t80
                            """);
        });
    }

    @Test
    public void testFallbackInListWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_in_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30)
                    """);
            drainWalQueue();

            assertQuery("SELECT price, extra FROM t_in_wal WHERE sym IN ('A', 'B', 'C')")
                    .noLeakCheck()
                    .returns("""
                            price\textra
                            1.0\t10
                            2.0\t20
                            3.0\t30
                            """);
        });
    }

    @Test
    public void testFallbackInListWithNonCoveredColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_fallback (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_fallback VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 2)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to FilterOnValues
            assertQuery("SELECT price, extra FROM t_in_fallback WHERE sym IN ('A', 'B')")
                    .noLeakCheck()
                    .returns("""
                            price\textra
                            10.5\t1
                            20.5\t2
                            """);
        });
    }

    @Test
    public void testFallbackSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_star (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_star VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // SELECT * includes ts which is not covered — falls back to regular scan
            assertQuery("SELECT * FROM t_star WHERE sym = 'A'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t10.5
                            2024-01-01T02:00:00.000000Z\tA\t11.5
                            """);
        });
    }

    @Test
    public void testFallbackStringWithNonCoveredColumn() throws Exception {
        // Selecting non-covered column alongside covered STRING forces fallback to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fb_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fb_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello', 1),
                    ('2024-01-01T01:00:00', 'Y', 'world', 2),
                    ('2024-01-01T02:00:00', 'X', 'foo', 3)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to regular indexed scan
            assertQuery("SELECT label, extra FROM t_fb_string WHERE sym = 'X'")
                    .noLeakCheck()
                    .returns("""
                            label\textra
                            hello\t1
                            foo\t3
                            """);
        });
    }

    @Test
    public void testFallbackVarcharWithNonCoveredColumn() throws Exception {
        // Selecting non-covered column alongside covered VARCHAR forces fallback to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fb_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fb_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 1),
                    ('2024-01-01T01:00:00', 'B', 'bob', 2),
                    ('2024-01-01T02:00:00', 'A', 'anna', 3)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to regular indexed scan
            assertQuery("SELECT name, extra FROM t_fb_varchar WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            name\textra
                            alice\t1
                            anna\t3
                            """);
        });
    }

    @Test
    public void testFallbackWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_filter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_filter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'A', 30.5)
                    """);
            engine.releaseAllWriters();

            // Additional filter on covered column — CoveringIndex does not support filters
            assertQuery("SELECT price FROM t_filter WHERE sym = 'A' AND price > 15.0")
                    .noLeakCheck()
                    .returns("""
                            price
                            20.5
                            30.5
                            """);
        });
    }

    @Test
    public void testFallbackWithNonCoveredColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noncov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        other INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noncov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 2),
                    ('2024-01-01T02:00:00', 'A', 11.5, 3)
                    """);
            engine.releaseAllWriters();

            // Selecting a non-covered column forces fallback to regular scan
            assertQuery("SELECT price, other FROM t_noncov WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price\tother
                            10.5\t1
                            11.5\t3
                            """);
        });
    }

    @Test
    public void testFilterOnExcludedValuesThrowingFilterDoesNotLeakIndexReader() throws Exception {
        // Regression: FilterOnExcludedValues opens per-symbol index cursors via
        // HeapRowCursor.of(), whose first hasNext() evaluates the post-filter on each
        // sub-cursor. When that filter throws (here, a DECIMAL scale-adjustment overflow
        // on small < big), the singleton HeapRowCursor was left un-closed because
        // PageFrameRecordCursorImpl.rowCursor never got assigned. The per-symbol index
        // cursors then stayed outside the PostingIndexFwdReader.freeCursors pool and
        // their block buffers leaked. The fix closes the row cursor factory from
        // PageFrameRecordCursorImpl.close() so the singleton always cleans up.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_excl_leak (
                        sym SYMBOL INDEX TYPE POSTING DELTA INCLUDE (v),
                        small DECIMAL(38, 3),
                        big DECIMAL(76, 2),
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // big holds the maximum value of DECIMAL(76, 2). Scaling it up by 10^1 to
            // match small's scale overflows the 256-bit intermediate at filter time.
            execute("""
                    INSERT INTO t_excl_leak
                    SELECT
                        rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7',null),
                        '1.000'::DECIMAL(38, 3),
                        '99999999999999999999999999999999999999999999999999999999999999999999999999.99'::DECIMAL(76, 2),
                        rnd_double(),
                        timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L)
                    FROM long_sequence(120)
                    """);
            drainWalQueue();

            // SELECT v (a column value) forces per-row materialization through the per-symbol
            // cursors, so HeapRowCursor.of() evaluates the post-filter and throws on it.
            Throwable caught = null;
            try (RecordCursorFactory f = select(
                    "SELECT v FROM t_excl_leak " +
                            "WHERE NOT ((sym IN ('s7', null) OR small < big))");
                 RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            } catch (Throwable t) {
                caught = t;
            }
            assertNotNull("expected DECIMAL scale-adjustment overflow", caught);
        });
    }

    @Test
    public void testFilterOnSubQueryThrowingFilterDoesNotLeakIndexReader() throws Exception {
        // Regression: FilterOnSubQuery builds a per-symbol index cursor for every key
        // returned by the sub-query through HeapRowCursorFactory.getCursor, whose call into
        // HeapRowCursor.of evaluates the post-filter on each sub-cursor. When that filter
        // throws (here, a DECIMAL scale-adjustment overflow on small < big), the throw fires
        // inside getCursor before its return assigns PageFrameRecordCursorImpl.rowCursor, so
        // the singleton HeapRowCursor is left with populated per-symbol SymbolIndexFiltered
        // RowCursor sub-cursors that each hold an open index reader cursor. The fix frees
        // FilterOnSubQueryRecordCursorFactory.rowCursorFactory in _close(), which cascades
        // into HeapRowCursorFactory.close() and returns the per-symbol cursors to the pool.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sub_leak (
                        sym SYMBOL INDEX TYPE POSTING DELTA,
                        small DECIMAL(38, 3),
                        big DECIMAL(76, 2),
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // big holds the maximum value of DECIMAL(76, 2). Scaling it up by 10^1 to
            // match small's scale overflows the 256-bit intermediate at filter time.
            execute("""
                    INSERT INTO t_sub_leak
                    SELECT
                        rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7',null),
                        '1.000'::DECIMAL(38, 3),
                        '99999999999999999999999999999999999999999999999999999999999999999999999999.99'::DECIMAL(76, 2),
                        rnd_double(),
                        timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L)
                    FROM long_sequence(120)
                    """);
            drainWalQueue();

            Throwable caught = null;
            try (RecordCursorFactory f = select(
                    "SELECT v FROM t_sub_leak " +
                            "WHERE sym IN (SELECT 's0' UNION SELECT 's1') AND small < big");
                 RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            } catch (Throwable t) {
                caught = t;
            }
            assertNotNull("expected DECIMAL scale-adjustment overflow", caught);
        });
    }

    @Test
    public void testFilterOnValuesThrowingFilterDoesNotLeakIndexReader() throws Exception {
        // Regression: FilterOnValues opens a per-symbol index cursor for every IN-list key
        // through HeapRowCursorFactory.getCursor, whose call into HeapRowCursor.of evaluates
        // the post-filter on each sub-cursor. When that filter throws (here, a DECIMAL
        // scale-adjustment overflow on small < big), the throw fires inside getCursor before
        // its return assigns PageFrameRecordCursorImpl.rowCursor, so the singleton
        // HeapRowCursor is left with populated per-symbol SymbolIndexFilteredRowCursor
        // sub-cursors that each hold an open index reader cursor. The fix frees
        // FilterOnValuesRecordCursorFactory.rowCursorFactory in _close(), which cascades into
        // HeapRowCursorFactory.close() and returns the per-symbol cursors to the pool.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_val_leak (
                        sym SYMBOL INDEX TYPE POSTING DELTA,
                        small DECIMAL(38, 3),
                        big DECIMAL(76, 2),
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // big holds the maximum value of DECIMAL(76, 2). Scaling it up by 10^1 to
            // match small's scale overflows the 256-bit intermediate at filter time.
            execute("""
                    INSERT INTO t_val_leak
                    SELECT
                        rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7',null),
                        '1.000'::DECIMAL(38, 3),
                        '99999999999999999999999999999999999999999999999999999999999999999999999999.99'::DECIMAL(76, 2),
                        rnd_double(),
                        timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L)
                    FROM long_sequence(120)
                    """);
            drainWalQueue();

            Throwable caught = null;
            try (RecordCursorFactory f = select(
                    "SELECT v FROM t_val_leak " +
                            "WHERE sym IN ('s0', 's1') AND small < big");
                 RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            } catch (Throwable t) {
                caught = t;
            }
            assertNotNull("expected DECIMAL scale-adjustment overflow", caught);
        });
    }

    @Test
    public void testFilteredIndexScanWithArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE commodities_market_data (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX TYPE POSTING,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO commodities_market_data
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Full query with array access — verify no crash
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) AS open,
                        max(best_bid) AS high,
                        min(best_bid) AS low,
                        last(best_bid) AS close,
                        avg(best_bid) AS avgr,
                        sum(bids[2][1]) AS volume
                    FROM commodities_market_data
                    WHERE symbol = 'CL' AND exchange = 'NYMEX'
                    SAMPLE BY 15m
                    """); var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) count++;
                assertTrue(count > 0);
            }
        });
    }

    @Test
    public void testFilteredIndexScanWithArrayColumnBitmap() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE cmd_bitmap (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO cmd_bitmap
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Verify the query doesn't crash — any result is fine
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) AS open,
                        max(best_bid) AS high,
                        min(best_bid) AS low,
                        last(best_bid) AS close,
                        avg(best_bid) AS avgr,
                        sum(bids[2][1]) AS volume
                    FROM cmd_bitmap
                    WHERE symbol = 'CL' AND exchange = 'NYMEX'
                    SAMPLE BY 15m
                    """); var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) count++;
                assertTrue(count > 0);
            }
        });
    }

    @Test
    public void testFsstLongVarcharTriggersCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label, payload),
                        label VARCHAR,
                        payload STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           CASE WHEN x % 5 = 0 THEN 'B' ELSE 'A' END,
                           ('label_text_with_some_repeating_words_lorem_ipsum_dolor_sit_amet_' || x)::VARCHAR,
                           'string_payload_with_repeating_text_lorem_ipsum_dolor_sit_amet_consectetur_' || x
                    FROM long_sequence(500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM (SELECT label, payload FROM t_fsst WHERE sym = 'A')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n400\n");

            assertQuery("SELECT sym, count() FROM t_fsst WHERE sym = 'A' AND payload != '' GROUP BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("sym\tcount\nA\t400\n");

            assertQuery("SELECT label, payload FROM t_fsst WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            label\tpayload
                            label_text_with_some_repeating_words_lorem_ipsum_dolor_sit_amet_499\t\
                            string_payload_with_repeating_text_lorem_ipsum_dolor_sit_amet_consectetur_499
                            """);
            assertQuery("SELECT label, payload FROM t_fsst WHERE sym = 'B' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            label\tpayload
                            label_text_with_some_repeating_words_lorem_ipsum_dolor_sit_amet_500\t\
                            string_payload_with_repeating_text_lorem_ipsum_dolor_sit_amet_consectetur_500
                            """);
        });
    }

    @Test
    public void testFsstRoundtrip() throws Exception {
        assertMemoryLeak(() -> {
            String[] values = {
                    "order_confirmation_alpha_2024",
                    "order_confirmation_beta_2024",
                    "order_confirmation_gamma_2024"
            };
            byte[][] valueBytes = new byte[values.length][];
            int totalLen = 0;
            for (int i = 0; i < values.length; i++) {
                valueBytes[i] = values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                totalLen += valueBytes[i].length;
            }
            int reps = 100;
            int trainCount = values.length * reps;
            long offsetsBytes = (long) (trainCount + 1) * Long.BYTES;
            long trainBufSize = (long) totalLen * reps;
            long cmpCap = trainBufSize * 2 + 16;
            long decCap = trainBufSize + 32;
            long batchScratchBytes = (long) trainCount * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;

            long trainBuf = Unsafe.malloc(trainBufSize, MemoryTag.NATIVE_DEFAULT);
            long srcOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long cmpBuf = Unsafe.malloc(cmpCap, MemoryTag.NATIVE_DEFAULT);
            long cmpOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long decBuf = Unsafe.malloc(decCap, MemoryTag.NATIVE_DEFAULT);
            long decOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long tableBuf = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long batchScratchAddr = Unsafe.malloc(batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                long pos = 0;
                int ord = 0;
                for (int rep = 0; rep < reps; rep++) {
                    for (byte[] bytes : valueBytes) {
                        Unsafe.putLong(srcOffsAddr + (long) ord * Long.BYTES, pos);
                        for (byte b : bytes) {
                            Unsafe.putByte(trainBuf + pos++, b);
                        }
                        ord++;
                    }
                }
                Unsafe.putLong(srcOffsAddr + (long) trainCount * Long.BYTES, pos);

                long packed = FSSTNative.trainAndCompressBlock(
                        trainBuf, srcOffsAddr, trainCount,
                        cmpBuf, cmpCap, cmpOffsAddr,
                        tableBuf, batchScratchAddr);
                assertTrue("train+compress must succeed", packed >= 0);
                long compressed = FSSTNative.unpackCompressed(packed);
                assertTrue("repetitive data must compress smaller", compressed < pos);

                assertTrue("table import must succeed", FSSTNative.importTable(decoder, tableBuf) > 0);
                long decoded = FSSTNative.decompressBlock(
                        decoder, cmpBuf, cmpOffsAddr, Long.BYTES, trainCount,
                        decBuf, decCap, decOffsAddr);
                assertEquals("decoded length must match original", pos, decoded);

                for (int i = 0; i < trainCount; i++) {
                    long lo = Unsafe.getLong(decOffsAddr + (long) i * Long.BYTES);
                    long hi = Unsafe.getLong(decOffsAddr + (long) (i + 1) * Long.BYTES);
                    byte[] expected = valueBytes[i % values.length];
                    assertEquals("value " + i + " length", expected.length, (int) (hi - lo));
                    for (int j = 0; j < expected.length; j++) {
                        assertEquals("value " + i + " byte " + j,
                                expected[j], Unsafe.getByte(decBuf + lo + j));
                    }
                }
            } finally {
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(batchScratchAddr, batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(tableBuf, FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decBuf, decCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(cmpOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(cmpBuf, cmpCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(trainBuf, trainBufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetPageFrameCursor_MultiKeyMixedKnownUnknown() throws Exception {
        // Multi-key getPageFrameCursor: bind variable list with one known and
        // one unknown value. Drives the keyValueFuncs != null branch where a
        // pre-resolved VALUE_NOT_FOUND is re-resolved at exec time, plus the
        // "skip unknown, keep known" path that leaves multiKeys non-empty.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_mk_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_mk_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("k1", "A");
            bindVariableService.setStr("k2", "MISSING");
            assertQuery("SELECT count() cnt FROM t_pf_mk_bind WHERE sym IN (:k1, :k2)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n1\n");
        });
    }

    @Test
    public void testGetSymbolTableViaPageFrameMultiKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_sym
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END,
                           CASE x % 3 WHEN 0 THEN 'tag_x' WHEN 1 THEN 'tag_y' ELSE 'tag_z' END,
                           x * 1.5
                    FROM long_sequence(2_000)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT sym, tag FROM t_pf_sym WHERE sym IN ('A','B') AND price > 100 ORDER BY price LIMIT 10")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\ttag
                            B\ttag_y
                            A\ttag_x
                            B\ttag_y
                            A\ttag_x
                            B\ttag_y
                            A\ttag_x
                            B\ttag_y
                            A\ttag_x
                            B\ttag_y
                            A\ttag_x
                            """);

            assertQuery("SELECT sym, count() FROM t_pf_sym WHERE sym IN ('A','B') GROUP BY sym ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("sym\tcount\nA\t666\nB\t667\n");
        });
    }

    @Test
    public void testInListWithDuplicateBindVarKeys() throws Exception {
        // Literal IN-list duplicates are deduped upstream by the WhereClauseParser
        // (see testInListWithDuplicateKeys), so only bind-variable / runtime-constant
        // duplicates reach the multi-key covering build loops. Without a contains()
        // guard there, openPartitionCursors opens one posting cursor per slot and the
        // heap-merge emits each matching row once per duplicate key: duplicate rows in
        // the record cursor, inflated count/sum in the page-frame (parallel GROUP BY)
        // path, and the latest row emitted multiple times in LATEST ON.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_dup_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_dup_bind VALUES
                    ('2024-01-01T00:00:00.000000Z', 'A', 1.0),
                    ('2024-01-01T00:00:01.000000Z', 'B', 2.0),
                    ('2024-01-01T00:00:02.000000Z', 'A', 3.0)
                    """);

            // 1) Plain projection - record cursor (getCursor build loop).
            final ObjList<BindVarTuple> projection = new ObjList<>();
            projection.add(BindVarTuple.ok(
                    "both bind vars resolve to 'A' - each A row appears once",
                    """
                            price
                            1.0
                            3.0
                            """,
                    bindVariableService -> {
                        bindVariableService.setStr(0, "A");
                        bindVariableService.setStr(1, "A");
                    }
            ));
            projection.add(BindVarTuple.ok(
                    "distinct keys A,B - all matching rows returned",
                    """
                            price
                            1.0
                            2.0
                            3.0
                            """,
                    bindVariableService -> {
                        bindVariableService.setStr(0, "A");
                        bindVariableService.setStr(1, "B");
                    }
            ));
            assertQuery("SELECT price FROM t_in_dup_bind WHERE sym IN ($1, $2) ORDER BY ts").noLeakCheck().noRandomAccess().assertBinds(projection);

            // 2) Parallel GROUP BY - page-frame cursor (getPageFrameCursor build loop).
            final ObjList<BindVarTuple> aggregate = new ObjList<>();
            aggregate.add(BindVarTuple.ok(
                    "duplicate key 'A' - count/sum not inflated",
                    """
                            sym\tc\ts
                            A\t2\t4.0
                            """,
                    bindVariableService -> {
                        bindVariableService.setStr(0, "A");
                        bindVariableService.setStr(1, "A");
                    }
            ));
            assertQuery("SELECT sym, count() c, sum(price) s FROM t_in_dup_bind WHERE sym IN ($1, $2) GROUP BY sym").noLeakCheck().expectSize().assertBinds(aggregate);

            // 3) LATEST ON - multi-key latest-by covering cursor.
            final ObjList<BindVarTuple> latest = new ObjList<>();
            latest.add(BindVarTuple.ok(
                    "duplicate key 'A' - latest row emitted once",
                    """
                            sym\tprice
                            A\t3.0
                            """,
                    bindVariableService -> {
                        bindVariableService.setStr(0, "A");
                        bindVariableService.setStr(1, "A");
                    }
            ));
            assertQuery("SELECT sym, price FROM t_in_dup_bind WHERE sym IN ($1, $2) LATEST ON ts PARTITION BY sym").noLeakCheck().noRandomAccess().assertBinds(latest);
        });
    }

    @Test
    public void testInListWithDuplicateKeys() throws Exception {
        // Gap 11: IN-list with duplicate keys
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_dup VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // SQL engine deduplicates IN-list values — 'A' appears once
            assertQuery("SELECT price FROM t_in_dup WHERE sym IN ('A', 'A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            20.0
                            """);
        });
    }

    @Test
    public void testIncludeOnlyValidWithPosting() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE bad (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE BITMAP INCLUDE (price),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE is only supported for POSTING index type"));
            }
        });
    }

    @Test
    public void testIncludeParseMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (a, b, c),
                        a INT,
                        b LONG,
                        c DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("multi")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(4, coveringCols.size());
            }
        });
    }

    @Test
    public void testIncludeWithNonExistentColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_nonexist (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (nonexistent),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE column doesn't exist"));
            }
        });
    }

    @Test
    public void testIncludeWithStringColumnAccepted() throws Exception {
        // STRING is now supported in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_string_include (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (s),
                        s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try (TableReader r = engine.getReader("t_string_include")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testIncludeWithVarcharColumnAccepted() throws Exception {
        // VARCHAR is now supported in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar_include (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        v VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try (TableReader r = engine.getReader("t_varchar_include")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testIncrementalSealPreservesCoveringForCleanStrides() throws Exception {
        // Incremental seal: gen0 dense + sparse gens → only dirty strides re-encoded.
        // Clean strides must copy old sidecar data verbatim.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "incr_seal_cover";
                int plen = path.size();

                // 300 keys (2 strides: 0-255 in stride 0, 256-299 in stride 1)
                int keyCount = 300;
                long colAddr = Unsafe.malloc((long) keyCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keyCount; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    // First writer: write all keys, commit, close (seal → gen0 dense)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );
                        for (int k = 0; k < keyCount; k++) {
                            writer.add(k, k);
                        }
                        writer.setMaxValue(keyCount - 1);
                        writer.commit();
                        writer.seal(); // explicit seal: gen0 dense + sidecar
                    }

                    // Second writer: touch only key 260 (stride 1) → stride 0 stays clean
                    PostingIndexWriter writer2 = new PostingIndexWriter(configuration);
                    writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    writer2.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{1},
                            new int[]{ColumnType.LONG},
                            1
                    );
                    writer2.add(260, keyCount); // new row for key 260
                    writer2.setMaxValue(keyCount);
                    writer2.commit();
                    writer2.seal(); // incremental seal: stride 0 clean, stride 1 dirty
                    writer2.close();

                    // Read: verify covering values for clean stride keys
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG}), EMPTY_CVR, 0)) {
                        // Key 0 is in stride 0 (clean stride) — should have correct covered value
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue("covering should be available after incremental seal", cc.isCoveredAvailable(0));
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 100 is in stride 0 (clean stride)
                        cursor = reader.getCursor(100, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(100, cc.next());
                        assertEquals(1100L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);

                        // Key 260 is in stride 1 (dirty stride) — should also work
                        cursor = reader.getCursor(260, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));

                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) keyCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    // Regression: sealIncremental sized the shared dirty-stride sidecar
    // scratch at Long.BYTES per merged value, but
    // writeSidecarFixedStrideForColumn assembles one key's raw values into it
    // at (1 << shift) bytes each -- 16 for UUID, 32 for LONG256. A symbol key
    // holding more than half (16B) or a quarter (32B) of a dirty stride's
    // merged values wrote past the allocation: the hot key below overran the
    // scratch by ~94 KB of native heap. The overrun is silent in a plain run
    // (the compressor reads the same out-of-bounds bytes straight back), so
    // this test pins the workload and the covered query results; a guard-zone
    // probe over the scratch demonstrated the overrun before the fix.
    @Test
    public void testIncrementalSealWideIncludeHotSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        u UUID,
                        l LONG256
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // One hot symbol (key 0) holds 4_000 of the 4_300 rows; 300 cold
            // symbols (keys 1..300, one row each) push the key count past
            // DENSE_STRIDE (256) so the sealed index has two strides, with
            // the hot key holding nearly all of stride 0's values.
            execute("""
                    INSERT INTO t_wide_o3
                    SELECT timestamp_sequence('2024-01-01', 1_000_000) ts,
                           (CASE WHEN x <= 4_000 THEN 'sym_hot' ELSE 'sym_' || (x - 4_000) END)::SYMBOL sym,
                           'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID u,
                           '0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7'::LONG256 l
                    FROM long_sequence(4_300)
                    """);

            // ALTER ADD INDEX on existing data builds a dense gen0 per
            // partition, the precondition for the incremental seal.
            execute("ALTER TABLE t_wide_o3 ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (u, l)");

            // Pure-append O3 batch (internally out of order, min ts above the
            // committed max) touching only stride-0 keys, so stride 1 stays
            // clean and sealIncremental does not fall back to sealFull.
            execute("""
                    INSERT INTO t_wide_o3 VALUES
                    ('2024-01-01T02:00:00', 'sym_hot',
                     'b1b2c3d4-e5f6-4708-9192-a3b4c5d6e7f8'::UUID,
                     '0x8aa1b2c3d4e5f60718293a4b5c6d7e8f9aa1b2c3d4e5f60718293a4b5c6d7e8f'::LONG256),
                    ('2024-01-01T01:59:00', 'sym_1',
                     'c1c2c3d4-e5f6-4718-8192-a3b4c5d6e7f9'::UUID,
                     '0x9bb1c2d3e4f5061728394a5b6c7d8e9f0bb1c2d3e4f5061728394a5b6c7d8e9f'::LONG256)
                    """);

            // Covered read of a dirty-stride cold key: one gen0 row plus the
            // O3 row, decoded from the re-encoded stride-0 sidecar blocks.
            assertQuery("SELECT ts, u, l FROM t_wide_o3 WHERE sym = 'sym_1'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tu\tl
                            2024-01-01T01:06:40.000000Z\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7
                            2024-01-01T01:59:00.000000Z\tc1c2c3d4-e5f6-4718-8192-a3b4c5d6e7f9\t0x9bb1c2d3e4f5061728394a5b6c7d8e9f0bb1c2d3e4f5061728394a5b6c7d8e9f
                            """);

            // Covered read of a clean-stride key (stride 1 copies the old
            // sidecar block verbatim during the incremental seal).
            assertQuery("SELECT ts, u, l FROM t_wide_o3 WHERE sym = 'sym_300'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tu\tl
                            2024-01-01T01:11:39.000000Z\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7
                            """);

            // Filtered count over the hot key's covered values decodes its
            // entire sidecar block: 4_000 gen0 rows match, the O3 row does not.
            assertQuery("""
                    SELECT count() FROM t_wide_o3
                    WHERE sym = 'sym_hot'
                      AND u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID
                      AND l = '0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7'::LONG256
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            count
                            4000
                            """);
        });
    }

    @Test
    public void testIndexRebuildAfterDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rebuild (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rebuild VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0),
                    ('2024-01-01T02:00:00', 'B', 20.0),
                    ('2024-01-02T01:00:00', 'A', 30.0),
                    ('2024-01-02T02:00:00', 'B', 40.0)
                    """);
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_rebuild WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            30.0
                            """);

            // Drop and re-add: new index version created, old files purged
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_rebuild WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            30.0
                            """);

            assertQuery("SELECT price FROM t_rebuild WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            20.0
                            40.0
                            """);

            execute("DROP TABLE t_rebuild");
        });
    }

    @Test
    public void testInsertIntoSelectWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_src (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_src VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);

            execute("""
                    CREATE TABLE t_dst (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("INSERT INTO t_dst SELECT * FROM t_src");
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_dst WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            11.5
                            """);
        });
    }

    @Test
    public void testLatestByAllColumnTypes() throws Exception {
        // LATEST BY through CoveringRecord — exercises every type accessor
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            v_byte, v_short, v_int, v_long, v_float, v_double,
                            v_bool, v_date, v_uuid, v_ipv4,
                            v_varchar, v_string
                        ),
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_varchar VARCHAR,
                        v_string STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1000, 10_000, 1.5, 2.5, true,  '2024-06-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.2.3.4', 'hello', 'world'),
                    ('2024-01-02T00:00:00', 'A', 2, 200, 2000, 20_000, 2.5, 3.5, false, '2024-07-01T00:00:00.000Z', '22222222-2222-2222-2222-222222222222', '5.6.7.8', 'bye',   'earth'),
                    ('2024-01-02T12:00:00', 'B', 3, 300, 3000, 30_000, 3.5, 4.5, true,  '2024-08-01T00:00:00.000Z', '33333333-3333-3333-3333-333333333333', '9.0.1.2', 'foo',   'bar')
                    """);
            engine.releaseAllWriters();

            assertQuery("""
                    SELECT v_byte, v_short, v_int, v_long, v_float, v_double,
                           v_bool, v_date, v_uuid, v_ipv4, v_varchar, v_string
                    FROM t_latest_all WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            v_byte\tv_short\tv_int\tv_long\tv_float\tv_double\tv_bool\tv_date\tv_uuid\tv_ipv4\tv_varchar\tv_string
                            2\t200\t2000\t20000\t2.5\t3.5\tfalse\t2024-07-01T00:00:00.000Z\t22222222-2222-2222-2222-222222222222\t5.6.7.8\tbye\tearth
                            """);
        });
    }

    @Test
    public void testLatestByArrayColumn() throws Exception {
        // LATEST BY with ARRAY covered column through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals),
                        vals DOUBLE[]
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_arr VALUES
                    ('2024-01-01T00:00:00', 'A', ARRAY[1.0, 2.0]),
                    ('2024-01-02T00:00:00', 'A', ARRAY[3.0, 4.0])
                    """);
            engine.releaseAllWriters();

            assertQuery("""
                    SELECT vals
                    FROM t_latest_arr WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            vals
                            [3.0,4.0]
                            """);
        });
    }

    @Test
    public void testLatestByBinaryColumn() throws Exception {
        // LATEST BY with BINARY covered column through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data),
                        data BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_latest_bin VALUES ('2024-01-01T00:00:00', 'A', rnd_bin(10, 10, 0))");
            execute("INSERT INTO t_latest_bin VALUES ('2024-01-02T00:00:00', 'A', rnd_bin(10, 10, 0))");
            engine.releaseAllWriters();

            // Just verify the query runs without errors and returns a row
            assertQuery("""
                    SELECT count(*) AS count
                    FROM t_latest_bin WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testLatestByDecimalAndLong256() throws Exception {
        // LATEST BY for DECIMAL128 and LONG256 through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_dec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d128, hash),
                        d128 DECIMAL(38, 10),
                        hash LONG256
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_dec VALUES
                    ('2024-01-01T00:00:00', 'A', '123.4567890000'::DECIMAL(38, 10), 0x01),
                    ('2024-01-02T00:00:00', 'A', '999.9999999999'::DECIMAL(38, 10), 0x0f)
                    """);
            engine.releaseAllWriters();

            assertQuery("""
                    SELECT d128, hash
                    FROM t_latest_dec WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            d128\thash
                            999.9999999999\t0x0f
                            """);
        });
    }

    @Test
    public void testLatestByEmptyResultExercisesOfEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_empty VALUES ('2024-01-01', 'A', 10.0)");
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_empty WHERE sym = 'NEVER_INSERTED'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("SELECT price FROM t_empty WHERE sym IN ('NEVER_INSERTED','ALSO_UNKNOWN')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("SELECT price FROM t_empty WHERE sym = 'NEVER_INSERTED' AND price > 0")
                    .noLeakCheck()
                    .returns("price\n");

            assertQuery("SELECT price FROM t_empty WHERE sym = 'NEVER_INSERTED' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testLatestByFilterMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Day 1: A has price 5, 10. Day 2: A has price 3, 8.
            // With filter price > 7: latest matching = day 2 price=8
            execute("""
                    INSERT INTO t_latest_mp VALUES
                    ('2024-01-01T01:00:00', 'A', 5.0),
                    ('2024-01-01T02:00:00', 'A', 10.0),
                    ('2024-01-02T01:00:00', 'A', 3.0),
                    ('2024-01-02T02:00:00', 'A', 8.0)
                    """);
            engine.releaseAllWriters();

            // Latest partition (day 2) has price=8 at 02:00 which matches price > 7
            assertQuery("SELECT price FROM t_latest_mp WHERE sym = 'A' AND price > 7 LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            8.0
                            """);
        });
    }

    @Test
    public void testLatestByFilterNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nomatch (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nomatch VALUES
                    ('2024-01-01T01:00:00', 'A', 1.0),
                    ('2024-01-01T02:00:00', 'A', 2.0),
                    ('2024-01-01T03:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            // Filter that matches no rows — empty result
            assertQuery("SELECT price FROM t_latest_nomatch WHERE sym = 'A' AND price > 100 LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testLatestByMultiKeyAllTypes() throws Exception {
        // LATEST BY multi-key with diverse types through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v_float, v_short, v_byte),
                        v_float FLOAT,
                        v_short SHORT,
                        v_byte BYTE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10, 1),
                    ('2024-01-02T00:00:00', 'A', 2.5, 20, 2),
                    ('2024-01-01T12:00:00', 'B', 3.5, 30, 3),
                    ('2024-01-02T12:00:00', 'B', 4.5, 40, 4)
                    """);
            engine.releaseAllWriters();

            assertQuery("""
                    SELECT sym, v_float, v_short, v_byte
                    FROM t_latest_multi
                    WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\tv_float\tv_short\tv_byte
                            A\t2.5\t20\t2
                            B\t4.5\t40\t4
                            """);
        });
    }

    @Test
    public void testLatestByNullValues() throws Exception {
        // LATEST BY with NULL values in various types
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v_int, v_double, v_varchar),
                        v_int INT,
                        v_double DOUBLE,
                        v_varchar VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 1.0, 'ok'),
                    ('2024-01-02T00:00:00', 'A', NULL, NaN, NULL)
                    """);
            engine.releaseAllWriters();

            assertQuery("""
                    SELECT v_int, v_double, v_varchar
                    FROM t_latest_nulls WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            v_int\tv_double\tv_varchar
                            null\tnull\t
                            """);
        });
    }

    @Test
    public void testLatestByWithFilterExercisesFindLatestRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lb_filter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_lb_filter
                    SELECT dateadd('m', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100,
                           x::INT
                    FROM long_sequence(500)
                    """);
            engine.releaseAllWriters();

            assertSqlCursors(
                    "SELECT sym, price, qty FROM t_lb_filter WHERE sym = 'A' AND price < 5 " +
                            "LATEST ON ts PARTITION BY sym",
                    "SELECT /*+ no_covering */ sym, price, qty FROM t_lb_filter WHERE sym = 'A' AND price < 5 " +
                            "LATEST ON ts PARTITION BY sym"
            );

            assertQuery("SELECT sym, price, qty FROM t_lb_filter WHERE sym = 'A' AND price < 0 " +
                    "LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("sym\tprice\tqty\n");
        });
    }

    @Test
    public void testLatestByWithResidualFilterCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_filter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_filter VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0, 100),
                    ('2024-01-01T02:00:00', 'A', 5.0, 200),
                    ('2024-01-01T03:00:00', 'A', 20.0, 300),
                    ('2024-01-01T04:00:00', 'A', 3.0, 400),
                    ('2024-01-01T01:00:00', 'B', 15.0, 500),
                    ('2024-01-01T02:00:00', 'B', 25.0, 600)
                    """);
            engine.releaseAllWriters();

            // LATEST ON + WHERE filter: find latest row for 'A' where price > 10
            // Rows: 01:00 price=10 (no), 02:00 price=5 (no), 03:00 price=20 (YES), 04:00 price=3 (no)
            // Latest matching = 03:00 with price=20, qty=300
            assertQuery("SELECT price, qty FROM t_latest_filter WHERE sym = 'A' AND price > 10 LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price, qty
                                  filter: sym='A'
                            """)
                    .returns("""
                            price\tqty
                            20.0\t300
                            """);

            // LATEST ON + IN-list + filter: A→price=20@03:00, B→price=25@02:00
            assertQuery("SELECT sym, price, qty FROM t_latest_filter WHERE sym IN ('A', 'B') AND price > 10 LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice\tqty
                            A\t20.0\t300
                            B\t25.0\t600
                            """);
        });
    }

    @Test
    public void testLatestOnAllSymbolsMultiPartition() throws Exception {
        // LATEST ON without WHERE across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all_mp VALUES
                    ('2024-01-01T10:00:00', 'A', 1.0),
                    ('2024-01-01T11:00:00', 'B', 2.0),
                    ('2024-01-02T10:00:00', 'A', 3.0),
                    ('2024-01-02T11:00:00', 'C', 4.0),
                    ('2024-01-03T10:00:00', 'B', 5.0)
                    """);
            engine.releaseAllWriters();

            // A latest in day 2, B latest in day 3, C latest in day 2
            assertQuery("SELECT sym, val FROM t_latest_all_mp LATEST ON ts PARTITION BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tval
                            A\t3.0
                            C\t4.0
                            B\t5.0
                            """);
        });
    }

    @Test
    public void testLatestOnAllSymbolsNoWhere() throws Exception {
        // LATEST ON without WHERE clause — scans all symbols.
        // Reproduces issue where posting index caused empty results.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 2050.5),
                    ('2024-01-01T00:01:00', 'SILVER', 'CME', 24.3),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 2051.0),
                    ('2024-01-01T00:03:00', 'OIL', 'NYMEX', 78.2),
                    ('2024-01-01T00:04:00', 'SILVER', 'LME', 24.5),
                    ('2024-01-01T00:05:00', 'GOLD', 'CME', 2052.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON without WHERE — should return one row per symbol
            assertQuery("SELECT ts, sym, exchange FROM t_latest_all LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\texchange
                            2024-01-01T00:03:00.000000Z\tOIL\tNYMEX
                            2024-01-01T00:04:00.000000Z\tSILVER\tLME
                            2024-01-01T00:05:00.000000Z\tGOLD\tCME
                            """);

            // Also verify with covered columns in SELECT
            assertQuery("SELECT sym, price FROM t_latest_all LATEST ON ts PARTITION BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice
                            OIL\t78.2
                            SILVER\t24.5
                            GOLD\t2052.0
                            """);
        });
    }

    @Test
    public void testLatestOnAllSymbolsWal() throws Exception {
        // LATEST ON without WHERE via WAL
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all_wal VALUES
                    ('2024-01-01T00:00:00', 'X', 10.0),
                    ('2024-01-01T01:00:00', 'Y', 20.0),
                    ('2024-01-01T02:00:00', 'X', 30.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT sym, price FROM t_latest_all_wal LATEST ON ts PARTITION BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice
                            Y\t20.0
                            X\t30.0
                            """);
        });
    }

    @Test
    public void testLatestOnManyKeys() throws Exception {
        // Gap 4: LATEST ON with 5+ keys
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_5k (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_5k VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'C', 3.0),
                    ('2024-01-01T03:00:00', 'D', 4.0),
                    ('2024-01-01T04:00:00', 'E', 5.0),
                    ('2024-01-02T00:00:00', 'A', 10.0),
                    ('2024-01-02T01:00:00', 'C', 30.0),
                    ('2024-01-02T02:00:00', 'E', 50.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_latest_5k WHERE sym IN ('A','B','C','D','E') LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            2.0
                            30.0
                            4.0
                            50.0
                            """);
        });
    }

    @Test
    public void testLatestOnMultiKeyNonCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nc VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-02T00:00:00', 'A', 3.0, 30),
                    ('2024-01-02T01:00:00', 'C', 4.0, 40)
                    """);
            engine.releaseAllWriters();

            // LATEST ON with non-covered column — uses LatestByValues with non-cached cursors
            assertQuery("SELECT count() FROM (SELECT * FROM t_latest_nc WHERE sym IN ('A', 'B', 'C') LATEST ON ts PARTITION BY sym)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            3
                            """);
        });
    }

    @Test
    public void testLatestOnMultiPartitionManyCommits() throws Exception {
        // LATEST ON after multiple commits across 3 partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_3p (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_3p VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'A', 1.5),
                    ('2024-01-02T00:00:00', 'A', 2.0),
                    ('2024-01-03T00:00:00', 'A', 3.0),
                    ('2024-01-03T01:00:00', 'B', 4.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_latest_3p WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            3.0
                            """);
        });
    }

    @Test
    public void testMaterialiseFromParquetCleansUpTempFilesOnOpenAppendFailure() throws Exception {
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && name != null
                        && Utf8s.endsWithAscii(name, "price.d")) {
                    failArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_mat_fail (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_mat_fail
                    SELECT
                        dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4),
                        x::DOUBLE,
                        'V' || (x % 4)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_mat_fail CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            failArmed.set(true);
            execute("ALTER TABLE t_mat_fail ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_mat_fail'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\ntrue\n");

            execute("ALTER TABLE t_mat_fail RESUME WAL");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_mat_fail'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT indexed FROM table_columns('t_mat_fail') WHERE \"column\" = 'sym'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("indexed\ntrue\n");
            assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_mat_fail WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
        });
    }

    @Test
    public void testMetadataPersistenceAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_persist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // First read: verify metadata
            try (TableReader r = engine.getReader("t_persist")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                IntList covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(3, covering.size());
            }

            // Release all cached readers and writers to force reopening
            engine.releaseAllReaders();

            // Second read: metadata should persist
            try (TableReader r = engine.getReader("t_persist")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                IntList covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(3, covering.size());
                assertEquals(metadata.getColumnIndex("price"), covering.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), covering.getQuick(1));
            }
        });
    }

    @Test
    public void testMixedBitmapAndPostingIndexesInline() throws Exception {
        // Two SYMBOL columns on the same table with different index types:
        // a BITMAP-indexed column and a POSTING-indexed column with INCLUDE.
        // Validates that the 2-bit IndexType encoding and IndexFactory dispatch
        // route each column to the correct reader/writer.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed (
                        ts TIMESTAMP,
                        bsym SYMBOL INDEX,
                        psym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_mixed VALUES
                    ('2024-01-01T00:00:00', 'A', 'X', 1.0),
                    ('2024-01-01T01:00:00', 'B', 'Y', 2.0),
                    ('2024-01-01T02:00:00', 'A', 'X', 3.0),
                    ('2024-01-02T00:00:00', 'B', 'Y', 4.0),
                    ('2024-01-02T01:00:00', 'A', 'X', 5.0)
                    """);
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_mixed")) {
                TableReaderMetadata metadata = r.getMetadata();
                int bsymIdx = metadata.getColumnIndex("bsym");
                int psymIdx = metadata.getColumnIndex("psym");
                assertTrue(metadata.isColumnIndexed(bsymIdx));
                assertTrue(metadata.isColumnIndexed(psymIdx));
                assertEquals(IndexType.BITMAP, metadata.getColumnIndexType(bsymIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(psymIdx));
                assertFalse(metadata.getColumnMetadata(bsymIdx).isCovering());
                assertTrue(metadata.getColumnMetadata(psymIdx).isCovering());
            }

            // Each indexed column must answer queries via its own index type.
            assertQuery("SELECT bsym, count() FROM t_mixed WHERE bsym IN ('A','B') ORDER BY bsym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            bsym\tcount
                            A\t3
                            B\t2
                            """);
            assertQuery("SELECT psym, price FROM t_mixed WHERE psym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            psym\tprice
                            X\t1.0
                            X\t3.0
                            X\t5.0
                            """);
        });
    }

    @Test
    public void testMixedBitmapAndPostingIndexesPersistAcrossReopen() throws Exception {
        // The 2-bit IndexType field in column metadata flags must round-trip
        // through table close + reopen. A buggy encoding would lose the
        // BITMAP/POSTING distinction after restart.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed_reopen (
                        ts TIMESTAMP,
                        bsym SYMBOL INDEX,
                        psym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_mixed_reopen VALUES ('2024-01-01T00:00:00', 'A', 'X', 1.0)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (TableReader r = engine.getReader("t_mixed_reopen")) {
                TableReaderMetadata metadata = r.getMetadata();
                assertEquals(IndexType.BITMAP, metadata.getColumnIndexType(metadata.getColumnIndex("bsym")));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(metadata.getColumnIndex("psym")));
            }
        });
    }

    @Test
    public void testMixedBitmapAndPostingIndexesViaAlter() throws Exception {
        // Add the two index types via separate ALTER statements to a table
        // with no inline index, then verify both indexes service queries.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed_alter (
                        ts TIMESTAMP,
                        bsym SYMBOL,
                        psym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_mixed_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 'X', 1.0),
                    ('2024-01-01T01:00:00', 'B', 'Y', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'X', 3.0)
                    """);
            execute("ALTER TABLE t_mixed_alter ALTER COLUMN bsym ADD INDEX");
            execute("ALTER TABLE t_mixed_alter ALTER COLUMN psym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_mixed_alter")) {
                TableReaderMetadata metadata = r.getMetadata();
                assertEquals(IndexType.BITMAP, metadata.getColumnIndexType(metadata.getColumnIndex("bsym")));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(metadata.getColumnIndex("psym")));
            }
            assertQuery("SELECT count() FROM t_mixed_alter WHERE bsym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            2
                            """);
            assertQuery("SELECT psym, price FROM t_mixed_alter WHERE psym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            psym\tprice
                            X\t1.0
                            X\t3.0
                            """);
        });
    }

    @Test
    public void testMixedBitmapAndPostingIndexesWal() throws Exception {
        // Same mixed-index shape, but the table is WAL-enabled. Validates
        // that SequencerMetadata persists both index types through WAL replay.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed_wal (
                        ts TIMESTAMP,
                        bsym SYMBOL INDEX,
                        psym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_mixed_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 'X', 1.0),
                    ('2024-01-01T01:00:00', 'B', 'Y', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'X', 3.0)
                    """);
            drainWalQueue();

            try (TableReader r = engine.getReader("t_mixed_wal")) {
                TableReaderMetadata metadata = r.getMetadata();
                assertEquals(IndexType.BITMAP, metadata.getColumnIndexType(metadata.getColumnIndex("bsym")));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(metadata.getColumnIndex("psym")));
            }
            assertQuery("SELECT count() FROM t_mixed_wal WHERE bsym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            2
                            """);
            assertQuery("SELECT psym, price FROM t_mixed_wal WHERE psym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            psym\tprice
                            X\t1.0
                            X\t3.0
                            """);
        });
    }

    @Test
    public void testMultiKeyPageFrameMultiPartitionDoesNotLeakIndexReader() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mki_leak (
                        sym SYMBOL INDEX TYPE POSTING DELTA INCLUDE (v),
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_mki_leak
                    SELECT rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7',null),
                           rnd_double(),
                           timestamp_sequence(to_timestamp('2024-01-01','yyyy-MM-dd'), 1_800_000_000L)
                    FROM long_sequence(120)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_mki_leak CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            // max(const) over a 3-key IN where two literals don't resolve.
            // Routes through Async Group By -> MultiKeyCoveringPageFrameCursor
            // and produces one frame per partition that drains the heap.
            try (RecordCursorFactory f = select(
                    "SELECT max(112526.51::DECIMAL(9, 2)) AS a0 FROM t_mki_leak " +
                            "WHERE sym IN ('XQCE', 's5', 'YYYZZ'::VARCHAR)");
                 RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
            }
        });
    }

    /**
     * Regression for the MultiKey cursor's resume bug. The resume branch
     * of MultiKeyCoveringPageFrameCursor.nextImpl previously did not
     * advance currentKeyIdx after the parked CoveringRowCursor exhausted,
     * so the cursor kept reopening the SAME (key, partition) tuple and
     * producing the same frames forever. The page-frame buffers
     * accumulated until the RSS limit fired -- a 2-key multi-key query
     * with ~28 M rows per key tripped at 15.4 GiB on a 16 GB Mac.
     * <p>
     * To trigger the bug deterministically with small data we lower
     * {@code maxRowsPerFrame} via the {@code @TestOnly} setter so a
     * 200-row dataset is enough to force the multi-frame-per-key resume
     * path. Without the fix the test hangs or OOMs; with the fix it
     * returns the expected count.
     */
    @Test
    public void testMultiKeyResumeAdvancesKeyIndexAfterCursorExhausts() throws Exception {
        final int capForTest = 50;
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(capForTest);
        try {
            assertMemoryLeak(() -> {
                execute("""
                        CREATE TABLE t_mk_resume (
                            ts TIMESTAMP,
                            sym SYMBOL,
                            payload VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                // 200 rows per partition, alternating between two symbols.
                // With a 50-row per-frame cap, each (key, partition) tuple
                // produces 2 frames -- exactly the shape the bug exposed.
                execute("""
                        INSERT INTO t_mk_resume
                        SELECT
                            dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                            CASE WHEN x % 2 = 0 THEN 'K0' ELSE 'K1' END,
                            'payload-' || x
                        FROM long_sequence(200)
                        """);
                drainWalQueue();
                execute("ALTER TABLE t_mk_resume ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (payload)");
                drainWalQueue();

                // Multi-key COUNT — would loop forever on the bugged code
                // because the resume branch never advances past K0 once
                // its parked cursor exhausts; the test's surefire timeout
                // would surface that as a hang.
                assertQuery("SELECT count() FROM t_mk_resume WHERE sym IN ('K0','K1')")
                        .noRandomAccess()
                        .expectSize()
                        .noLeakCheck()
                        .returns("count\n200\n");

                // Per-key counts also exercise the resume path on both
                // keys and validate that the per-key totals stay correct
                // (without the fix some frames would carry duplicate K0
                // rows from re-iteration, inflating K0 and missing K1).
                assertQuery("SELECT sym, count() c FROM t_mk_resume WHERE sym IN ('K0','K1') GROUP BY sym ORDER BY sym")
                        .expectSize()
                        .noLeakCheck()
                        .returns("sym\tc\nK0\t100\nK1\t100\n");

                // Covered VARCHAR read through the multi-key cursor: the
                // 200 unique payloads must come back exactly once each.
                assertQuery("SELECT count_distinct(payload::string) c FROM t_mk_resume WHERE sym IN ('K0','K1')")
                        .noRandomAccess()
                        .expectSize()
                        .noLeakCheck()
                        .returns("c\n200\n");
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testMultiPartitionAlterAndQuery() throws Exception {
        // Multiple partitions, ALTER TABLE, then query across all
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_alter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_multi_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-02T00:00:00', 'A', 20.0),
                    ('2024-01-03T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // ALTER: add column
            execute("ALTER TABLE t_multi_alter ADD COLUMN extra INT");

            // Insert into new partition with new column
            execute("""
                    INSERT INTO t_multi_alter (ts, sym, price, extra) VALUES
                    ('2024-01-04T00:00:00', 'A', 40.0, 999)
                    """);
            engine.releaseAllWriters();

            // Query should return all A's across all partitions
            assertQuery("SELECT price FROM t_multi_alter WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            20.0
                            30.0
                            40.0
                            """);
        });
    }

    @Test
    public void testMultiVarcharIncludeFsst() throws Exception {
        // Two VARCHAR columns in INCLUDE — exercises per-includeIdx FSST cache
        // where each column has a different symbol table.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_vc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, info),
                        name VARCHAR,
                        info VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_multi_vc
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           rnd_str('name_alpha_one','name_beta_two','name_gamma_three'),
                           rnd_str('info_first_item','info_second_item','info_third_item')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, info FROM t_multi_vc WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name, info
                                  filter: sym='A'
                            """);

            assertQuery("SELECT COUNT(*) FROM t_multi_vc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n733\n");
        });
    }

    @Test
    public void testNoCoveringHint_DataCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_data VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            // Both paths should produce the same results
            String expected = """
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """;
            assertQuery("SELECT price, qty FROM t_hint_data WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ price, qty FROM t_hint_data WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testNoCoveringHint_DisablesCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Without hint: CoveringIndex
            assertQuery("SELECT price FROM t_hint WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """);

            // With no_covering hint: no CoveringIndex
            assertQuery("SELECT /*+ no_covering */ price FROM t_hint WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
        });
    }

    @Test
    public void testNoCoveringHint_InList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT /*+ no_covering */ price FROM t_hint_in WHERE sym IN ('A', 'B')")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex");
        });
    }

    @Test
    public void testNoCoveringHint_LatestBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'B', 30.5)
                    """);
            engine.releaseAllWriters();

            // Data correctness
            String expected = """
                    price
                    20.5
                    """;
            assertQuery("SELECT price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_covering */ price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noLeakCheck()
                    .withPlanNotContaining("CoveringIndex")
                    .returns(expected);
        });
    }

    @Test
    public void testNoIndexHint_DataCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_data VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """;
            assertQuery("SELECT price, qty FROM t_noidx_data WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_index */ price, qty FROM t_noidx_data WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testNoIndexHint_DisablesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 30.5)
                    """);
            engine.releaseAllWriters();

            // Without hint: CoveringIndex
            assertQuery("SELECT price FROM t_noidx WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """);

            // With no_index hint: full table scan, no index at all
            assertQuery("SELECT /*+ no_index */ price FROM t_noidx WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index");
        });
    }

    @Test
    public void testNoIndexHint_ImpliesNoCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_impl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_impl VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            // no_index implies no_covering — neither CoveringIndex nor SymbolIndex should appear
            assertQuery("SELECT /*+ no_index */ price FROM t_noidx_impl WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("CoveringIndex", "Index");
        });
    }

    @Test
    public void testNoIndexHint_InList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    price
                    10.5
                    20.5
                    """;
            assertQuery("SELECT price FROM t_noidx_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns(expected);
            // no_index disables both covering and bitmap index
            assertQuery("SELECT /*+ no_index */ price FROM t_noidx_in WHERE sym IN ('A', 'B')")
                    .noLeakCheck()
                    .withPlanNotContaining("Index")
                    .returns(expected);
        });
    }

    @Test
    public void testNoIndexHint_LatestBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'B', 30.5)
                    """);
            engine.releaseAllWriters();

            // no_index disables index-based LATEST BY
            assertQuery("SELECT /*+ no_index */ * FROM t_noidx_latest LATEST ON ts PARTITION BY sym")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Indexed");

            // Data correctness for single-key LATEST ON
            String expected = """
                    ts\tsym\tprice
                    2024-01-01T01:00:00.000000Z\tA\t20.5
                    """;
            assertQuery("SELECT * FROM t_noidx_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_index */ * FROM t_noidx_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testNoIndexHint_WithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'A', 60.5, 200),
                    ('2024-01-01T02:00:00', 'B', 70.5, 300),
                    ('2024-01-01T03:00:00', 'A', 80.5, 400)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    price\tqty
                    60.5\t200
                    80.5\t400
                    """;
            assertQuery("SELECT price, qty FROM t_noidx_filt WHERE sym = 'A' AND price > 50")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT /*+ no_index */ price, qty FROM t_noidx_filt WHERE sym = 'A' AND price > 50")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testNonCoveringReaderReturnsRegularCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "plain_cursor_test";
                int plen = path.size();

                // Write index without covering
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    writer.add(0, 0);
                    writer.add(0, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                    // Cursor should still implement CoveringRowCursor but isCoveredAvailable(0) returns false
                    assertTrue(cursor instanceof CoveringRowCursor);
                    assertFalse(((CoveringRowCursor) cursor).isCoveredAvailable(0));

                    // Regular iteration should work
                    assertTrue(cursor.hasNext());
                    assertEquals(0, cursor.next());
                    assertTrue(cursor.hasNext());
                    assertEquals(1, cursor.next());
                    assertFalse(cursor.hasNext());
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testNonCoveringTableUnchanged() throws Exception {
        // POSTING index without INCLUDE is a plain, non-covering posting index.
        // The timestamp auto-include only rounds out an explicit INCLUDE set, so
        // a bare INDEX TYPE POSTING must carry no covering column list.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE plain (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("plain")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                assertFalse(metadata.getColumnMetadata(symIdx).isCovering());
                assertNull(metadata.getColumnMetadata(symIdx).getCoveringColumnIndices());
            }
        });
    }

    @Test
    public void testO3CommitSealFailureMarksWriterDistressed() throws Exception {
        // Regression test for #8: when sealPostingIndexesForO3Partitions
        // throws (e.g. an I/O failure on a covering column file mmap),
        // finishO3Commit must mark the writer as distressed before
        // rethrowing. Without that, the writer pool keeps handing the same
        // half-committed writer to subsequent operations.
        // The seal opens covering .d column files via openRO and then mmaps
        // them. Track the fd opened for our covering column and fail its
        // mmap so a real CairoException propagates from the seal (failing
        // openRO only flips the seal's internal "mapped=false" flag and
        // doesn't surface the failure to finishO3Commit).
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final long[] targetFd = {-1};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failArmed.get() && fd == targetFd[0]) {
                    return io.questdb.std.FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (failArmed.get() && fd != -1 && name != null
                        && Utf8s.endsWithAscii(name, "price.d")) {
                    targetFd[0] = fd;
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_seal_fail (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal_fail VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try (TableWriter w = TestUtils.getWriter(engine, "t_seal_fail")) {
                // O3 row inserted into day 1 after day 2 already exists —
                // commit must seal the affected partition's posting index,
                // and that seal will fail because openRO("...d") returns -1.
                String tsStr = "2024-01-01T00:30:00.000000Z";
                TableWriter.Row r = w.newRow(io.questdb.cairo.MicrosTimestampDriver.INSTANCE.parseFloor(tsStr, 0, tsStr.length()));
                r.putSym(1, "C");
                r.putDouble(2, 99.0);
                r.append();
                try {
                    w.commit();
                    fail("expected seal failure to surface");
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable ignore) {
                    // expected
                }
                assertTrue("writer must be distressed after a seal failure",
                        w.isDistressed());
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testO3CommitSealFailureOnOpenROMarksWriterDistressed() throws Exception {
        // Red test for TableWriter.sealPostingIndexForPartition (around
        // TableWriter.java:10729). When openRO on a covering column file
        // returns -1, the seal silently sets mapped=false and skips the
        // entire seal block including indexer.mergeTentativeIntoActiveIfAny()
        // and rebuildSidecars(). finishO3Commit catches throws from
        // sealPostingIndexesForO3Partitions and marks the writer distressed,
        // but a silent mapped=false produces no throw — the writer stays
        // healthy from the pool's perspective while the partition's posting
        // index is half-rebuilt. Subsequent operations get handed the same
        // half-committed writer.
        //
        // The companion test testO3CommitSealFailureMarksWriterDistressed
        // validates the mmap-failure path (which does throw) and was the
        // workaround the author noted for this exact bug. After the fix,
        // openRO failure must propagate the same way.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failArmed.get() && name != null && Utf8s.endsWithAscii(name, "price.d")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_seal_fail_open (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal_fail_open VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try (TableWriter w = TestUtils.getWriter(engine, "t_seal_fail_open")) {
                String tsStr = "2024-01-01T00:30:00.000000Z";
                TableWriter.Row r = w.newRow(io.questdb.cairo.MicrosTimestampDriver.INSTANCE.parseFloor(tsStr, 0, tsStr.length()));
                r.putSym(1, "C");
                r.putDouble(2, 99.0);
                r.append();
                try {
                    w.commit();
                    fail("expected seal failure to surface");
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable ignore) {
                    // expected
                }
                assertTrue("writer must be distressed after a covering openRO failure",
                        w.isDistressed());
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testO3MultiPartitionSidecarRebuild() throws Exception {
        // Gap 15/16: O3 affecting multiple partitions. Verifies sidecar rebuild
        // handles multiple O3-affected partitions correctly.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Insert day 3 first
            execute("""
                    INSERT INTO t_o3_multi VALUES
                    ('2024-01-03T00:00:00', 'A', 30.0),
                    ('2024-01-03T12:00:00', 'B', 31.0)
                    """);
            drainWalQueue();
            // O3: insert day 1 and day 2 (both before day 3)
            execute("""
                    INSERT INTO t_o3_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', 'B', 11.0),
                    ('2024-01-02T00:00:00', 'A', 20.0),
                    ('2024-01-02T12:00:00', 'B', 21.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // All 3 partitions should return correct covered values
            assertQuery("SELECT price FROM t_o3_multi WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            20.0
                            30.0
                            """);

            assertQuery("SELECT price FROM t_o3_multi WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            11.0
                            21.0
                            31.0
                            """);
        });
    }

    @Test
    public void testO3PreMappedConfigureCoveringPreservesTimestampIndex() throws Exception {
        // Regression: PostingIndexWriter's pre-mapped configureCovering
        // overload (used exclusively by TableWriter.finishO3Commit's reseal
        // path at TableWriter.java:10760) used to hard-reset
        // timestampColumnIndex to -1, dropping the linear-prediction codec
        // for the designated-timestamp covering on every O3-affected
        // partition. compressSidecarBlock then routed the timestamp
        // through CoveringCompressor.compressLongs (generic delta-FoR)
        // instead of compressLongsLinearPred, silently disabling the
        // headline optimisation.
        //
        // Verify the writer-space timestamp index is now propagated
        // through both the LongList and long[] @TestOnly overloads.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_o3_ts";
                int plen = path.size();
                int rowCount = 8;
                long tsAddr = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    long base = 1_700_000_000_000_000L;
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(tsAddr + (long) i * Long.BYTES, base + i * 1_000_000L);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(
                            configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        // Long[] @TestOnly with explicit timestamp slot.
                        writer.configureCovering(
                                new long[]{tsAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.TIMESTAMP},
                                1,
                                2 // writer-space designated-timestamp index
                        );
                        assertEquals("pre-mapped overload must preserve timestamp index",
                                2, writer.getTimestampColumnIndex());

                        // The compat 6-arg @TestOnly overload (no timestamp
                        // slot) keeps its old "no covered timestamp"
                        // semantics: index is reset to -1.
                        writer.configureCovering(
                                new long[]{tsAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.TIMESTAMP},
                                1
                        );
                        assertEquals("compat overload defaults timestamp index to -1",
                                -1, writer.getTimestampColumnIndex());
                    }
                    // Restore plen for any subsequent path reuse.
                    path.trimTo(plen);
                } finally {
                    Unsafe.free(tsAddr, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testO3WithNullCoveredValues() throws Exception {
        // Gap 18: O3 with NULL values in covered columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-02T00:00:00', 'A', 20.0)
                    """);
            drainWalQueue();
            // O3: insert day 1 with NULL price
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T00:00:00', 'A', NULL)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_o3_null WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            null
                            20.0
                            """);
        });
    }

    @Test
    public void testO3WithSymbolIncludeColumn() throws Exception {
        // Gap 17 extended: O3 with SYMBOL INCLUDE columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_sym VALUES
                    ('2024-01-02T00:00:00', 'A', 'hot', 20.0)
                    """);
            drainWalQueue();
            // O3: insert day 1
            execute("""
                    INSERT INTO t_o3_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 10.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertQuery("SELECT sym, tag, price FROM t_o3_sym WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\ttag\tprice
                            A\tcold\t10.0
                            A\thot\t20.0
                            """);
        });
    }

    @Test
    public void testPageFrameAggregationUuidIpv4() throws Exception {
        // Page frame aggregation for UUID and IPv4 types
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_uuid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, addr),
                        id UUID,
                        addr IPv4
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_uuid VALUES
                    ('2024-01-01T00:00:00', 'A', '11111111-1111-1111-1111-111111111111', '1.2.3.4'),
                    ('2024-01-01T01:00:00', 'A', '22222222-2222-2222-2222-222222222222', '5.6.7.8'),
                    ('2024-01-01T02:00:00', 'B', '33333333-3333-3333-3333-333333333333', '9.0.1.2')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count(*) AS count FROM t_pf_uuid WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testPageFrameBinaryGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Enough rows to exercise page frame growth (> INITIAL_CAPACITY=4096)
            execute("""
                    INSERT INTO t_pf_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 60_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        (x % 100)::DOUBLE
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // GROUP BY exercises the page frame path
            assertQuery("SELECT sym, avg(price) avg FROM t_pf_bin WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tavg
                            A\t49.0
                            B\t50.0
                            """);
        });
    }

    @Test
    public void testPageFrameCursor_Aggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_agg (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Generate 200 rows with 3 symbols (A/B/C cycling via modulo)
            execute("""
                    INSERT INTO t_pf_agg
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00'),
                           CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END,
                           10.0 + x * 0.1,
                           x::INT
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Aggregation should produce correct results via either path
            assertSqlCursors(
                    "SELECT count() FROM t_pf_agg WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ count() FROM t_pf_agg WHERE sym = 'A'"
            );
        });
    }

    @Test
    public void testPageFrameCursor_ArrayWrite() throws Exception {
        // Drives ARRAY column path of the page-frame cursor through a SELECT
        // that resolves to CoveringIndexRecordCursorFactory and then asks for
        // its PageFrameCursor directly. writeCoveredRow's switch lacks an arm
        // for ColumnType.ARRAY, so the row loop falls into default -> {} and
        // never writes aux/data. fillFrameForKey then takes the generic
        // includeIdx >= 0 branch and pins auxPageAddresses[q] = 0,
        // auxPageSizes[q] = 0, pageSizes[q] = 0 (TYPE_SIZE[ARRAY] is 0).
        // A consumer reading the frame sees an aux address of 0 and decodes
        // empty arrays for every row.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals),
                        vals DOUBLE[]
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_arr
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        ARRAY[x::DOUBLE, (x + 1)::DOUBLE, (x + 2)::DOUBLE]
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, vals FROM t_pf_arr WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                long rows = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    long count = f.getPartitionHi() - f.getPartitionLo();
                    rows += count;
                    // 'vals' is column index 1 in the projection (sym is 0) and is
                    // a covered INCLUDE column. Under metadata-only single-key frame
                    // production the covered values are NOT materialized at frame
                    // production -- the worker covered arm decodes them on navigate
                    // -- so the production page addresses/sizes read directly off the
                    // frame here are legitimately PLACEHOLDER zeroes. (Same precedent
                    // as testAddressCacheStoresCoveredMetadata in
                    // CoveringIndexParallelDecodeTest.) This test never navigates the
                    // frame, so it only ever observes those production placeholders.
                    assertEquals("covered ARRAY aux page address is a production placeholder (0)",
                            0L, f.getAuxPageAddress(1));
                    assertEquals("covered ARRAY aux page size is a production placeholder (0)",
                            0L, f.getAuxPageSize(1));
                    assertEquals("covered ARRAY data page size is a production placeholder (0)",
                            0L, f.getPageSize(1));
                }
                assertEquals(10, rows);
            }
        });
    }

    @Test
    public void testPageFrameCursor_BinaryWrite() throws Exception {
        // Drives writeBinaryToFrame path of the page-frame cursor.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        rnd_bin(8, 16, 0)
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, payload FROM t_pf_bin WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                long rows = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    rows += f.getPartitionHi() - f.getPartitionLo();
                }
                assertEquals(10, rows);
            }
        });
    }

    @Test
    public void testPageFrameCursor_GrowFrameBuffers() throws Exception {
        // Drives growFrameBuffers + ensureVarDataCapacity by inserting more
        // than INITIAL_CAPACITY (4096) rows for one symbol with VARCHAR
        // payload large enough to overflow the initial var-data buffer.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_grow (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, payload),
                        name VARCHAR,
                        payload STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 5000 rows for 'A', each with a 64-byte varchar (320KB total var
            // data, 5x the initial 64KB capacity) and a 32-char string.
            execute("""
                    INSERT INTO t_pf_grow
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        rpad('v', 64, 'x'),
                        rpad('s', 32, 'y')
                    FROM long_sequence(5000)
                    """);
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, name, payload FROM t_pf_grow WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                long rows = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    rows += f.getPartitionHi() - f.getPartitionLo();
                }
                assertEquals(5000, rows);
            }
        });
    }

    @Test
    public void testPageFrameCursor_LifecycleAndToTop() throws Exception {
        // Drive the page frame cursor directly to exercise lifecycle methods
        // that ordinary aggregation queries do not invoke: size(),
        // supportsSizeCalculation(), calculateSize(), getRemainingRowsInInterval(),
        // toTop(), and getSymbolTable()/newSymbolTable() on a populated cursor.
        // The trailing assertQueryNoLeakCheck cross-checks the record cursor
        // path (different cursor implementation, same factory) and exercises
        // RecordCursor.toTop()/getSymbolTable()/newSymbolTable() through the
        // standard test framework helpers.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_lifecycle (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_lifecycle VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'A', 11.0),
                    ('2024-01-01T02:00:00', 'B', 20.0),
                    ('2024-01-01T03:00:00', 'A', 12.0)
                    """);
            engine.releaseAllWriters();

            // sym is queryColToIncludeIdx[0] == -1 (the indexed column itself);
            // querying sym + price gives us a SYMBOL column at metadata index
            // 0 for getSymbolTable/newSymbolTable assertions and exercises the
            // symbol-buffer path of fillFrameForKey.
            try (var factory = select("SELECT sym, price FROM t_pf_lifecycle WHERE sym = 'A'")) {
                assertTrue(factory.supportsPageFrameCursor());
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    assertEquals(-1, cursor.size());
                    assertFalse(cursor.supportsSizeCalculation());
                    assertEquals(0, cursor.getRemainingRowsInInterval());

                    RecordCursor.Counter counter = new RecordCursor.Counter();
                    counter.add(123);
                    cursor.calculateSize(counter);
                    assertEquals("calculateSize is documented as no-op", 123, counter.get());

                    StaticSymbolTable st0 = cursor.getSymbolTable(0);
                    assertNotNull("populated cursor must surface symbol map for sym col", st0);
                    SymbolTable st0New = cursor.newSymbolTable(0);
                    assertNotNull("populated cursor must build a fresh symbol table", st0New);

                    int frames = 0;
                    long rows = 0;
                    PageFrame f;
                    while ((f = cursor.next(0)) != null) {
                        frames++;
                        rows += f.getPartitionHi() - f.getPartitionLo();
                    }
                    assertEquals("expected 3 rows for sym='A'", 3, rows);

                    // toTop() must let us re-iterate and produce the same frame count
                    cursor.toTop();
                    int frames2 = 0;
                    long rows2 = 0;
                    while ((f = cursor.next(0)) != null) {
                        frames2++;
                        rows2 += f.getPartitionHi() - f.getPartitionLo();
                    }
                    assertEquals(frames, frames2);
                    assertEquals(rows, rows2);
                }
            }

            // Record cursor pass: assertQueryNoLeakCheck opens a fresh cursor
            // twice and exercises toTop, the variable-column helper, and
            // testSymbolAPI (which checks that getInt on the indexed sym
            // column returns its resolved key).
            assertQuery("SELECT sym, price FROM t_pf_lifecycle WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            sym\tprice
                            A\t10.0
                            A\t11.0
                            A\t12.0
                            """);
        });
    }

    @Test
    public void testPageFrameCursor_MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T01:00:00', 'B', 4.0),
                    ('2024-01-03T00:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT val FROM t_pf_multi WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val
                            1.0
                            3.0
                            5.0
                            """);
        });
    }

    @Test
    public void testPageFrameCursor_OfEmptyMultiKey() throws Exception {
        // Multi-key page frame cursor: an IN-list with all values resolving to
        // VALUE_NOT_FOUND must produce no frames. The cursor is still wired to
        // the table reader so symbol-table lookups remain valid for upstream
        // operators that probe before iteration.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_ofempty_mk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_pf_ofempty_mk VALUES ('2024-01-01T00:00:00', 'A', 10.0)");
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, price FROM t_pf_ofempty_mk WHERE sym IN ('NOPE1', 'NOPE2')")) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    assertNull(cursor.next(0));
                    cursor.toTop();
                    assertNull(cursor.next(0));
                }
            }

            assertQuery("SELECT sym, price FROM t_pf_ofempty_mk WHERE sym IN ('NOPE1', 'NOPE2')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("sym\tprice\n");
        });
    }

    @Test
    public void testPageFrameCursor_OfEmptySingleKeyKeepsSymbolTables() throws Exception {
        // When the WHERE key resolves to VALUE_NOT_FOUND, the page frame cursor
        // produces no frames but must still expose working symbol tables. Upstream
        // operators (e.g. ORDER BY on a SYMBOL column) probe getSymbolTable() on
        // the base cursor during init, before any iteration; returning null there
        // would break that contract.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_ofempty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_pf_ofempty VALUES ('2024-01-01T00:00:00', 'A', 10.0)");
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, price FROM t_pf_ofempty WHERE sym = 'NOPE'")) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    assertNotNull("getSymbolTable must be usable on an empty cursor", cursor.getSymbolTable(0));
                    assertNotNull("newSymbolTable must be usable on an empty cursor", cursor.newSymbolTable(0));
                    assertNull(cursor.next(0));
                    cursor.toTop();
                    assertNull(cursor.next(0));
                }
            }

            // Record cursor pass over the empty result.
            assertQuery("SELECT sym, price FROM t_pf_ofempty WHERE sym = 'NOPE'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("sym\tprice\n");
        });
    }

    @Test
    public void testPageFrameCursor_SetScanProfileReachesReader() throws Exception {
        // The covering page-frame cursor now implements TablePageFrameCursor (it was a
        // bare PageFrameCursor before), so setScanProfile() is no longer a silent no-op:
        // it delegates to the live TableReader exposed by getTableReader(). Drive the
        // cursor directly and assert the profile actually lands on the reader.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_profile (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_profile
                    SELECT '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L, 'A', x * 1.5
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            // The query wraps the covering factory in a SelectedRecordCursorFactory;
            // unwrap to the covering factory so we drive its CoveringPageFrameCursor
            // directly (that is the cursor whose getTableReader/setScanProfile changed).
            try (RecordCursorFactory factory = select("SELECT sym, price FROM t_pf_profile WHERE sym = 'A'")) {
                RecordCursorFactory covering = factory instanceof CoveringIndexRecordCursorFactory ? factory : factory.getBaseFactory();
                assertTrue("expected a covering-index factory, got " + factory.getClass().getSimpleName(),
                        covering instanceof CoveringIndexRecordCursorFactory);

                try (PageFrameCursor cursor = covering.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    assertTrue("covering page-frame cursor must be a TablePageFrameCursor",
                            cursor instanceof TablePageFrameCursor);

                    TableReader reader = ((TablePageFrameCursor) cursor).getTableReader();
                    assertNotNull("getTableReader must expose the live reader", reader);
                    assertEquals(ReaderScanProfile.DEFAULT, reader.getScanProfile());

                    // setScanProfile must delegate through to the reader, not no-op.
                    cursor.setScanProfile(ReaderScanProfile.SEQUENTIAL_CACHED);
                    assertEquals(ReaderScanProfile.SEQUENTIAL_CACHED, reader.getScanProfile());

                    // A second profile proves the delegation is live, not a one-shot.
                    cursor.setScanProfile(ReaderScanProfile.SEQUENTIAL_EVICT);
                    assertEquals(ReaderScanProfile.SEQUENTIAL_EVICT, reader.getScanProfile());
                }
            }
        });
    }

    @Test
    public void testPageFrameCursor_SupportsPageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_support (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_support VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // Verify the factory reports page frame support
            assertQuery("SELECT val FROM t_pf_support WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val
                            1.0
                            """);
        });
    }

    @Test
    public void testPageFrameOfEmptyAllPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_empty
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           rnd_double() * 100
                    FROM long_sequence(200)
                    """);
            drainWalQueue();

            assertQuery("SELECT count() FROM t_pf_empty WHERE sym = 'NEVER_INSERTED' AND price > 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n0\n");

            assertQuery("SELECT count() FROM t_pf_empty WHERE sym IN ('NEVER','UNKNOWN') AND price > 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testPageFrameToTopRecursive() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_toTop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_toTop
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           CASE WHEN x <= 100 THEN 'A' ELSE 'B' END,
                           ((x % 10) * 10)::DOUBLE
                    FROM long_sequence(200)
                    """);
            drainWalQueue();

            assertQuery("SELECT count() FROM (" +
                    "SELECT a.price FROM (SELECT price FROM t_pf_toTop WHERE sym = 'A' AND price > 50) a " +
                    "JOIN (SELECT price FROM t_pf_toTop WHERE sym = 'B' AND price > 50) b ON a.price = b.price)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n400\n");
        });
    }

    @Test
    public void testParallelKeyedGroupByOverCoveringIndexUnderSelectedRecord() throws Exception {
        // Regression: a parallel keyed GROUP BY whose base is CoveringIndex wrapped by
        // SelectedRecord (e.g. via a CTE) crashed with ClassCastException because
        // CoveringPageFrameCursor was a plain PageFrameCursor while
        // SelectedRecordCursorFactory casts to TablePageFrameCursor. The fix
        // promotes CoveringPageFrameCursor to TablePageFrameCursor.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bug9 (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (k, v),
                        k SYMBOL,
                        v INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_bug9 VALUES
                        ('a', 'k1', 1, 0),
                        ('b', 'k2', 2, 1_000_000),
                        (null, 'k1', 3, 2_000_000),
                        (null, 'k2', 4, 3_000_000),
                        (null, 'k1', 5, 4_000_000)
                    """);
            drainWalQueue();

            // The CTE introduces a SelectedRecord layer above the WHERE-driven
            // CoveringIndex factory; the constant aggregate (avg(-1)) keeps the
            // group-by on the Async (parallel) keyed path rather than the
            // vectorised one.
            String q = "WITH cte0 AS (SELECT * FROM t_bug9) "
                    + "SELECT t0.k AS e0, avg(-1) AS a0 FROM cte0 t0 WHERE sym IS NULL "
                    + "ORDER BY e0";
            assertQuery(q)
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [e0]
                                Async Group By workers: 1
                                  keys: [e0]
                                  values: [avg(-1)]
                                  filter: null
                                    SelectedRecord
                                        SelectedRecord
                                            CoveringIndex on: sym with: k
                                              filter: sym=null
                            """);
            assertQuery(q)
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            e0\ta0
                            k1\t-1.0
                            k2\t-1.0
                            """);
        });
    }

    @Test
    public void testPciFileFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "pci_format_test";
                int plen = path.size();

                int rowCount = 4;
                long colAddrDouble = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                long colAddrInt = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddrDouble + (long) i * Double.BYTES, i * 1.5);
                        Unsafe.putInt(colAddrInt + (long) i * Integer.BYTES, i * 10);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddrDouble, colAddrInt},
                                new long[]{0, 0},
                                new int[]{3, 2},  // DOUBLE shift=3, INT shift=2
                                new int[]{1, 2},  // column indices
                                new int[]{ColumnType.DOUBLE, ColumnType.INT},
                                2
                        );

                        writer.add(0, 0);
                        writer.add(0, 1);
                        writer.add(1, 2);
                        writer.add(1, 3);
                        writer.setMaxValue(3);
                        writer.commit();
                    }

                    // Verify .pci file content
                    FilesFacade ff = configuration.getFilesFacade();
                    Path pciPath = path.trimTo(plen);
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(pciPath, name, COLUMN_NAME_TXN_NONE)));

                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue(liveSealTxn >= 0);
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 1, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                } finally {
                    Unsafe.free(colAddrDouble, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(colAddrInt, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPostingIndexReaderWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_reader_test";
                int plen = path.size();

                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    // Populate column data: row i has value 10.0 * (i + 1)
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    // Write index with covering
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        // key 0 -> rows 0, 3, 6
                        writer.add(0, 0);
                        writer.add(0, 3);
                        writer.add(0, 6);
                        // key 1 -> rows 1, 4
                        writer.add(1, 1);
                        writer.add(1, 4);
                        writer.setMaxValue(6);
                        writer.commit();
                    }

                    // Read back with covering cursor — verify both row IDs and covered values
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));


                        // key 0: rows 0, 3, 6 -> values 10.0, 40.0, 70.0
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(10.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(40.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(6, cc.next());
                        assertEquals(70.0, cc.getCoveredDouble(0), 0.001);

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // key 1: rows 1, 4 -> values 20.0, 50.0
                        cursor = reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.isCoveredAvailable(0));


                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(20.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(4, cc.next());
                        assertEquals(50.0, cc.getCoveredDouble(0), 0.001);

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPostingIndexRowIdsMatchBitmap() throws Exception {
        assertMemoryLeak(() -> {
            // Create two tables — same data, different index types
            execute("""
                    CREATE TABLE cmd_posting (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX TYPE POSTING,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    CREATE TABLE cmd_bitmap2 (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            // Generate identical data into both tables
            execute("""
                    INSERT INTO cmd_posting
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(500)
                    """);
            execute("INSERT INTO cmd_bitmap2 SELECT * FROM cmd_posting");
            engine.releaseAllWriters();

            // Compare row counts — both index types must return the same count
            long bitmapCount;
            long postingCount;

            try (var f = select("SELECT count() FROM cmd_bitmap2 WHERE symbol = 'CL'");
                 var c = f.getCursor(sqlExecutionContext)) {
                c.hasNext();
                bitmapCount = c.getRecord().getLong(0);
            }
            try (var f = select("SELECT count() FROM cmd_posting WHERE symbol = 'CL'");
                 var c = f.getCursor(sqlExecutionContext)) {
                c.hasNext();
                postingCount = c.getRecord().getLong(0);
            }
            assertEquals("count mismatch for symbol='CL'", bitmapCount, postingCount);
        });
    }

    @Test
    public void testPostingIndexSplitPageFrame() throws Exception {
        // Regression: posting index cursor returned partition-absolute row IDs
        // instead of frame-relative (bitmap subtracts minValue, posting didn't).
        // With >1M rows in a single partition, the page frame splits and the
        // second frame has partitionLo>0. The absolute row IDs double-count
        // partitionLo against the already-offset page addresses.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_split_frame (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL
                    """);
            // Insert 1.5M rows — enough to split into 2 frames (pageFrameMaxRows=1M)
            // sym alternates between A and B so both keys span both frames
            execute("""
                    INSERT INTO t_split_frame
                    SELECT
                        timestamp_sequence('2024-01-01', 1_000_000) AS ts,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END AS sym,
                        x * 1.5 AS val
                    FROM long_sequence(1_500_000)
                    """);
            engine.releaseAllWriters();

            // Verify we have enough rows to split into multiple page frames
            try (TableReader reader = engine.getReader("t_split_frame")) {
                assertEquals(1_500_000, reader.size());
            }

            // Indexed scan must return correct data from both frames.
            // Before the fix, frame 2 (partitionLo=1M) would read wrong data.
            // A = even x (2,4,...,1500000), avg(x)=750001, avg(val)=750001*1.5
            assertQuery("SELECT count(), avg(val) FROM t_split_frame WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count\tavg
                            750000\t1125001.5
                            """);

            // B = odd x (1,3,...,1499999), avg(x)=750000, avg(val)=750000*1.5
            assertQuery("SELECT count(), avg(val) FROM t_split_frame WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count\tavg
                            750000\t1125000.0
                            """);
        });
    }

    @Test
    public void testPostingIndexWithO3Inserts() throws Exception {
        // Posting index (without covering) works correctly with O3
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0),
                    ('2024-01-01T03:00:00', 'B', 30.0),
                    ('2024-01-01T05:00:00', 'A', 50.0)
                    """);

            // Out-of-order inserts
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 5.0),
                    ('2024-01-01T02:00:00', 'A', 20.0),
                    ('2024-01-01T04:00:00', 'B', 40.0)
                    """);
            drainWalQueue();

            // All A's returned in ts order via posting index + column reads
            assertQuery("SELECT price FROM t_o3 WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price
                            5.0
                            10.0
                            20.0
                            50.0
                            """);

            assertQuery("SELECT price FROM t_o3 WHERE sym = 'B'")
                    .noLeakCheck()
                    .returns("""
                            price
                            30.0
                            40.0
                            """);
        });
    }

    @Test
    public void testPostingIndexWithVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        name VARCHAR,
                        val INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 1),
                    ('2024-01-01T01:00:00', 'B', 'bob', 2),
                    ('2024-01-01T02:00:00', 'A', 'alice2', 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, val FROM t_varchar WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            name\tval
                            alice\t1
                            alice2\t3
                            """);
        });
    }

    @Test
    public void testPostingIndexWithVarcharLargePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar_big (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        name VARCHAR,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            // Insert enough data to create substantial partitions
            for (int h = 0; h < 3; h++) {
                StringBuilder sb = new StringBuilder("INSERT INTO t_varchar_big VALUES ");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) sb.append(',');
                    String sym = switch (i % 5) {
                        case 0 -> "A";
                        case 1 -> "B";
                        case 2 -> "C";
                        case 3 -> "D";
                        default -> "E";
                    };
                    sb.append(String.format("('2024-01-01T%02d:%02d:%02d', '%s', 'name_%d_%d', %d.%d)",
                            h, i / 60 % 60, i % 60, sym, h, i, 100 + h, i));
                }
                execute(sb.toString());
            }
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM t_varchar_big WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            6000
                            """);

            // Access varchar column via index — verify no crash with var-size column
            assertQuery("SELECT count() FROM (SELECT name, val FROM t_varchar_big WHERE sym = 'A' LIMIT 5)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            5
                            """);
        });
    }

    @Test
    public void testPostingIndexWriterWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_writer_test";
                int plen = path.size();

                // Create a fake "covered column" in native memory (simulates a DOUBLE column)
                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        // Configure covering: 1 covered column (DOUBLE at shift=3)
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},  // columnTop = 0
                                new int[]{3},   // shift = 3 (Double.BYTES = 8 = 2^3)
                                new int[]{2},   // columnIndex = 2
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        // Add 3 keys, 10 row IDs total
                        writer.add(0, 0);
                        writer.add(0, 3);
                        writer.add(0, 6);
                        writer.add(1, 1);
                        writer.add(1, 4);
                        writer.add(1, 7);
                        writer.add(1, 9);
                        writer.add(2, 2);
                        writer.add(2, 5);
                        writer.add(2, 8);
                        writer.setMaxValue(9);
                        writer.commit();
                    }
                    // Writer close triggers seal which writes .pci and .pc0

                    FilesFacade ff = configuration.getFilesFacade();
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)));
                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue(liveSealTxn >= 0);
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testReaderStateGetters() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "reader_state";
                int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 10; i++) {
                        writer.add(i % 3, i);
                    }
                    writer.setMaxValue(9);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 7L, 0L)) {
                    assertNotEquals(0, reader.getKeyBaseAddress());
                    assertNotEquals(0, reader.getValueBaseAddress());
                    assertTrue(reader.getKeyMemorySize() > 0);
                    assertTrue(reader.getValueMemorySize() > 0);
                    assertEquals(0L, reader.getColumnTop());
                    assertEquals(0, reader.getValueBlockCapacity());
                    assertEquals(COLUMN_NAME_TXN_NONE, reader.getColumnTxn());
                    assertEquals(7L, reader.getPartitionTxn());
                    assertEquals(3, reader.getKeyCount());
                    assertTrue(reader.isOpen());
                }
            }
        });
    }

    @Test
    public void testReindexPreservesCoveringWithDroppedColumnsBeforeTimestamp() throws Exception {
        // Regression: IndexBuilder and TableSnapshotRestore both fed
        // PostingIndexWriter.configureCovering with metadata.getTimestampIndex()
        // taken from RecordMetadata, which is a DENSE position. The writer
        // then compared it against coveredColumnIndices, which are WRITER
        // indices. After DROP COLUMN before the timestamp the two index
        // spaces diverge, and a covered column whose writer index happens
        // to equal the timestamp's dense position gets misclassified as
        // the designated timestamp at REINDEX time. The misclassified
        // column's bytes are routed through compressLongsLinearPred — a
        // long-only, monotonic-only encoder — bypassing the type-specific
        // encoder and corrupting the covering sidecar on disk.
        //
        // Layout that triggers the collision:
        //   CREATE: writer 0=a, 1=b, 2=ts, 3=sym, 4=c
        //   DROP a: live writer 1=b, 2=ts, 3=sym, 4=c
        //           live dense  0=b, 1=ts, 2=sym, 3=c, dense_ts = 1
        //   covering for sym holds writer 1 (b). 1 == 1 == dense_ts -> hit.
        //
        // Disable timestamp auto-include so the covering list contains
        // exactly the user-specified columns (otherwise ts at writer 2 is
        // appended too, which is unrelated to the bug).
        setProperty(io.questdb.PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, "false");
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_reindex (
                        a INT,
                        b INT,
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_reindex VALUES
                        (1, 100, '2024-01-01T00:00:00', 'A', 200),
                        (2, 101, '2024-01-01T01:00:00', 'A', 201),
                        (3, 102, '2024-01-01T02:00:00', 'A', 202)
                    """);
            execute("ALTER TABLE t_reindex ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (b, c)");

            // Sanity: covering query is correct before any drop / reindex.
            assertQuery("SELECT b, c FROM t_reindex WHERE sym = 'A' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            100\t200
                            101\t201
                            102\t202
                            """);

            execute("ALTER TABLE t_reindex DROP COLUMN a");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            execute("REINDEX TABLE t_reindex LOCK EXCLUSIVE");
            engine.releaseAllReaders();

            // After REINDEX, the covering query for b must still return
            // the original values. With the bug, b's bytes were re-encoded
            // through compressLongsLinearPred and the read path returns
            // garbage.
            assertQuery("SELECT b, c FROM t_reindex WHERE sym = 'A' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            100\t200
                            101\t201
                            102\t202
                            """);
        });
    }

    @Test
    public void testRenameIndexedColumn() throws Exception {
        // Rename a column that has INDEX TYPE POSTING
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rename (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rename VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            execute("ALTER TABLE t_rename RENAME COLUMN sym TO symbol");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Query using new name — index should still work
            assertQuery("SELECT val FROM t_rename WHERE symbol = 'A'")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.5
                            11.5
                            """);
        });
    }

    @Test
    public void testRowCursorGetSymbolTableForIncludedSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rc_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rc_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'tag_x', 1.0),
                    ('2024-01-01T01:00:00', 'A', 'tag_y', 2.0),
                    ('2024-01-01T02:00:00', 'A', 'tag_x', 3.0),
                    ('2024-01-01T03:00:00', 'B', 'tag_z', 4.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT tag, price FROM t_rc_sym WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            tag\tprice
                            tag_x\t1.0
                            tag_y\t2.0
                            tag_x\t3.0
                            """);

            assertQuery("SELECT tag, price FROM t_rc_sym WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            tag\tprice
                            tag_x\t3.0
                            """);
        });
    }

    @Test
    public void testRuntimeConstantSymbolKeyWithBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr(0, "A");
            assertQuery("SELECT price FROM t_bind WHERE sym = $1")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            30.0
                            """);

            bindVariableService.clear();
            bindVariableService.setStr(0, "NEVER_INSERTED");
            assertQuery("SELECT price FROM t_bind WHERE sym = $1")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");

            bindVariableService.clear();
            bindVariableService.setStr(0, "A");
            bindVariableService.setStr(1, "B");
            assertQuery("SELECT price FROM t_bind WHERE sym IN ($1, $2) ORDER BY price")
                    .noLeakCheck()
                    .returns("""
                            price
                            10.0
                            20.0
                            30.0
                            """);

            bindVariableService.clear();
            bindVariableService.setStr(0, "NEVER_INSERTED");
            bindVariableService.setStr(1, "ALSO_UNKNOWN");
            assertQuery("SELECT price FROM t_bind WHERE sym IN ($1, $2)")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testSampleByFillKeyedIndexedNullScanDoesNotLeak() throws Exception {
        // Regression: keyed SAMPLE BY FILL(PREV) drives the base twice and calls
        // baseCursor.toTop() between passes. PageFrameRecordCursorImpl.toTop()
        // used to drop rowCursor by assigning null instead of closing it, which
        // stranded the active PostingIndexFwdReader.Cursor outside the reader's
        // freeCursors pool and leaked the cursor's block buffer.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_leak (
                        sym SYMBOL INDEX TYPE POSTING,
                        v BYTE,
                        k INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Interleaved NULL/'A' sym values: the IS NULL probe has entries to
            // walk on every sampled hour, so loadDenseGenerationCached allocates.
            execute("""
                    INSERT INTO t_leak
                    SELECT
                        CASE WHEN x % 2 = 0 THEN cast(NULL as SYMBOL) ELSE cast('A' as SYMBOL) END,
                        (x % 8)::BYTE,
                        (x % 3)::INT,
                        dateadd('m', (x * 30)::INT, '2024-01-01T00:00:00.000000Z')::TIMESTAMP
                    FROM long_sequence(48)
                    """);
            drainWalQueue();

            // The keyed projection forces the keyed SampleByFillPrev path.
            try (RecordCursorFactory f = select(
                    "SELECT k, avg(v), ts FROM (SELECT * FROM t_leak WHERE sym IS NULL) "
                            + "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR ORDER BY ts ASC"
            );
                 RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                int rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                assertTrue("expected at least one fill-prev row", rows > 0);
            }
        });
    }

    @Test
    public void testSealCreatesNewValueFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal VALUES
                    ('2024-01-01T00:00:00', 'A', 1),
                    ('2024-01-01T01:00:00', 'B', 2),
                    ('2024-01-01T02:00:00', 'A', 3)
                    """);
            engine.releaseAllWriters();

            // Check what files exist
            java.io.File partDir = new java.io.File(root, "t_seal~/2024-01-01");
            String[] pvFiles = partDir.list((_, n) -> n.startsWith("sym.pv"));
            System.out.println("PV files: " + java.util.Arrays.toString(pvFiles));

            // Full scan without index (SELECT * doesn't use index)
            assertQuery("SELECT * FROM t_seal")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\tA\t1
                            2024-01-01T01:00:00.000000Z\tB\t2
                            2024-01-01T02:00:00.000000Z\tA\t3
                            """);

            // Index scan
            assertQuery("SELECT count() FROM t_seal WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testSealFsyncsCoveringSidecarFiles() throws Exception {
        // Regression: PostingIndexWriter.sealIncremental syncs the sealed
        // value file (.pv.<sealTxn>) before publishing the new metadata
        // page in .pk, but it never syncs the covering sidecar files
        // (.pc<N>... and .pci). On power loss the kernel is free to
        // reorder writes, so an installer can see the new sealTxn in .pk
        // while the sidecar tail pages are still dirty in page cache —
        // readers then map a torn sidecar and decode garbage.
        //
        // This test tracks every msync call by mapping mmap address back
        // to the file descriptor that was passed to mmap, and asserts
        // that at least one .pc<N> file and the .pci file were msync'd
        // by the time the seal completed.
        final java.util.concurrent.ConcurrentHashMap<Long, String> fdToPath = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.ConcurrentHashMap<Long, Long> addrToFd = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Set<String> syncedFiles = java.util.concurrent.ConcurrentHashMap.newKeySet();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                long addr = super.mmap(fd, len, offset, flags, memoryTag);
                if (addr > 0) {
                    addrToFd.put(addr, fd);
                }
                return addr;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                Long fd = addrToFd.get(addr);
                if (fd != null) {
                    String path = fdToPath.get(fd);
                    if (path != null) {
                        syncedFiles.add(path);
                    }
                }
                super.msync(addr, len, async);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd > 0 && name != null) {
                    fdToPath.put(fd, Utf8s.stringFromUtf8Bytes(name));
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            // Force every memory-mapped sync to actually flush so msync
            // gets called by MemoryCMARWImpl.sync() instead of being a no-op.
            setProperty(io.questdb.PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("""
                    CREATE TABLE t_sync (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert in-order rows into 2 partitions.
            execute("""
                    INSERT INTO t_sync VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0)
                    """);
            // Now do an O3 insert that lands in partition 1. This forces
            // sealPostingIndexesForO3Partitions() and produces sealed .pv
            // and covering sidecar files for partition 1.
            execute("""
                    INSERT INTO t_sync VALUES
                    ('2024-01-01T00:30:00', 'C', 9.0)
                    """);
            // Clear the recorded set just before the seal runs by closing
            // the writer (which triggers the final seal of the last
            // partition) — but keep the tracking maps live so addresses
            // recorded earlier still resolve.
            engine.releaseAllWriters();

            boolean sidecarSynced = false;
            boolean coverInfoSynced = false;
            for (String f : syncedFiles) {
                if (f.contains("sym.pci")) {
                    coverInfoSynced = true;
                }
                if (f.matches(".*sym\\.pc\\d+\\..*")) {
                    sidecarSynced = true;
                }
            }
            assertTrue("seal must msync the .pci covering info sidecar; synced files: " + syncedFiles,
                    coverInfoSynced);
            assertTrue("seal must msync the .pc<N> covering data sidecar(s); synced files: " + syncedFiles,
                    sidecarSynced);
        });
    }

    @Test
    public void testSeekToLastEmptyPartition() throws Exception {
        // Gap 6: LATEST ON where key exists in day 1 but not day 2.
        // seekToLast in day 2 returns -1, cursor tries day 1.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seek_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seek_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // A exists in day 1 only. LATEST ON scans day 2 first (DESC),
            // finds 0 rows for A, falls to day 1.
            assertQuery("SELECT price FROM t_seek_empty WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            """);
        });
    }

    @Test
    public void testSeekToLastForwardCursorThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "seek_last_fwd";
                int plen = path.size();
                int rowCount = 4;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, i + 1);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3},
                                new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                        );
                        for (int i = 0; i < rowCount; i++) writer.add(0, i);
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        try (RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0})) {
                            assertTrue(cursor instanceof CoveringRowCursor);
                            try {
                                ((CoveringRowCursor) cursor).seekToLast();
                                fail("expected UnsupportedOperationException");
                            } catch (UnsupportedOperationException ignored) {
                            }
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSelfJoinTriggersBVariantGetters() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_join (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, label, payload, l256, bdata, k),
                        tag SYMBOL,
                        label VARCHAR,
                        payload STRING,
                        l256 LONG256,
                        bdata BINARY,
                        k INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_join VALUES
                    ('2024-01-01T00:00:00', 'A', 'tag_a', 'lbl_a1', 'pay_a1',
                     cast('0x1111111111111111111111111111111111111111111111111111111111111111' as LONG256),
                     rnd_bin(8, 16, 0), 1),
                    ('2024-01-01T01:00:00', 'A', 'tag_b', 'lbl_a2', 'pay_a2',
                     cast('0x2222222222222222222222222222222222222222222222222222222222222222' as LONG256),
                     rnd_bin(8, 16, 0), 2),
                    ('2024-01-01T02:00:00', 'B', 'tag_a', 'lbl_b1', 'pay_b1',
                     cast('0x3333333333333333333333333333333333333333333333333333333333333333' as LONG256),
                     rnd_bin(8, 16, 0), 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM (" +
                    "SELECT a.tag, a.label, a.payload, a.l256, b.tag b_tag, b.label b_label, b.payload b_payload, b.l256 b_l256 " +
                    "FROM (SELECT tag, label, payload, l256, k FROM t_join WHERE sym = 'A') a " +
                    "JOIN (SELECT tag, label, payload, l256, k FROM t_join WHERE sym = 'A') b ON a.k = b.k)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n2\n");

            assertQuery("SELECT a.label, b.label FROM (SELECT label, tag FROM t_join WHERE sym = 'A') a " +
                    "JOIN (SELECT label, tag FROM t_join WHERE sym = 'B') b ON a.tag = b.tag")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("label\tlabel1\nlbl_a1\tlbl_b1\n");
        });
    }

    @Test
    public void testSequencerMetadataPreservesIndexType() throws Exception {
        // Verify that WAL table sequencer metadata persists and reloads index type
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seq_idx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_seq_idx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            drainWalQueue();

            // Verify index type is POSTING through the reader
            try (TableReader r = engine.getReader("t_seq_idx")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertEquals(IndexType.POSTING, r.getMetadata().getColumnIndexType(symIdx));
            }

            // Force sequencer metadata reload by releasing everything and reopening
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_seq_idx")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertEquals("index type should survive reload", IndexType.POSTING, r.getMetadata().getColumnIndexType(symIdx));
            }
        });
    }

    @Test
    public void testShowColumnsWithBitmapIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show_bmp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX CAPACITY 256,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // BITMAP index should show indexType=BITMAP and empty indexInclude
            assertQuery("SHOW COLUMNS FROM t_show_bmp")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tBITMAP\t
                            val\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
        });
    }

    @Test
    public void testShowColumnsWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show_cols (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify indexType and indexInclude columns are present for POSTING index.
            // The timestamp is auto-appended to INCLUDE for POSTING indexes (see
            // CreateTableOperationImpl.maybeAppendTimestamp), so it shows up here.
            assertQuery("SHOW COLUMNS FROM t_show_cols")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\tprice,qty,ts
                            price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            qty\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
        });
    }

    @Test
    public void testShowCreateTableIncludeAfterAllCoveredColumnsDropped() throws Exception {
        // Regression: when every covered column is dropped, the writer
        // tombstones each entry to -1 but does not shrink the covering
        // list (TableWriter.tombstoneCoveredColumnInOtherIndexes). The
        // SHOW CREATE TABLE renderer guards on coveringCols.size() > 0
        // and emits "INCLUDE (" / ")" unconditionally, so the rendered
        // DDL becomes "INCLUDE ()" — which the parser then rejects with
        // "at least one column name expected in INCLUDE", breaking the
        // SHOW CREATE TABLE -> execute round-trip.
        // Disable the timestamp auto-include so the covering list does
        // not retain ts as a survivor, making it possible to fully empty
        // the list via DROP COLUMN.
        setProperty(io.questdb.PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, "false");
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_all (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_drop_all ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            execute("ALTER TABLE t_drop_all DROP COLUMN price");
            execute("ALTER TABLE t_drop_all DROP COLUMN qty");

            // Capture the rendered DDL. SHOW CREATE TABLE returns a single
            // VARCHAR column.
            String ddl;
            try (var factory = select("SHOW CREATE TABLE t_drop_all");
                 var cursor = factory.getCursor(sqlExecutionContext)) {
                assertTrue(cursor.hasNext());
                Utf8Sequence varchar = cursor.getRecord().getVarcharA(0);
                Assert.assertNotNull(varchar);
                ddl = varchar.toString();
            }
            assertFalse("SHOW CREATE TABLE produced invalid INCLUDE () in: " + ddl,
                    ddl.contains("INCLUDE ()"));
            assertFalse("SHOW CREATE TABLE produced invalid INCLUDE() in: " + ddl,
                    ddl.contains("INCLUDE()"));

            // The rendered DDL must round-trip: dropping the table and
            // re-running the DDL must succeed.
            execute("DROP TABLE t_drop_all");
            execute(ddl);

            // SHOW COLUMNS must not crash on the all-tombstoned list and
            // must report indexInclude as empty for sym.
            assertQuery("SHOW COLUMNS FROM t_drop_all")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\t
                            """);
        });
    }

    @Test
    public void testShowCreateTableIncludeAfterDropColumn() throws Exception {
        // Regression: the persisted covering list stores writer indices
        // (stable across DROP COLUMN), but the SHOW CREATE TABLE / SHOW
        // COLUMNS renderers in CairoTable-backed metadata pass those indices
        // to CairoTable.getColumnQuiet(int), which is dense-position keyed.
        // After dropping an unrelated column the dense positions of all
        // later columns shift down by one while writer indices stay put,
        // so the renderer either looks up the wrong column or runs off the
        // end of the list and silently omits a covered column.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_show (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        unrelated DOUBLE,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_drop_show ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            // Drop a column that is NOT in the INCLUDE list. This shifts
            // dense positions of price/qty down by one but leaves their
            // writer indices unchanged, so a writer-keyed renderer should
            // still print every covered column (price, qty, and the ts
            // that the ALTER path auto-appends for POSTING indexes).
            execute("ALTER TABLE t_drop_show DROP COLUMN unrelated");

            assertQuery("SHOW CREATE TABLE t_drop_show")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ddl
                            CREATE TABLE 't_drop_show' (\s
                            \tts TIMESTAMP,
                            \tsym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, ts),
                            \tprice DOUBLE,
                            \tqty INT
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);

            assertQuery("SHOW COLUMNS FROM t_drop_show")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\tprice,qty,ts
                            price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            qty\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
        });
    }

    @Test
    public void testShowCreateTableWithInclude() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            assertQuery("SHOW CREATE TABLE t_show")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ddl
                            CREATE TABLE 't_show' (\s
                            \tts TIMESTAMP,
                            \tsym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, ts),
                            \tprice DOUBLE,
                            \tqty INT
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testSidecarMissingFileFallsBackToUnavailable() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_missing_sidecar";
                int plen = path.size();

                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3},
                                new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                        );
                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    FilesFacade ff = configuration.getFilesFacade();
                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    LPSZ sidecarFile = PostingIndexUtils.coverDataFileName(
                            path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn);
                    assertTrue("sidecar present before deletion", ff.exists(sidecarFile));
                    assertTrue("sidecar removed", ff.removeQuiet(sidecarFile));

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        try {
                            assertTrue(cursor instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) cursor;

                            int count = 0;
                            while (cursor.hasNext()) {
                                long rowId = cursor.next();
                                assertEquals(count, rowId);
                                assertFalse("missing sidecar must surface as unavailable",
                                        cc.isCoveredAvailable(0));
                                count++;
                            }
                            assertEquals(rowCount, count);
                        } finally {
                            Misc.free(cursor);
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSidecarVersioningAcrossSeals() throws Exception {
        // Successive seals must create NEW versioned sidecar files
        // (.pc<N>.<C>.<S>) rather than truncating the previous sealed
        // version in place — otherwise a reader mmap'd to the prior
        // sealed-version file would be invalidated.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_seal_versioning";
                int plen = path.size();
                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }
                    FilesFacade ff = configuration.getFilesFacade();

                    // First writer instance: write data and explicit seal.
                    long firstSealTxn;
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 0; i < 10; i++) {
                            writer.add(i % 3, i);
                        }
                        writer.setMaxValue(9);
                        writer.commit();
                        writer.seal();
                        firstSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                                ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    }
                    assertTrue("first seal must produce a positive sealTxn", firstSealTxn > 0);
                    // Verify .pc0 at the first sealTxn is on disk.
                    assertTrue("first-seal sidecar .pc0 must exist",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, firstSealTxn)));

                    // Second writer instance: reopen, write more data, force another seal.
                    long secondSealTxn;
                    try (PostingIndexWriter writer2 = new PostingIndexWriter(configuration)) {
                        writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                        writer2.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 10; i < 30; i++) {
                            writer2.add(i % 3, i);
                        }
                        writer2.setMaxValue(29);
                        writer2.commit();
                        writer2.seal();
                        secondSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                                ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    }
                    assertTrue("second seal must advance sealTxn beyond the first",
                            secondSealTxn > firstSealTxn);

                    // The previous-seal .pc0 file must STILL exist on disk
                    // after the second seal — the previous sealed version
                    // stays untouched until the background purge job collects it.
                    assertTrue("first-seal sidecar .pc0 must survive a subsequent seal",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, firstSealTxn)));
                    // And the second seal's .pc0 at the new sealTxn must exist too.
                    assertTrue("second-seal sidecar .pc0 must exist at the new sealTxn",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, secondSealTxn)));

                    // Reader at the latest sealTxn sees ALL committed data.
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        int totalRows = 0;
                        for (int key = 0; key < 3; key++) {
                            CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(key, 0, Long.MAX_VALUE, new int[]{0});
                            while (cc.hasNext()) {
                                long rowId = cc.next();
                                double covered = cc.getCoveredDouble(0);
                                assertEquals("covered value must round-trip across the seal boundary",
                                        100.0 + rowId, covered, 0.001);
                                totalRows++;
                            }
                            Misc.free(cc);
                        }
                        assertEquals("reader must see every committed row across both seals", 30, totalRows);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSingleKeyResumeProducesAllRowsAcrossFrameCap() throws Exception {
        final int capForTest = 50;
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(capForTest);
        try {
            assertMemoryLeak(() -> {
                execute("""
                        CREATE TABLE t_sk_resume (
                            ts TIMESTAMP,
                            sym SYMBOL,
                            payload VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                execute("""
                        INSERT INTO t_sk_resume
                        SELECT
                            dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                            'K0',
                            'payload-' || x
                        FROM long_sequence(200)
                        """);
                drainWalQueue();
                execute("ALTER TABLE t_sk_resume ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (payload)");
                drainWalQueue();

                assertQuery("SELECT count() FROM t_sk_resume WHERE sym = 'K0'")
                        .noRandomAccess()
                        .expectSize()
                        .noLeakCheck()
                        .returns("count\n200\n");

                assertQuery("SELECT count_distinct(payload::string) c FROM t_sk_resume WHERE sym = 'K0'")
                        .noRandomAccess()
                        .expectSize()
                        .noLeakCheck()
                        .returns("c\n200\n");
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testSparseGenCoverWithLeadingBlockTrim() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_sparse_min";
                int plen = path.size();
                int rowCount = 600;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, i);
                    }

                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );

                    // Gen 0 (dense): introduces keys 0, 1, 2 so the writer's total keyCount=3.
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.add(2, 2);
                    writer.setMaxValue(2);
                    writer.commit();

                    // Gen 1 (sparse): touches only keys 0 and 1 — key 2 inactive makes the gen sparse.
                    // key 0: rows 100..399 (300 values → sidecarBase=300 in gen 1 for key 1)
                    // key 1: rows 400..599 (200 values → 4 blocks of 64/64/64/8)
                    for (int i = 100; i < 400; i++) writer.add(0, i);
                    for (int i = 400; i < 600; i++) writer.add(1, i);
                    writer.setMaxValue(rowCount - 1);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // minValue=464 drops block 0 (rows 400..463); blocks 1..3 remain.
                        final long minValue = 464;
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(1, minValue, Long.MAX_VALUE, new int[]{0});
                        try {
                            assertTrue(cc.isCoveredAvailable(0));
                            int expectedRow = 464;
                            while (cc.hasNext()) {
                                // RowCursor.next() returns rowId relative to minValue.
                                long actualRow = cc.next() + minValue;
                                assertEquals(expectedRow, actualRow);
                                double covered = cc.getCoveredDouble(0);
                                assertEquals("row " + actualRow + " covered value",
                                        (double) actualRow, covered, 0.0);
                                expectedRow++;
                            }
                            assertEquals(600, expectedRow);
                        } finally {
                            Misc.free(cc);
                        }
                    }

                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testStringPageFrameDataCorrectness() throws Exception {
        // STRING columns also go through the page frame path now
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_str_pf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_str_pf VALUES
                    ('2024-01-01T00:00:00', 'A', 'hello'),
                    ('2024-01-01T01:00:00', 'A', 'world'),
                    ('2024-01-01T02:00:00', 'A', null),
                    ('2024-01-01T03:00:00', 'B', 'other')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT label FROM t_str_pf WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("label\nhello\nworld\n\n");
        });
    }

    @Test
    public void testTruncateTableWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_trunc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_trunc VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Verify data exists
            assertQuery("SELECT price FROM t_trunc WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            10.5
                            """);

            execute("TRUNCATE TABLE t_trunc");

            // Table should be empty
            assertQuery("SELECT price FROM t_trunc WHERE sym = 'A'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            price
                            """);

            // Insert new data — index should work after truncate
            execute("""
                    INSERT INTO t_trunc VALUES
                    ('2024-01-02T00:00:00', 'C', 30.5),
                    ('2024-01-02T01:00:00', 'C', 31.5)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price FROM t_trunc WHERE sym = 'C'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price
                            30.5
                            31.5
                            """);
        });
    }

    @Test
    public void testVarcharPageFrameCountMixed() throws Exception {
        // COUNT(*) with mixed VARCHAR + DOUBLE INCLUDE.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_count (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_count
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_str('alpha','beta','gamma'),
                           rnd_double() * 100
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            // COUNT via non-covering path as baseline
            assertQuery("SELECT COUNT(*) FROM t_vw_count WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            528
                            """);
        });
    }

    @Test
    public void testVarcharPageFrameDataCorrectness() throws Exception {
        // Verify VARCHAR values are correctly read through the page frame path
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_data VALUES
                    ('2024-01-01T00:00:00', 'A', 'short'),
                    ('2024-01-01T01:00:00', 'A', 'medium_length_string'),
                    ('2024-01-01T02:00:00', 'A', 'this is a longer string that exceeds the inline limit of nine bytes'),
                    ('2024-01-01T03:00:00', 'A', ''),
                    ('2024-01-01T04:00:00', 'A', null),
                    ('2024-01-01T05:00:00', 'B', 'other')
                    """);
            engine.releaseAllWriters();

            // Compare covering vs non-covering results
            assertQuery("SELECT name FROM t_vw_data WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("name\nshort\nmedium_length_string\nthis is a longer string that exceeds the inline limit of nine bytes\n\n\n");
        });
    }

    @Test
    public void testVarcharPageFrameInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_in VALUES
                    ('2024-01-01T00:00:00', 'A', 'alpha'),
                    ('2024-01-01T01:00:00', 'B', 'beta'),
                    ('2024-01-01T02:00:00', 'C', 'gamma'),
                    ('2024-01-01T03:00:00', 'A', 'delta')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name FROM t_vw_in WHERE sym IN ('A', 'B') ORDER BY ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            name
                            alpha
                            beta
                            delta
                            """);
        });
    }

    @Test
    public void testVarcharPageFrameMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'day1_alpha', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'day1_beta', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'day2_alpha', 30.0),
                    ('2024-01-03T00:00:00', 'A', 'day3_alpha', 50.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_vw_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            day1_alpha\t10.0
                            day2_alpha\t30.0
                            day3_alpha\t50.0
                            """);

            // Aggregate across partitions
            assertQuery("SELECT count(*), min(price), max(price) FROM t_vw_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count\tmin\tmax
                            3\t10.0\t50.0
                            """);
        });
    }

    @Test
    public void testVarcharPageFramePlan() throws Exception {
        // VARCHAR in INCLUDE should use CoveringIndex with page frame path (not row-by-row)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 'hello', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'world', 20.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_vw_plan WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: name, price
                                  filter: sym='A'
                            """);
        });
    }

    @Test
    public void testVarcharPageFrameVectorizedGroupBy() throws Exception {
        // Vectorized GROUP BY on covered DOUBLE column should work even when
        // VARCHAR is also in INCLUDE (page frame path now supports var-width).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_agg (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_agg VALUES
                    ('2024-01-01T00:00:00', 'A', 'alpha', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'beta', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'gamma', 30.0),
                    ('2024-01-01T03:00:00', 'A', null, 50.0)
                    """);
            engine.releaseAllWriters();

            // Vectorized aggregate uses page frame cursor
            assertQuery("SELECT count(*), min(price), max(price) FROM t_vw_agg WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),min(price),max(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .returns("""
                            count\tmin\tmax
                            3\t10.0\t50.0
                            """);
        });
    }

    @Test
    public void testVarcharPageFrameWithFilter() throws Exception {
        // Residual filter on covered DOUBLE column with VARCHAR also in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 'cheap', 5.0),
                    ('2024-01-01T01:00:00', 'A', 'mid', 50.0),
                    ('2024-01-01T02:00:00', 'A', 'expensive', 95.0),
                    ('2024-01-01T03:00:00', 'B', 'other', 75.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT name, price FROM t_vw_filt WHERE sym = 'A' AND price > 20")
                    .noLeakCheck()
                    .returns("""
                            name\tprice
                            mid\t50.0
                            expensive\t95.0
                            """);
        });
    }

    @Test
    public void testWalAlterTableAddIncludeColumnTopFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        v_int INT,
                        v_str STRING,
                        v_vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 'one', 'vc_one'),
                    ('2024-01-01T01:00:00', 'B', 2, 'two', 'vc_two'),
                    ('2024-01-02T00:00:00', 'A', 3, 'three', 'vc_three')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t_alter ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (v_int, v_str, v_vc)");
            drainWalQueue();

            execute("""
                    INSERT INTO t_alter VALUES
                    ('2024-01-03T00:00:00', 'A', 4, 'four', 'vc_four'),
                    ('2024-01-03T01:00:00', 'B', 5, 'five', 'vc_five')
                    """);
            drainWalQueue();

            assertSqlCursors(
                    "SELECT v_int, v_str, v_vc FROM t_alter WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ v_int, v_str, v_vc FROM t_alter WHERE sym = 'A'"
            );

            assertQuery("SELECT count() FROM t_alter WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n3\n");
        });
    }

    @Test
    public void testWideInclude10Columns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide10 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT,
                        c1 LONG,
                        c2 DOUBLE,
                        c3 FLOAT,
                        c4 SHORT,
                        c5 BYTE,
                        c6 BOOLEAN,
                        c7 DATE,
                        c8 TIMESTAMP,
                        c9 CHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide10 VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1.5, 2.5, 10, 3, true, '2024-01-01', '2024-01-01T00:00:00', 'x'),
                    ('2024-01-01T01:00:00', 'B', 2, 200, 2.5, 3.5, 20, 4, false, '2024-01-02', '2024-01-01T01:00:00', 'y'),
                    ('2024-01-01T02:00:00', 'A', 3, 300, 3.5, 4.5, 30, 5, true, '2024-01-03', '2024-01-01T02:00:00', 'z')
                    """);
            engine.releaseAllWriters();

            // Query all 10 covered columns
            assertQuery("SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9 FROM t_wide10 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tc8\tc9
                            1\t100\t1.5\t2.5\t10\t3\ttrue\t2024-01-01T00:00:00.000Z\t2024-01-01T00:00:00.000000Z\tx
                            3\t300\t3.5\t4.5\t30\t5\ttrue\t2024-01-03T00:00:00.000Z\t2024-01-01T02:00:00.000000Z\tz
                            """);

            // Query a subset of covered columns
            assertQuery("SELECT c0, c5, c9 FROM t_wide10 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc5\tc9
                            1\t3\tx
                            3\t5\tz
                            """);

            // Plan should use CoveringIndex (SelectedRecord wraps subset)
            assertQuery("SELECT c0, c9 FROM t_wide10 WHERE sym = 'A'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: c0, c9
                                  filter: sym='A'
                            """);
        });
    }

    @Test
    public void testWideInclude20Columns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide20 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 LONG, c6 LONG, c7 LONG, c8 LONG, c9 LONG,
                        c10 DOUBLE, c11 DOUBLE, c12 DOUBLE, c13 DOUBLE, c14 DOUBLE,
                        c15 FLOAT, c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide20 VALUES
                    ('2024-01-01T00:00:00', 'X', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0),
                    ('2024-01-01T01:00:00', 'Y', 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0),
                    ('2024-01-01T02:00:00', 'X', 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210.0, 211.0, 212.0, 213.0, 214.0, 215.0, 216.0, 217.0, 218.0, 219.0)
                    """);
            engine.releaseAllWriters();

            // Query first and last covered columns
            assertQuery("SELECT c0, c19 FROM t_wide20 WHERE sym = 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc19
                            0\t19.0
                            200\t219.0
                            """);

            // Query all 20 covered columns
            assertQuery("SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM t_wide20 WHERE sym = 'Y'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tc8\tc9\tc10\tc11\tc12\tc13\tc14\tc15\tc16\tc17\tc18\tc19
                            100\t101\t102\t103\t104\t105\t106\t107\t108\t109\t110.0\t111.0\t112.0\t113.0\t114.0\t115.0\t116.0\t117.0\t118.0\t119.0
                            """);

            // IN-list across wide INCLUDE
            assertQuery("SELECT c0, c10, c19 FROM t_wide20 WHERE sym IN ('X', 'Y') ORDER BY c0")
                    .noLeakCheck()
                    .returns("""
                            c0\tc10\tc19
                            0\t10.0\t19.0
                            100\t110.0\t119.0
                            200\t210.0\t219.0
                            """);
        });
    }

    @Test
    public void testWideInclude20ColumnsMultiPartition() throws Exception {
        // 20 INCLUDE columns across multiple partitions and commits
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide20_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 LONG, c6 LONG, c7 LONG, c8 LONG, c9 LONG,
                        c10 DOUBLE, c11 DOUBLE, c12 DOUBLE, c13 DOUBLE, c14 DOUBLE,
                        c15 FLOAT, c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Partition 1, commit 1
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0)
                    """);
            // Partition 1, commit 2 (same partition, different gen)
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-01T12:00:00', 'A', 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0)
                    """);
            // Partition 2
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-02T00:00:00', 'A', 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210.0, 211.0, 212.0, 213.0, 214.0, 215.0, 216.0, 217.0, 218.0, 219.0),
                    ('2024-01-02T01:00:00', 'B', 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310.0, 311.0, 312.0, 313.0, 314.0, 315.0, 316.0, 317.0, 318.0, 319.0)
                    """);
            engine.releaseAllWriters();

            // Query across partitions and gens, select subset of wide INCLUDE
            assertQuery("SELECT c0, c9, c14, c19 FROM t_wide20_mp WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc9\tc14\tc19
                            0\t9\t14.0\t19.0
                            100\t109\t114.0\t119.0
                            200\t209\t214.0\t219.0
                            """);

            // LATEST ON with wide INCLUDE
            assertQuery("SELECT c0, c19 FROM t_wide20_mp WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            c0\tc19
                            200\t219.0
                            """);
        });
    }

    @Test
    public void testWideIncludeAlterTable() throws Exception {
        // Add a wide INCLUDE via ALTER TABLE, then insert and query
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_wide (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT, c4 SHORT,
                        c5 INT, c6 LONG, c7 DOUBLE, c8 FLOAT, c9 SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("ALTER TABLE t_alter_wide ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)");

            // Insert after adding index so sidecar data is written
            execute("""
                    INSERT INTO t_alter_wide VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2.0, 3.0, 4, 5, 6, 7.0, 8.0, 9),
                    ('2024-01-01T12:00:00', 'A', 10, 11, 12.0, 13.0, 14, 15, 16, 17.0, 18.0, 19),
                    ('2024-01-01T06:00:00', 'B', 20, 21, 22.0, 23.0, 24, 25, 26, 27.0, 28.0, 29)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT c0, c4, c9 FROM t_alter_wide WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc4\tc9
                            0\t4\t9
                            10\t14\t19
                            """);

            assertQuery("SELECT c0, c4, c9 FROM t_alter_wide WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc4\tc9
                            20\t24\t29
                            """);
        });
    }

    @Test
    public void testWideIncludeDistinct() throws Exception {
        // DISTINCT on symbol with wide INCLUDE — should still use posting index
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_distinct (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_distinct VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5.0, 6.0, 7.0, 8.0, 9.0),
                    ('2024-01-01T01:00:00', 'B', 10, 11, 12, 13, 14, 15.0, 16.0, 17.0, 18.0, 19.0),
                    ('2024-01-01T02:00:00', 'A', 20, 21, 22, 23, 24, 25.0, 26.0, 27.0, 28.0, 29.0),
                    ('2024-01-01T03:00:00', 'C', 30, 31, 32, 33, 34, 35.0, 36.0, 37.0, 38.0, 39.0)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT DISTINCT sym FROM t_wide_distinct")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym
                            A
                            B
                            C
                            """);
        });
    }

    @Test
    public void testWideIncludeInListSubset() throws Exception {
        // IN-list query selecting a sparse subset of wide INCLUDE columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT,
                        c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE, c10 DOUBLE, c11 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_in VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0),
                    ('2024-01-01T01:00:00', 'B', 10, 11, 12, 13, 14, 15, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0),
                    ('2024-01-01T02:00:00', 'C', 20, 21, 22, 23, 24, 25, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0)
                    """);
            engine.releaseAllWriters();

            // IN-list selecting every 3rd covered column
            assertQuery("SELECT c0, c3, c6, c9 FROM t_wide_in WHERE sym IN ('A', 'B')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            c0\tc3\tc6\tc9
                            0\t3\t6.0\t9.0
                            10\t13\t16.0\t19.0
                            """);
        });
    }

    @Test
    public void testWideIncludeLatestOn() throws Exception {
        // LATEST ON with wide INCLUDE — verifies last row per symbol
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 INT, c5 LONG, c6 DOUBLE, c7 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 2, 3.0, 4.0, 5, 6, 7.0, 8.0),
                    ('2024-01-01T01:00:00', 'B', 10, 20, 30.0, 40.0, 50, 60, 70.0, 80.0),
                    ('2024-01-01T02:00:00', 'A', 100, 200, 300.0, 400.0, 500, 600, 700.0, 800.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON single key
            assertQuery("SELECT c0, c1, c2, c3, c4, c5, c6, c7 FROM t_wide_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7
                            100\t200\t300.0\t400.0\t500\t600\t700.0\t800.0
                            """);

            // LATEST ON IN-list (ordered by symbol key)
            assertQuery("SELECT sym, c0, c7 FROM t_wide_latest WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            sym\tc0\tc7
                            A\t100\t800.0
                            B\t10\t80.0
                            """);
        });
    }

    @Test
    public void testWideIncludeManyRowsPerKey() throws Exception {
        // Many rows for same key with wide INCLUDE — stress sidecar writes
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_many (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 INT, c5 LONG, c6 DOUBLE, c7 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Insert 200 rows for key 'A', 50 for 'B'
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO t_wide_many VALUES ");
            for (int i = 0; i < 200; i++) {
                if (i > 0) sb.append(", ");
                String sym = i % 4 == 0 ? "B" : "A";
                sb.append("('2024-01-01T00:").append(String.format("%02d", i / 60)).append(":")
                        .append(String.format("%02d", i % 60)).append("', '")
                        .append(sym).append("', ")
                        .append(i).append(", ").append(i * 10L).append(", ")
                        .append(i * 1.5).append(", ").append(i * 2.5f).append(", ")
                        .append(i + 1000).append(", ").append((i + 1000) * 10L).append(", ")
                        .append((i + 1000) * 1.5).append(", ").append((i + 1000) * 2.5f).append(")");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Verify row count for key A (150 rows: indices not divisible by 4)
            assertQuery("SELECT count(*) count FROM t_wide_many WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            150
                            """);

            // Verify row count for key B (50 rows: indices divisible by 4)
            assertQuery("SELECT count(*) count FROM t_wide_many WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            50
                            """);

            // Spot check first row for A (i=1)
            assertQuery("SELECT c0, c1 FROM t_wide_many WHERE sym = 'A' LIMIT 1")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc1
                            1\t10
                            """);
        });
    }

    @Test
    public void testWideIncludeMetadataRoundtrip() throws Exception {
        // Verify metadata preserves wide INCLUDE column indices
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_meta (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 SHORT, c5 BYTE, c6 DATE, c7 TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_wide_meta")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(9, coveringCols.size());
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testWideIncludeNullsAllColumns() throws Exception {
        // All covered columns are NULL — tests null sentinel handling for every type
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (i, l, d, f, s, b, bl, dt, ts2, ch),
                        i INT,
                        l LONG,
                        d DOUBLE,
                        f FLOAT,
                        s SHORT,
                        b BYTE,
                        bl BOOLEAN,
                        dt DATE,
                        ts2 TIMESTAMP,
                        ch CHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', 42, 100, 3.14, 2.7, 10, 5, true, '2024-01-01', '2024-01-01T01:00:00', 'z')
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT i, l, d, f, s, b, bl, dt, ts2, ch FROM t_wide_nulls WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            i\tl\td\tf\ts\tb\tbl\tdt\tts2\tch
                            null\tnull\tnull\tnull\t0\t0\tfalse\t\t\t
                            42\t100\t3.14\t2.7\t10\t5\ttrue\t2024-01-01T00:00:00.000Z\t2024-01-01T01:00:00.000000Z\tz
                            """);
        });
    }

    @Test
    public void testWideIncludeNullsUuidIpv4Long256() throws Exception {
        // NULL handling for the fixed-size types not covered by
        // testWideIncludeNullsAllColumns: UUID (16 bytes, dual Long.MIN_VALUE
        // sentinel), IPv4 (4 bytes, sentinel = 0), LONG256 (32 bytes, all-zero
        // sentinel). Verifies the covering path renders these as NULL/empty
        // and not as the raw sentinel bit pattern.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_nulls_2 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, addr, l256),
                        id UUID,
                        addr IPv4,
                        l256 LONG256
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_nulls_2 VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', '11111111-1111-1111-1111-111111111111', '10.0.0.1',
                        cast('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef' as LONG256))
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT id, addr, l256 FROM t_wide_nulls_2 WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            id\taddr\tl256
                            \t\t
                            11111111-1111-1111-1111-111111111111\t10.0.0.1\t0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
                            """);
        });
    }

    @Test
    public void testWideIncludeWalTable() throws Exception {
        // Wide INCLUDE on WAL table with multiple commits
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_wide_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5.0, 6.0, 7.0, 8.0, 9.0)
                    """);
            execute("""
                    INSERT INTO t_wide_wal VALUES
                    ('2024-01-01T01:00:00', 'A', 10, 11, 12, 13, 14, 15.0, 16.0, 17.0, 18.0, 19.0)
                    """);
            drainWalQueue();

            assertQuery("SELECT c0, c4, c9 FROM t_wide_wal WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c0\tc4\tc9
                            0\t4\t9.0
                            10\t14\t19.0
                            """);
        });
    }

    @Test
    public void testWideIncludeWithVarchar() throws Exception {
        // Mix of fixed-size and variable-length columns in a wide INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, label, tag, note, extra_d, extra_i),
                        price DOUBLE,
                        qty INT,
                        label VARCHAR,
                        tag VARCHAR,
                        note STRING,
                        extra_d DOUBLE,
                        extra_i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 'label_a1', 'tag_a', 'note_a1', 1.1, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 'label_b1', 'tag_b', 'note_b1', 2.2, 2),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 'label_a2', 'tag_a', 'note_a2', 3.3, 3)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT price, qty, label, tag, note, extra_d, extra_i FROM t_wide_varchar WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            price\tqty\tlabel\ttag\tnote\textra_d\textra_i
                            10.5\t100\tlabel_a1\ttag_a\tnote_a1\t1.1\t1
                            30.5\t300\tlabel_a2\ttag_a\tnote_a2\t3.3\t3
                            """);

            // Query only varchar columns
            assertQuery("SELECT label, tag, note FROM t_wide_varchar WHERE sym = 'B'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            label\ttag\tnote
                            label_b1\ttag_b\tnote_b1
                            """);
        });
    }

    @Test
    public void testWideMultiTypeIncludeForCoveringRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_cover (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            v_byte, v_short, v_int, v_long, v_float, v_double,
                            v_bool, v_char, v_date, v_uuid, v_ipv4, v_l256,
                            v_str, v_vc, v_bin, v_arr, v_sym2,
                            v_dec64, v_dec128, v_dec256
                        ),
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_char CHAR,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_l256 LONG256,
                        v_str STRING,
                        v_vc VARCHAR,
                        v_bin BINARY,
                        v_arr DOUBLE[],
                        v_sym2 SYMBOL,
                        v_dec64 DECIMAL(18,4),
                        v_dec128 DECIMAL(38,10),
                        v_dec256 DECIMAL(40,5)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_cover VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1000, 10_000, 1.5, 2.5, true, 'x',
                     '2024-06-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.2.3.4',
                     cast('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef' as LONG256),
                     'str_one', 'vc_one', rnd_bin(8, 16, 0), ARRAY[1.0, 2.0], 'tag_x',
                     '12.34'::DECIMAL(18,4), '123.4567'::DECIMAL(38,10), '100.50'::DECIMAL(40,5)),
                    ('2024-01-01T01:00:00', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                    """);
            engine.releaseAllWriters();

            assertSqlCursors(
                    "SELECT v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, " +
                            "v_char, v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin, " +
                            "v_arr, v_sym2, v_dec64, v_dec128, v_dec256 " +
                            "FROM t_wide_cover WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, " +
                            "v_char, v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin, " +
                            "v_arr, v_sym2, v_dec64, v_dec128, v_dec256 " +
                            "FROM t_wide_cover WHERE sym = 'A'"
            );

            assertQuery("SELECT count() FROM t_wide_cover WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testWideMultiTypeIncludePageFrameAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_wide (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            v_byte, v_short, v_int, v_long, v_float, v_double,
                            v_bool, v_char, v_date, v_uuid, v_ipv4, v_l256,
                            v_str, v_vc, v_bin
                        ),
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_char CHAR,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_l256 LONG256,
                        v_str STRING,
                        v_vc VARCHAR,
                        v_bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_wide
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           CASE WHEN x % 3 = 0 THEN 'B' ELSE 'A' END,
                           x::BYTE, x::SHORT, x::INT, x::LONG, x::FLOAT, x::DOUBLE,
                           x % 2 = 0,
                           CASE x % 3 WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END,
                           dateadd('s', x::INT, '2020-01-01'),
                           rnd_uuid4(),
                           ('1.2.3.' || (x % 256))::IPv4,
                           rnd_long256(),
                           ('str_' || x),
                           ('vc_' || x)::VARCHAR,
                           rnd_bin(4, 8, 0)
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM (" +
                    "SELECT v_byte, v_short, v_int, v_long, v_float, v_double, v_bool, " +
                    "v_char, v_date, v_uuid, v_ipv4, v_l256, v_str, v_vc, v_bin " +
                    "FROM t_pf_wide WHERE sym = 'A' AND v_int > 0)")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n134\n");
        });
    }

    @Test
    public void testWideTableManyUncoveredColumns() throws Exception {
        // Table with 30 columns total but only 2 in INCLUDE — tests that
        // the sidecar doesn't interfere with many non-covered columns
        assertMemoryLeak(() -> {
            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE TABLE t_30col (\n");
            ddl.append("    ts TIMESTAMP,\n");
            ddl.append("    sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),\n");
            ddl.append("    price DOUBLE,\n");
            ddl.append("    qty INT");
            for (int i = 0; i < 26; i++) {
                ddl.append(",\n    extra_").append(i).append(" DOUBLE");
            }
            ddl.append("\n) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(ddl.toString());

            // Insert with all extra columns set to their index value
            StringBuilder ins = new StringBuilder();
            ins.append("INSERT INTO t_30col VALUES ('2024-01-01T00:00:00', 'A', 10.5, 100");
            for (int i = 0; i < 26; i++) {
                ins.append(", ").append(i).append(".0");
            }
            ins.append("), ('2024-01-01T01:00:00', 'B', 20.5, 200");
            for (int i = 0; i < 26; i++) {
                ins.append(", ").append(i + 100).append(".0");
            }
            ins.append(")");
            execute(ins.toString());
            engine.releaseAllWriters();

            // Covered query should use CoveringIndex
            assertQuery("SELECT price, qty FROM t_30col WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price, qty
                                  filter: sym='A'
                            """)
                    .returns("""
                            price\tqty
                            10.5\t100
                            """);

            // Query that includes uncovered columns falls back
            assertQuery("SELECT price, extra_0 FROM t_30col WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price\textra_0
                            10.5\t0.0
                            """);
        });
    }

    @Test
    public void testWideTablePartialCoverage() throws Exception {
        // Table with many columns but only some in INCLUDE — queries that select
        // non-covered columns must fall back to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_partial (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT,
                        uncovered1 DOUBLE,
                        uncovered2 INT,
                        uncovered3 VARCHAR,
                        uncovered4 LONG,
                        uncovered5 BOOLEAN
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_partial VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 99.9, 42, 'hello', 12345, true),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 88.8, 43, 'world', 67890, false),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 77.7, 44, 'test', 11111, true)
                    """);
            engine.releaseAllWriters();

            // Covered columns only — should use CoveringIndex
            assertQuery("SELECT price, qty FROM t_partial WHERE sym = 'A'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: price, qty
                                  filter: sym='A'
                            """)
                    .returns("""
                            price\tqty
                            10.5\t100
                            30.5\t300
                            """);

            // Mixed covered + uncovered — should NOT use CoveringIndex
            assertQuery("SELECT price, uncovered1 FROM t_partial WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            price\tuncovered1
                            10.5\t99.9
                            30.5\t77.7
                            """);

            // All uncovered — should NOT use CoveringIndex
            assertQuery("SELECT uncovered1, uncovered3 FROM t_partial WHERE sym = 'A'")
                    .noLeakCheck()
                    .returns("""
                            uncovered1\tuncovered3
                            99.9\thello
                            77.7\ttest
                            """);
        });
    }

    /**
     * Builds a minimal RecordMetadata for direct PostingIndexReader construction.
     * Pads slots up to the highest covered index with placeholder LONG columns;
     * each covered index gets the type the writer was given. Tests that bypass
     * TableReader must supply this — the reader resolves covered-column types
     * from live metadata, not from the .pci.
     */
    private static RecordMetadata coveringMetadata(int[] coveredIndices, int[] coveredTypes) {
        int maxIdx = -1;
        for (int idx : coveredIndices) {
            if (idx > maxIdx) maxIdx = idx;
        }
        GenericRecordMetadata m = new GenericRecordMetadata();
        for (int i = 0; i <= maxIdx; i++) {
            int type = ColumnType.LONG;
            for (int j = 0; j < coveredIndices.length; j++) {
                if (coveredIndices[j] == i) {
                    type = coveredTypes[j];
                    break;
                }
            }
            m.add(new TableColumnMetadata("c" + i, type, IndexType.NONE, 0, false, null, i, false));
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    private static long readSidecarMmapSize(PostingIndexFwdReader reader, int includeIdx) throws Exception {
        Class<?> base = reader.getClass().getSuperclass();
        Field f = base.getDeclaredField("sidecarMems");
        f.setAccessible(true);
        ObjList<MemoryMR> mems = (ObjList<MemoryMR>) f.get(reader);
        if (includeIdx >= mems.size()) {
            return -1;
        }
        MemoryMR mem = mems.getQuick(includeIdx);
        return mem == null ? -1 : mem.size();
    }

    private static long sidecarFileLengthOnDisk(Path basePath, CharSequence name, int includeIdx) {
        int len = basePath.size();
        try {
            LPSZ pcFile = PostingIndexUtils.coverDataFileName(
                    basePath, name, includeIdx, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 0);
            return TestFilesFacadeImpl.INSTANCE.length(pcFile);
        } finally {
            basePath.trimTo(len);
        }
    }

    /**
     * Regression for the LIMIT -N over a covering index with a residual
     * filter. The covering factory used to ignore the requested scan order in
     * getPageFrameCursor and always opened ascending partition frames, so the
     * parallel negative-limit machinery collected the lowest-timestamp rows
     * while believing they were the highest, silently returning the first N
     * rows instead of the last N.
     */
    @Test
    public void testNegativeLimitSingleKeyReturnsLastRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_neg (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_neg
                    SELECT dateadd('d', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE
                    FROM long_sequence(10)
                    """);
            execute("ALTER TABLE t_neg ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            // The residual price > 0 routes the query through the parallel
            // filter wrapper, which absorbs the limit (no separate Limit node).
            // The parallel path stays in effect for single-key queries.
            assertQuery("SELECT price FROM t_neg WHERE sym = 'A' AND price > 0 LIMIT -3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Filter")
                    .returns("""
                            price
                            8.0
                            9.0
                            10.0
                            """);
            // A filter that eliminates the low rows still returns the true tail.
            assertQuery("SELECT price FROM t_neg WHERE sym = 'A' AND price > 5 LIMIT -3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            price
                            8.0
                            9.0
                            10.0
                            """);
            // Larger negative limit spanning all partitions.
            assertQuery("SELECT price FROM t_neg WHERE sym = 'A' AND price > 0 LIMIT -20")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    /**
     * Like {@link #testNegativeLimitSingleKeyReturnsLastRows} but with several
     * matching rows per partition and a per-frame cap of 2, so each partition
     * is split into multiple sub-frames. A fix that merely reversed partition
     * order while leaving the sub-frames ascending within a partition would
     * still return the wrong rows here; only a true backward scan that emits
     * the high row-range sub-frame first passes.
     */
    @Test
    public void testNegativeLimitSingleKeyMultiFramePerPartition() throws Exception {
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
        try {
            assertMemoryLeak(() -> {
                // Single partition (PARTITION BY YEAR), 10 matching rows -> with a
                // 2-row cap the partition splits into 5 row-range sub-frames.
                execute("CREATE TABLE t_neg_mf (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
                execute("""
                        INSERT INTO t_neg_mf
                        SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE
                        FROM long_sequence(10)
                        """);
                execute("ALTER TABLE t_neg_mf ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

                assertQuery("SELECT price FROM t_neg_mf WHERE sym = 'A' AND price > 0 LIMIT -3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                price
                                8.0
                                9.0
                                10.0
                                """);
                // A sub-frame boundary-aligned limit (-4) and an odd one (-5).
                assertQuery("SELECT price FROM t_neg_mf WHERE sym = 'A' AND price > 0 LIMIT -4")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                price
                                7.0
                                8.0
                                9.0
                                10.0
                                """);
                assertQuery("SELECT price FROM t_neg_mf WHERE sym = 'A' AND price > 0 LIMIT -5")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                price
                                6.0
                                7.0
                                8.0
                                9.0
                                10.0
                                """);
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    /**
     * A bind-variable limit has an unknown sign at compile time. Single-key
     * covering still serves it through the parallel backward scan: a negative
     * value resolved at runtime must return the last N rows.
     */
    @Test
    public void testNegativeLimitSingleKeyBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_neg_bv (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_neg_bv
                    SELECT dateadd('d', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE
                    FROM long_sequence(10)
                    """);
            execute("ALTER TABLE t_neg_bv ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            // assertBinds compiles the query once and rebinds per case, so the
            // runtime-resolved limit sign must steer the same factory between the
            // tail (negative) and the head (positive). The negative limit buffers
            // the tail, so its size is known; the positive limit streams, so its
            // size stays undetermined - hence the per-case expectSize below.
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "lim=-3 returns the last 3 rows",
                    """
                            price
                            8.0
                            9.0
                            10.0
                            """,
                    bindVariableService -> bindVariableService.setLong("lim", -3)
            ).expectSize(true));
            cases.add(BindVarTuple.ok(
                    "lim=3 returns the first 3 rows",
                    """
                            price
                            1.0
                            2.0
                            3.0
                            """,
                    bindVariableService -> bindVariableService.setLong("lim", 3)
            ).expectSize(false));
            assertQuery("SELECT price FROM t_neg_bv WHERE sym = 'A' AND price > 0 LIMIT :lim")
                    .noLeakCheck()
                    .assertBinds(cases);
        });
    }

    /**
     * Multi-key covering is not globally timestamp-ordered, so codegen routes
     * its negative limits to the serial path (LimitRecordCursorFactory over a
     * FilteredRecordCursorFactory), which computes the last N via size + skip.
     * The data here keeps each key in a disjoint timestamp range so the tail is
     * unambiguous. The query must NOT take the parallel Async Filter path and
     * must NOT return the first N rows.
     */
    @Test
    public void testNegativeLimitMultiKeyFallsBackToSerial() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_neg_mk (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // 2024-01-02..06 -> 'A' price 1..5; 2024-01-07..11 -> 'B' price 6..10
            execute("""
                    INSERT INTO t_neg_mk
                    SELECT dateadd('d', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                           CASE WHEN x <= 5 THEN 'A' ELSE 'B' END,
                           x::DOUBLE
                    FROM long_sequence(10)
                    """);
            execute("ALTER TABLE t_neg_mk ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            // Multi-key negative limit must not use the parallel path, but must
            // still read the covering index.
            assertQuery("SELECT price FROM t_neg_mk WHERE sym IN ('A', 'B') AND price > 0 LIMIT -3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex")
                    .withPlanNotContaining("Async Filter")
                    .returns("""
                            price
                            8.0
                            9.0
                            10.0
                            """);
            // Positive limit over multi-key still uses the parallel path.
            assertQuery("SELECT price FROM t_neg_mk WHERE sym IN ('A', 'B') AND price > 0 LIMIT 3")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Filter");
        });
    }
}
