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

package io.questdb.test.tools;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.StringSink;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns every WAL table an opted-in test creates into a table with at least one
 * genuine composite partition, without changing a single row.
 * <p>
 * A test opts in with {@code AbstractCairoTest#enableCompositePartitionRandomisation(Rnd)},
 * which flips a 50% coin. On the runs the coin picks, every {@code CREATE TABLE}
 * or {@code INSERT} the test executes is followed by {@link #apply}, which
 * sweeps the tables and round-trips the middle of each one's timestamp range:
 * stash that range into a scratch table, cut it out with a REPLACE RANGE commit
 * carrying zero new rows, then insert the stash straight back. The cut alone
 * already leaves the range composite - a relocated front/back pair with dead
 * space where the cut range used to be - and the reinsert is what makes the
 * whole round-trip content-neutral, so every caller's existing, hardcoded
 * expected results still hold.
 * <p>
 * The REPLACE RANGE commit only exists on the WAL path, so opting in also turns
 * on {@code cairo.wal.enabled.default}. Tests that assert on non-WAL specifics -
 * transaction numbering, {@code tables()} output, WAL-unsupported DDL - cannot
 * opt in.
 * <p>
 * Two things the round-trip does NOT preserve, both of which are reasons for an
 * individual test to call {@code AbstractCairoTest#disableCompositePartitionRandomisation()}:
 * <ul>
 * <li>the table's TRANSACTION number, since the cut is a commit of its own. See
 * {@link #injectedTxnCount} for the count a test can subtract back out.</li>
 * <li>the physical ORDER of rows sharing a timestamp, since the cut range is
 * reinserted after the rows that stayed. The row SET is identical, so only a
 * query that neither orders nor aggregates within a timestamp can tell.</li>
 * </ul>
 */
public final class CompositePartitionRandomiser {
    private static final Log LOG = LogFactory.getLog(CompositePartitionRandomiser.class);
    // Tables the round-trip has already run on, by directory name. A table whose partitions the
    // round-trip cannot make composite - too few rows, or a shape merge-append rewrites whole rather
    // than cutting into pieces - would otherwise be retried by every later sweep, and each retry
    // costs a scratch table's CREATE and DROP.
    private static final Set<String> attempted = ConcurrentHashMap.newKeySet();
    // Transactions the round-trip has committed against each table, keyed by directory name so a
    // rename does not lose the count. The cut is content-neutral in ROWS but not in TRANSACTIONS,
    // and a test that asserts on a table's transaction number has to subtract these.
    private static final ConcurrentHashMap<String, Integer> injectedTxn = new ConcurrentHashMap<>();
    // The sweep runs its own DDL through the same execute() path that calls it.
    private static final ThreadLocal<Boolean> sweeping = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private CompositePartitionRandomiser() {
    }

    /**
     * Forgets which tables the round-trip has run on. Call between tests.
     */
    public static void clear() {
        attempted.clear();
        injectedTxn.clear();
    }

    /**
     * How many transactions the round-trip committed against {@code tableName}, which is how far
     * that table's transaction number has moved beyond what a test without this randomiser would
     * see. Zero when the randomiser is off or never composited the table.
     */
    public static int injectedTxnCount(CairoEngine engine, CharSequence tableName) {
        final TableToken tableToken = engine.getTableTokenIfExists(tableName);
        if (tableToken == null) {
            return 0;
        }
        return injectedTxn.getOrDefault(tableToken.getDirName(), 0);
    }

    /**
     * Drains the WAL queue, then makes every WAL table in {@code engine} composite unless it
     * already is.
     */
    public static void sweep(CairoEngine engine, SqlExecutionContext executionContext) {
        if (sweeping.get()) {
            return;
        }
        sweeping.set(Boolean.TRUE);
        try {
            TestUtils.drainWalQueue(engine);
            ObjHashSet<TableToken> tokens = new ObjHashSet<>();
            engine.getTableTokens(tokens, false);
            for (int i = 0, n = tokens.size(); i < n; i++) {
                TableToken tableToken = tokens.get(i);
                // Mat views are written by their own refresh, never directly.
                if (tableToken.isWal() && !tableToken.isMatView()) {
                    makeComposite(engine, executionContext, tableToken);
                }
            }
        } catch (EntryUnavailableException e) {
            // Another writer holds the table - leave it plain rather than fail the test.
            LOG.info().$("composite randomiser skipped a busy table [msg=").$safe(e.getFlyweightMessage()).I$();
        } catch (Exception e) {
            throw new AssertionError("composite partition randomiser failed", e);
        } finally {
            // Hand back the readers and writers this sweep took from the pool. QueryAssertion runs
            // its own leak check around a single assertion, a narrower window than the test's, and
            // a pooled entry the sweep opened inside it would otherwise be counted as the test's
            // own leaked native memory.
            // Hand back the readers, writers and sequencers this sweep took from the pool, so a
            // leak check that closes before the test's own teardown does not count them.
            engine.releaseInactive();
            sweeping.set(Boolean.FALSE);
        }
    }

    /**
     * {@link #sweep} after a statement that may have written rows. A no-op for anything else,
     * so a test's queries and ALTERs do not pay for a sweep.
     */
    public static void sweepAfter(CairoEngine engine, SqlExecutionContext executionContext, CharSequence sqlText) {
        if (isDataStatement(sqlText)) {
            sweep(engine, executionContext);
        }
    }

    /**
     * Whether {@code sqlText} can have changed a table, and so needs the WAL queue drained before
     * anything reads it. ALTER and UPDATE count: on a WAL table they are transactions like any
     * other, and skipping the drain leaves, say, an added column invisible to the next query.
     */
    private static boolean isDataStatement(CharSequence sqlText) {
        final String trimmed = sqlText.toString().trim();
        return Chars.startsWithIgnoreCase(trimmed, "create table")
                || Chars.startsWithIgnoreCase(trimmed, "insert")
                || Chars.startsWithIgnoreCase(trimmed, "alter")
                || Chars.startsWithIgnoreCase(trimmed, "update")
                || Chars.startsWithIgnoreCase(trimmed, "drop");
    }

    private static void makeComposite(CairoEngine engine, SqlExecutionContext executionContext, TableToken tableToken) throws Exception {
        final long rangeLo;
        final long rangeHi;
        final int tsIndex;
        final String tsColumnName;
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            tsIndex = metadata.getTimestampIndex();
            // A cut needs a designated timestamp, real partitions, and enough rows to
            // leave something on both sides of the hole.
            if (tsIndex < 0 || !PartitionBy.isPartitioned(metadata.getPartitionBy()) || reader.size() < 4) {
                return;
            }
            final TxReader txReader = reader.getTxFile();
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.isPartitionComposite(i)) {
                    // Already composite - the shape this randomiser exists to produce is there.
                    return;
                }
            }
            if (!attempted.add(tableToken.getDirName())) {
                // Tried once and it did not take. Trying again would not either, and every retry
                // pays for a scratch table.
                return;
            }
            final long minTimestamp = txReader.getMinTimestamp();
            final long maxTimestamp = txReader.getMaxTimestamp();
            // The middle half, so the first and last partitions keep rows on both sides of
            // the hole and come back as a front/back pair rather than a whole rewrite.
            final long quarter = (maxTimestamp - minTimestamp) / 4;
            if (quarter < 1) {
                return;
            }
            rangeLo = minTimestamp + quarter;
            rangeHi = maxTimestamp - quarter;
            tsColumnName = metadata.getColumnName(tsIndex);
            timestampDriver = ColumnType.getTimestampDriver(metadata.getColumnType(tsIndex));
        }

        final StringSink lo = new StringSink();
        timestampDriver.append(lo, rangeLo);
        final StringSink hi = new StringSink();
        timestampDriver.append(hi, rangeHi);

        // Stage the range's own rows into the WAL writer and commit them back over the range they
        // came from, all in ONE transaction. Doing it as stash-table / cut / reinsert would work
        // too, but it costs three transactions plus a CREATE and DROP - the transactions are
        // visible to anything that reads a table's txn number (mat view refresh state, for one),
        // and the scratch table's own pooled writer holds native path memory that a test's
        // leak check then attributes to the test.
        final String select = "SELECT * FROM '" + tableToken.getTableName() + "' WHERE "
                + tsColumnName + " >= '" + lo + "' AND " + tsColumnName + " < '" + hi + "'";
        try (
                RecordCursorFactory factory = engine.select(select, executionContext);
                WalWriter walWriter = engine.getWalWriter(tableToken)
        ) {
            final RecordMetadata cursorMetadata = factory.getMetadata();
            final TableRecordMetadata writerMetadata = walWriter.getMetadata();
            final EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(writerMetadata.getColumnCount());
            final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                    new BytecodeAssembler(),
                    cursorMetadata,
                    writerMetadata,
                    columnFilter,
                    engine.getConfiguration()
            );
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    final TableWriter.Row row = walWriter.newRow(record.getTimestamp(tsIndex));
                    copier.copy(executionContext, record, row);
                    row.append();
                }
            }
            walWriter.commitWithParams(rangeLo, rangeHi, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
        }
        injectedTxn.merge(tableToken.getDirName(), 1, Integer::sum);
        TestUtils.drainWalQueue(engine);
    }
}
