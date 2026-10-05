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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Trap guard for the append-scoped msync narrowing/skip change: the memories that are NOT marked
 * appendOnly must keep their full-extent SYNC durability. This exercises, across many SYNC commits:
 * <ul>
 *   <li>{@code _txn} / {@code _cv} — in-place updated metadata files (never appendOnly);</li>
 *   <li>a SYMBOL column with NULL values — drives {@code SymbolMapWriter.offsetMem.updateNullFlag()},
 *       an in-place {@code putBool(0,..)} (offset mem stays full-extent);</li>
 *   <li>an INDEXED symbol column — drives the bitmap index {@code .k}/{@code .v} files, whose entries
 *       are random-access writes below the high-water mark (stay full-extent).</li>
 * </ul>
 * After a crash following a LATE commit, every committed row must survive (zero loss). If the
 * narrowing change had wrongly narrowed/skipped these in-place memories, the offset mem null flag,
 * the index, or the _txn/_cv state would be lost and the row count / values would not reconcile.
 */
public class NonAppendOnlyMemsStaySyncedCrashTest extends AbstractCrashConsistencyTest {

    @Test
    public void testInPlaceAndNonAppendOnlyStaySyncedAcrossManyCommits() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());

            runWithCrashFacade(() -> {
                // s   : indexed SYMBOL -> exercises bitmap .k/.v (random-access, full-extent sync)
                // g   : SYMBOL allowed to be NULL -> exercises offsetMem.updateNullFlag in-place putBool
                // v   : plain STRING payload we read back for an exact value-by-value durability check
                execute("create table t (" +
                        "ts timestamp, " +
                        "s symbol index, " +
                        "g symbol, " +
                        "v string" +
                        ") timestamp(ts) partition by none");

                // Seed one row and mark the on-disk state durable: the table STRUCTURE (_meta and the
                // freshly created files) is "prior, already journaled" so the reader can open after the
                // crash. The rows we actually test for durability are the ones committed AFTER this
                // baseline -- their survival must come purely from the per-commit SYNC fdatasync/msync.
                execute("insert into t values (0, 'sym0', 'grp0', 'val-seed')");
                markDurableBaseline();

                // Many SEPARATE commits: each insert is its own transaction, so _txn/_cv are updated
                // in place repeatedly and every column/symbol/index memory is sync()'d on each commit.
                final int rows = 240;
                final List<String> expectedV = new ArrayList<>();
                expectedV.add("val-seed");
                for (int i = 1; i < rows; i++) {
                    final String v = "val-" + String.format("%06d", i);
                    // Every 4th row stores a NULL symbol in g -> sets the offset-mem null flag in place.
                    final String g = (i % 4 == 0) ? "null" : "'grp" + (i % 7) + "'";
                    execute("insert into t values (" +
                            (i * 1_000_000L) + ", " +
                            "'sym" + (i % 5) + "', " +
                            g + ", " +
                            "'" + v + "')");
                    expectedV.add(v);
                }

                // Crash AFTER the late commits (no markDurableBaseline -> nothing is "pre-journaled";
                // durability here must come entirely from the SYNC fdatasync/msync on each commit).
                crashAndReopen();

                // Bar 2: every committed row present and correct -> the non-appendOnly memories were
                // fully synced. A regression that narrowed/skipped them would drop the tail or the
                // index/null-flag state and fail this reconciliation.
                assertSyncDurable("t", "v", expectedV);

                // The indexed symbol column must still resolve via its (.k/.v) bitmap index after crash:
                // an index-driven query for every distinct key must return all rows, proving the index
                // was made durable. Total over the 5 keys must equal the row count.
                long viaIndex = 0;
                for (int k = 0; k < 5; k++) {
                    viaIndex += count("select count(*) from t where s = 'sym" + k + "'");
                }
                Assert.assertEquals("indexed symbol rows must all survive via .k/.v index", rows, viaIndex);

                // The nullable symbol column must have exactly the NULL rows we wrote (offset-mem null
                // flag + null entries durable). The seed row (i==0) is non-null; the loop writes a NULL
                // whenever i % 4 == 0 for i in [1, rows).
                long expectedNulls = 0;
                for (int i = 1; i < rows; i++) {
                    if (i % 4 == 0) {
                        expectedNulls++;
                    }
                }
                Assert.assertEquals("null-symbol rows must survive (offset mem stayed full-synced)",
                        expectedNulls, count("select count(*) from t where g is null"));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    private long count(String sql) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select(sql)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                if (c.hasNext()) {
                    return c.getRecord().getLong(0);
                }
                return 0;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException("count failed for: " + sql, e);
        }
    }
}
