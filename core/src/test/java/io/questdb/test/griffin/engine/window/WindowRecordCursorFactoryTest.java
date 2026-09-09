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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WindowRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testIncrementalFirstOpenRollsBackPartialReopen() throws Exception {
        // The very first getIncrementalCursor() doubles as the bootstrap: it reopens every
        // window function's (lazy) partition map. If the Nth reopen throws, the earlier
        // functions' native maps are already allocated while the later ones stay closed, and
        // ofIncremental() has already flipped isOpen=true. Without a transactional rollback a
        // retry would take the state-preserving branch, skip reopen, and drive computeNext over
        // a never-reopened (closed) map. The first open must roll back every partially reopened
        // function so a retry re-bootstraps from a clean slate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            try (RecordCursorFactory base = select("SELECT x FROM t")) {
                final CountingWindowFunction a = new CountingWindowFunction("a");
                final CountingWindowFunction b = new CountingWindowFunction("b");
                b.failReopen = true;

                final ObjList<Function> functions = new ObjList<>();
                functions.add(a);
                functions.add(b);
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("a", ColumnType.LONG));
                metadata.add(new TableColumnMetadata("b", ColumnType.LONG));

                // The factory owns base and the functions; closing it frees them.
                try (WindowRecordCursorFactory factory = new WindowRecordCursorFactory(base, metadata, functions, null)) {
                    // First open: b's reopen throws. The failure must propagate AND leave a
                    // clean slate: a was reopened then reset (its map freed by the rollback),
                    // and isOpen was cleared.
                    try {
                        factory.getIncrementalCursor(base.getCursor(sqlExecutionContext), sqlExecutionContext);
                        Assert.fail("expected the injected reopen failure to propagate");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected reopen failure");
                    }
                    Assert.assertEquals("a must have been reopened once", 1, a.reopenCount);
                    Assert.assertEquals("b's reopen was attempted", 1, b.reopenCount);
                    Assert.assertEquals("the failed first open must reset (free) a's partially reopened map", 1, a.resetCount);
                    Assert.assertEquals("b is reset on rollback too", 1, b.resetCount);

                    // Retry with the failure cleared. Because the first open rolled back to the
                    // closed state, this call re-enters the bootstrap branch and reopens BOTH
                    // functions again (rather than skipping reopen and touching a closed map).
                    b.failReopen = false;
                    try (RecordCursor cursor = factory.getIncrementalCursor(base.getCursor(sqlExecutionContext), sqlExecutionContext)) {
                        Assert.assertEquals("the retry re-bootstraps and reopens a", 2, a.reopenCount);
                        Assert.assertEquals("the retry re-bootstraps and reopens b", 2, b.reopenCount);
                        Assert.assertFalse("empty base yields no rows", cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testLiveViewRestoreOpenRollsBackPartialReopen() throws Exception {
        // openForLiveViewRestore() is the checkpoint-restore first-open path: it also flips
        // isOpen=true and reopens every window function's map. A failing reopen there must roll
        // back the same way, so the caller's rebuild re-bootstraps rather than skipping reopen
        // over a closed map on the next getIncrementalCursor().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            try (RecordCursorFactory base = select("SELECT x FROM t")) {
                final CountingWindowFunction a = new CountingWindowFunction("a");
                final CountingWindowFunction b = new CountingWindowFunction("b");
                b.failReopen = true;

                final ObjList<Function> functions = new ObjList<>();
                functions.add(a);
                functions.add(b);
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("a", ColumnType.LONG));
                metadata.add(new TableColumnMetadata("b", ColumnType.LONG));

                try (WindowRecordCursorFactory factory = new WindowRecordCursorFactory(base, metadata, functions, null)) {
                    try {
                        factory.openForLiveViewRestore(sqlExecutionContext);
                        Assert.fail("expected the injected reopen failure to propagate");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected reopen failure");
                    }
                    Assert.assertEquals("a must have been reopened once", 1, a.reopenCount);
                    Assert.assertEquals("the failed restore open must reset (free) a's partially reopened map", 1, a.resetCount);

                    // The rebuild path re-bootstraps: a fresh open reopens both functions again.
                    b.failReopen = false;
                    try (RecordCursor cursor = factory.getIncrementalCursor(base.getCursor(sqlExecutionContext), sqlExecutionContext)) {
                        Assert.assertEquals("the rebuild re-bootstraps and reopens a", 2, a.reopenCount);
                        Assert.assertEquals("the rebuild re-bootstraps and reopens b", 2, b.reopenCount);
                        Assert.assertFalse("empty base yields no rows", cursor.hasNext());
                    }
                }
            }
        });
    }

    // A minimal window function whose reopen() can be told to throw and that counts its reopen /
    // reset calls, so a test can observe whether a failed first open rolled back (reset) the
    // functions it had already reopened. Holds no native state of its own.
    private static class CountingWindowFunction extends BaseWindowFunction implements Reopenable {
        private final String name;
        private boolean failReopen;
        private int reopenCount;
        private int resetCount;

        private CountingWindowFunction(String name) {
            super(null);
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public void reopen() {
            reopenCount++;
            if (failReopen) {
                throw CairoException.nonCritical().put("injected reopen failure");
            }
        }

        @Override
        public void reset() {
            resetCount++;
        }
    }
}
