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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerProvider;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the per-query {@code MemoryTracker} propagation into the parallel
 * page-frame plumbing: a tracker bound on {@code SqlExecutionContext} at
 * workload start must surface on the {@code PageFrameSequence} and its atom
 * that workers read, and must clear when the workload ends.
 */
public class AsyncTrackerPropagationTest extends AbstractCairoTest {

    @Test
    public void testFilterFrameSequenceAndAtomCaptureTracker() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x, x AS y, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(50)) TIMESTAMP(ts)");
            drainWalQueue();

            // Pin the test to AsyncFilteredRecordCursorFactory. With JIT on, the
            // compiler may pick AsyncJitFilteredRecordCursorFactory instead, which
            // is a sibling class with its own atom type.
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT * FROM tab WHERE y > 0", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    Assert.assertTrue(factory.getBaseFactory() instanceof AsyncFilteredRecordCursorFactory);
                    final AsyncFilteredRecordCursorFactory base = (AsyncFilteredRecordCursorFactory) factory.getBaseFactory();

                    final MemoryTrackerProvider provider = engine.getMemoryTrackerProvider();
                    final MemoryTracker tracker = provider.acquire(sqlExecutionContext.getSecurityContext(), 1L, MemoryTrackerWorkload.QUERY);
                    sqlExecutionContext.setMemoryTracker(tracker);
                    try {
                        final SCSequence collectSubSeq = new SCSequence();
                        final PageFrameSequence<AsyncFilterAtom> fs = base.execute(
                                sqlExecutionContext, collectSubSeq, PartitionFrameCursorFactory.ORDER_ASC
                        );
                        try {
                            Assert.assertSame(tracker, fs.getMemoryTracker());
                            Assert.assertSame(tracker, fs.getAtom().getMemoryTracker());
                        } finally {
                            fs.await();
                            fs.reset();
                        }
                        // reset() drops the borrowed tracker reference on both the
                        // sequence and the atom, so the next acquisition starts clean.
                        Assert.assertNull(fs.getMemoryTracker());
                        Assert.assertNull(fs.getAtom().getMemoryTracker());
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                        tracker.close();
                    }
                }
            }
        });
    }

    @Test
    public void testFilterFrameSequenceCarriesNullWhenNoTrackerConfigured() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x, x AS y, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(50)) TIMESTAMP(ts)");
            drainWalQueue();

            // Pin the test to AsyncFilteredRecordCursorFactory. With JIT on, the
            // compiler may pick AsyncJitFilteredRecordCursorFactory instead, which
            // is a sibling class with its own atom type.
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT * FROM tab WHERE y > 0", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    final AsyncFilteredRecordCursorFactory base = (AsyncFilteredRecordCursorFactory) factory.getBaseFactory();

                    // No tracker set on the context: factory.execute() must observe
                    // null on both the sequence and the atom.
                    Assert.assertNull(sqlExecutionContext.getMemoryTracker());

                    final SCSequence collectSubSeq = new SCSequence();
                    final PageFrameSequence<AsyncFilterAtom> fs = base.execute(
                            sqlExecutionContext, collectSubSeq, PartitionFrameCursorFactory.ORDER_ASC
                    );
                    try {
                        Assert.assertNull(fs.getMemoryTracker());
                        Assert.assertNull(fs.getAtom().getMemoryTracker());
                    } finally {
                        fs.await();
                        fs.reset();
                    }
                }
            }
        });
    }
}
