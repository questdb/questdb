/*******************************************************************************
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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.std.IntList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for {@link LiveViewInMemoryBuffer#appendStaging} - the transactional fast-path append the
 * refresh worker uses to grow the published in-mem slot in place, under the writer sentinel, while
 * readers spin on it.
 * <p>
 * A native OOM part-way through the var-size append leaves that column's aux/data
 * append cursors advanced while {@code rowCount} has not moved yet. Without a
 * rewind the next append trips the order assert (aux cursor ==
 * {@code rowCount * auxWidth}) under {@code -ea}, and reads a torn / out-of-bounds
 * varchar with assertions disabled. {@code appendStaging} goes through
 * {@link LiveViewInMemoryBuffer#copyRowsFromWithRollback}, which rewinds the cursors so a failed
 * append is a true no-op.
 * <p>
 * These tests drive {@code appendStaging} - the method the worker actually calls - rather than
 * {@code copyRowsFromWithRollback} directly. That distinction is the point: when the rollback lived
 * only at the worker's call site, reverting that one call to the plain {@code copyRowsFrom} left
 * this file green, so the tests protected the wrapper but not the thing that depends on it.
 * <p>
 * The values are longer than {@link io.questdb.cairo.VarcharTypeDriver#VARCHAR_MAX_BYTES_FULLY_INLINED}
 * so each row writes a real {@code dataMem} payload, exercising both the aux and
 * the data cursor rewind (a fully-inlined value would move only the aux cursor).
 */
public class LiveViewInMemoryBufferRollbackTest extends AbstractCairoTest {

    private static final long PAGE_SIZE = 1024;
    private static final long STAGING_MIN_TS = 1_700_000_000_000_000L;

    @Test
    public void testAppendStagingRestoresCursorsAfterFailedAppend() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(varcharSchema(), 0, PAGE_SIZE);
                    LiveViewInMemoryBuffer retained = values("retained-row-0", "retained-row-1");
                    LiveViewInMemoryBuffer poison = throwingValues(1, "poison-row-0", "poison-row-1", "poison-row-2");
                    LiveViewInMemoryBuffer fresh = values("fresh-row-0", "fresh-row-1")
            ) {
                dst.appendStaging(retained, STAGING_MIN_TS);
                Assert.assertEquals(2, dst.rowCount());
                Assert.assertEquals("first append seeds the slot's seam", STAGING_MIN_TS, dst.seamTs());

                try {
                    dst.appendStaging(poison, STAGING_MIN_TS + 1);
                    Assert.fail("injected failure expected");
                } catch (InjectedException expected) {
                    // rollback rewinds the partially-advanced aux/data cursors
                }

                // Retained rows are intact and rowCount is unchanged - a true no-op. This is what the
                // worker's error path assumes when it drops the write sentinel without publishing.
                Assert.assertEquals(2, dst.rowCount());
                Assert.assertEquals("retained-row-0", dst.getVarcharA(0, 0).toString());
                Assert.assertEquals("retained-row-1", dst.getVarcharA(1, 0).toString());
                Assert.assertEquals("a failed append must not move the seam", STAGING_MIN_TS, dst.seamTs());

                // A subsequent real append must NOT trip the order assert and must read
                // back correctly - no torn / out-of-bounds varchar from the poison run.
                dst.appendStaging(fresh, STAGING_MIN_TS + 2);
                Assert.assertEquals(4, dst.rowCount());
                Assert.assertEquals("retained-row-0", dst.getVarcharA(0, 0).toString());
                Assert.assertEquals("retained-row-1", dst.getVarcharA(1, 0).toString());
                Assert.assertEquals("fresh-row-0", dst.getVarcharA(2, 0).toString());
                Assert.assertEquals("fresh-row-1", dst.getVarcharA(3, 0).toString());
                // A non-empty slot keeps its seam: staging rows are strictly newer.
                Assert.assertEquals(STAGING_MIN_TS, dst.seamTs());
            }
        });
    }

    @Test
    public void testFailedAppendWithoutRollbackPoisonsCursors() throws Exception {
        // Control: the same partial append WITHOUT the rollback wrapper leaves the
        // var-size aux cursor advanced past rowCount, so the next append trips the
        // order assert under -ea. This is exactly the poison appendStaging prevents,
        // and it is why appendStaging must not be "simplified" to a plain copyRowsFrom.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(varcharSchema(), 0, PAGE_SIZE);
                    LiveViewInMemoryBuffer retained = values("retained-row-0", "retained-row-1");
                    LiveViewInMemoryBuffer poison = throwingValues(1, "poison-row-0", "poison-row-1", "poison-row-2");
                    LiveViewInMemoryBuffer fresh = values("fresh-row-0", "fresh-row-1")
            ) {
                dst.copyRowsFrom(retained, 0, 2, 0);
                dst.setRowCount(2);

                try {
                    dst.copyRowsFrom(poison, 0, 3, 2); // no rollback wrapper
                    Assert.fail("injected failure expected");
                } catch (InjectedException expected) {
                    // one var row was appended before the throw; rowCount is still 2
                }

                // The advanced aux cursor makes the next append's order assert fail.
                Assert.assertThrows(AssertionError.class, () -> dst.copyRowsFrom(fresh, 0, 2, 2));
            }
        });
    }

    private static LiveViewInMemoryBuffer throwingValues(int failRow, String... vals) {
        return new SyntheticVarcharBuffer(failRow, vals);
    }

    // A single-VARCHAR-column source buffer that serves the given values through
    // getVarcharA. It holds no real rows: copyRowsFrom reads a var column purely
    // through the getter, so overriding the getter is enough to feed a copy.
    private static LiveViewInMemoryBuffer values(String... vals) {
        return new SyntheticVarcharBuffer(-1, vals);
    }

    private static IntList varcharSchema() {
        IntList types = new IntList();
        types.add(ColumnType.VARCHAR);
        return types;
    }

    private static final class InjectedException extends RuntimeException {
        InjectedException() {
            super("injected native OOM");
        }
    }

    private static final class SyntheticVarcharBuffer extends LiveViewInMemoryBuffer {
        private final int failRow;
        private final String[] vals;

        SyntheticVarcharBuffer(int failRow, String[] vals) {
            super(varcharSchema(), 0, PAGE_SIZE);
            this.failRow = failRow;
            this.vals = vals;
            // No rows are ever written into this buffer - copyRowsFrom reads a var column purely
            // through getVarcharA, which is overridden below. But appendStaging takes its row count
            // from the staging buffer, so the count has to be real even though the storage is not.
            setRowCount(vals.length);
        }

        @Override
        public Utf8Sequence getVarcharA(long row, int col) {
            if (row == failRow) {
                throw new InjectedException();
            }
            return new Utf8String(vals[(int) row]);
        }
    }
}
