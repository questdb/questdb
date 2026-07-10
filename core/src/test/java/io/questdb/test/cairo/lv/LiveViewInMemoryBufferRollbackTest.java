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
 * Coverage for {@link LiveViewInMemoryBuffer#copyRowsFromWithRollback} - the
 * transactional fast-path append the refresh worker uses to grow the published
 * in-mem slot in place.
 * <p>
 * A native OOM part-way through the var-size append leaves that column's aux/data
 * append cursors advanced while {@code rowCount} has not moved yet. Without a
 * rewind the next append trips the order assert (aux cursor ==
 * {@code rowCount * auxWidth}) under {@code -ea}, and reads a torn / out-of-bounds
 * varchar with assertions disabled. The rollback wrapper rewinds the cursors so a
 * failed append is a true no-op.
 * <p>
 * The values are longer than {@link io.questdb.cairo.VarcharTypeDriver#VARCHAR_MAX_BYTES_FULLY_INLINED}
 * so each row writes a real {@code dataMem} payload, exercising both the aux and
 * the data cursor rewind (a fully-inlined value would move only the aux cursor).
 */
public class LiveViewInMemoryBufferRollbackTest extends AbstractCairoTest {

    private static final long PAGE_SIZE = 1024;

    @Test
    public void testFailedAppendWithoutRollbackPoisonsCursors() throws Exception {
        // Control: the same partial append WITHOUT the rollback wrapper leaves the
        // var-size aux cursor advanced past rowCount, so the next append trips the
        // order assert under -ea. This is exactly the poison the rollback prevents.
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

    @Test
    public void testRollbackRestoresCursorsAfterFailedAppend() throws Exception {
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
                    dst.copyRowsFromWithRollback(poison, 0, 3, 2);
                    Assert.fail("injected failure expected");
                } catch (InjectedException expected) {
                    // rollback rewinds the partially-advanced aux/data cursors
                }

                // Retained rows are intact and rowCount is unchanged - a true no-op.
                Assert.assertEquals(2, dst.rowCount());
                Assert.assertEquals("retained-row-0", dst.getVarcharA(0, 0).toString());
                Assert.assertEquals("retained-row-1", dst.getVarcharA(1, 0).toString());

                // A subsequent real append must NOT trip the order assert and must read
                // back correctly - no torn / out-of-bounds varchar from the poison run.
                dst.copyRowsFromWithRollback(fresh, 0, 2, 2);
                dst.setRowCount(4);
                Assert.assertEquals("retained-row-0", dst.getVarcharA(0, 0).toString());
                Assert.assertEquals("retained-row-1", dst.getVarcharA(1, 0).toString());
                Assert.assertEquals("fresh-row-0", dst.getVarcharA(2, 0).toString());
                Assert.assertEquals("fresh-row-1", dst.getVarcharA(3, 0).toString());
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
