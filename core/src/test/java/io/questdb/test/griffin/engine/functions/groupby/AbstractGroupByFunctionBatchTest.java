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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.After;

/**
 * Fixture shared by the per-type {@code *GroupByFunctionBatchTest} classes, which all drive
 * {@code GroupByFunction.computeBatch()} over a hand-built native column of values.
 * <p>
 * Two invariants shape most of what those tests assert, so they are stated here once rather than
 * at each of the sites that pins them:
 * <p>
 * <b>Frames reach a worker's slot out of order.</b> A batch can therefore arrive at a LOWER rowId
 * than the one the accumulator already holds, which is why a batch carries an explicit start rowId
 * rather than implying one. {@code last_not_null} wins by the highest rowId, so a stored non-null
 * has to survive such a batch: the rowId comparison alone says no, and the accumulator is not
 * empty. Only a stored NULL is replaceable from below.
 * <p>
 * <b>The row-by-row fallback for a frame with column tops calls {@code computeFirst}</b>, which
 * writes through unconditionally - NULL included - leaving a real rowId next to a NULL value. A
 * later frame's non-null at a LOWER rowId must still replace that stored NULL, as
 * {@code computeNext} already does.
 * <p>
 * The allocators below hand back a raw address into a single buffer that lives until the next
 * allocate call or the end of the test, whichever comes first - one live allocation at a time, so
 * a test that forgets to free cannot leak past {@link #tearDown()}.
 * <p>
 * Narrow unit tests: no engine, no table, so they need no assertMemoryLeak. {@link #tearDown()}
 * is the leak check.
 */
public abstract class AbstractGroupByFunctionBatchTest {
    /**
     * Deliberately not 0: a function that ignored its column index and read column 0 would still
     * pass every test below if the tests used 0 themselves.
     */
    protected static final int COLUMN_INDEX = 789;
    private long lastAllocated;
    private long lastSize;

    @After
    public void tearDown() {
        freeLast();
    }

    protected long allocateBooleans(boolean... values) {
        final long addr = allocate((long) values.length * Byte.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putByte(addr + i, values[i] ? (byte) 1 : 0);
        }
        return addr;
    }

    protected long allocateBytes(byte... values) {
        final long addr = allocate((long) values.length * Byte.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putByte(addr + i, values[i]);
        }
        return addr;
    }

    protected long allocateChars(char... values) {
        final long addr = allocate((long) values.length * Character.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putChar(addr + (long) i * Character.BYTES, values[i]);
        }
        return addr;
    }

    protected long allocateDoubles(double... values) {
        final long addr = allocate((long) values.length * Double.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putDouble(addr + (long) i * Double.BYTES, values[i]);
        }
        return addr;
    }

    protected long allocateFloats(float... values) {
        final long addr = allocate((long) values.length * Float.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putFloat(addr + (long) i * Float.BYTES, values[i]);
        }
        return addr;
    }

    protected long allocateInts(int... values) {
        final long addr = allocate((long) values.length * Integer.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putInt(addr + (long) i * Integer.BYTES, values[i]);
        }
        return addr;
    }

    protected long allocateLongs(long... values) {
        final long addr = allocate((long) values.length * Long.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putLong(addr + (long) i * Long.BYTES, values[i]);
        }
        return addr;
    }

    protected long allocateShorts(short... values) {
        final long addr = allocate((long) values.length * Short.BYTES);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putShort(addr + (long) i * Short.BYTES, values[i]);
        }
        return addr;
    }

    protected void freeLast() {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
            lastAllocated = 0;
            lastSize = 0;
        }
    }

    protected SimpleMapValue prepare(GroupByFunction function) {
        var columnTypes = new ArrayColumnTypes();
        function.initValueTypes(columnTypes);
        SimpleMapValue value = new SimpleMapValue(columnTypes.getColumnCount());
        function.initValueIndex(0);
        function.setEmpty(value);
        return value;
    }

    /**
     * Frees whatever the previous allocate call handed out, then allocates a fresh buffer. An
     * empty batch allocates nothing and reads back as address 0, which is what a reducer sees for
     * a frame with no rows.
     */
    private long allocate(long size) {
        freeLast();
        if (size == 0) {
            return 0;
        }
        lastSize = size;
        lastAllocated = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        return lastAllocated;
    }
}
