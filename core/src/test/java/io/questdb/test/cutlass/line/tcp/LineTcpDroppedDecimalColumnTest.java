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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.line.tcp.LineTcpEventBuffer;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Non-WAL ILP rows carrying a value for a column the writer has already dropped are skipped
 * one entity at a time, using {@link LineTcpEventBuffer#columnValueLength(byte, long)}. When that
 * fails the whole row is cancelled, so the length must match what the writer put in the buffer.
 */
public class LineTcpDroppedDecimalColumnTest {
    private static final int BUF_SIZE = 512;

    @Test
    public void testDecimalValueLengthMatchesWrittenBytes() {
        final long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            final LineTcpEventBuffer buffer = new LineTcpEventBuffer(buf, BUF_SIZE);
            final Decimal256 decimal = new Decimal256();
            decimal.of(1, 2, 3, 4, 5);
            assertSkippedEntitySize(buffer, buf, decimal, ColumnType.getDecimalType(76, 5));
            decimal.ofNull();
            assertSkippedEntitySize(buffer, buf, decimal, ColumnType.getDecimalType(10, 2));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // the entity type byte is consumed before the value length is queried
    private static void assertSkippedEntitySize(LineTcpEventBuffer buffer, long lo, Decimal256 decimal, int columnType) {
        final long hi = buffer.addDecimal(lo, decimal, columnType);
        Assert.assertEquals(LineTcpParser.ENTITY_TYPE_DECIMAL, buffer.readByte(lo));
        final long valueLo = lo + Byte.BYTES;
        Assert.assertEquals(hi, valueLo + buffer.columnValueLength(LineTcpParser.ENTITY_TYPE_DECIMAL, valueLo));
    }
}
