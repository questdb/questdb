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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.NativeWriterRow;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NativeWriterRowTest {
    @Test
    public void testLong256Utf8IntoOwnedMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryCARW data = Vm.getCARWInstance(64, 8, MemoryTag.NATIVE_DEFAULT);
                 DirectUtf8Sink text = new DirectUtf8Sink(128)) {
                final String hex = "0x0000000000000004000000000000000300000000000000020000000000000001";
                final MemoryRow row = new MemoryRow(data);
                text.putAscii(hex);
                row.putLong256Utf8(0, text);
                row.putLong256Utf8(0, (Utf8Sequence) text);
                row.putLong256Utf8(0, new Utf8String(hex));
                text.clear();
                row.putLong256Utf8(0, text);
                row.putLong256Utf8(0, (DirectUtf8Sequence) null);
                row.putLong256Utf8(0, (Utf8Sequence) null);

                Assert.assertEquals(6, row.written);
                Assert.assertEquals(6 * 32, data.getAppendOffset());
                for (int r = 0; r < 6; r++) {
                    for (int limb = 0; limb < 4; limb++) {
                        Assert.assertEquals(r < 3 ? limb + 1 : Numbers.LONG_NULL, data.getLong(r * 32L + limb * 8L));
                    }
                }

                text.putAscii("0xG");
                try {
                    row.putLong256Utf8(0, text);
                    Assert.fail("invalid hexadecimal value must fail before appending");
                } catch (ImplicitCastException expected) {
                    Assert.assertEquals(6, row.written);
                    Assert.assertEquals(6 * 32, data.getAppendOffset());
                }
            }
        });
    }

    private static class MemoryRow extends NativeWriterRow {
        private final MemoryA data;
        private int written;

        private MemoryRow(MemoryA data) {
            this.data = data;
        }

        @Override
        public void append() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putSym(int columnIndex, char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int getColumnType(int columnIndex) {
            return ColumnType.LONG256;
        }

        @Override
        protected MemoryA getPrimaryColumn(int columnIndex) {
            return data;
        }

        @Override
        protected MemoryA getSecondaryColumn(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void setRowValueNotNull(int columnIndex) {
            Assert.assertEquals(0, columnIndex);
            written++;
        }
    }
}
