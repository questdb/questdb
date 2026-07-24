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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InVarcharArrayFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testMalformedVarcharArrayDoesNotMatchDecodedPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (str STRING, sym SYMBOL)");
            execute("""
                    INSERT INTO x VALUES
                        ('abc', 'abc'),
                        (NULL, NULL)
                    """);
            sqlExecutionContext.getBindVariableService().setArray(
                    0,
                    new TestVarcharArray(new Utf8String(new byte[]{'a', 'b', 'c', (byte) 0xC3}, false))
            );

            assertQuery("SELECT str IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            false
                            """);
            assertQuery("SELECT cast(str AS symbol) IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            false
                            """);
            assertQuery("SELECT sym IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            false
                            """);
        });
    }

    @Test
    public void testNullVarcharArrayElementMatchesNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (str STRING, sym SYMBOL)");
            execute("""
                    INSERT INTO x VALUES
                        ('abc', 'abc'),
                        (NULL, NULL)
                    """);
            sqlExecutionContext.getBindVariableService().setArray(
                    0,
                    new TestVarcharArray(
                            new Utf8String(new byte[]{'a', 'b', 'c', (byte) 0xC3}, false),
                            null
                    )
            );

            assertQuery("SELECT str IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            true
                            """);
            assertQuery("SELECT cast(str AS symbol) IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            true
                            """);
            assertQuery("SELECT sym IN ($1) matched FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            true
                            """);
        });
    }

    private static class TestVarcharArray extends ArrayView {

        private TestVarcharArray(Utf8Sequence... values) {
            flatView = new FlatArrayView() {
                @Override
                public void appendToMemFlat(MemoryA mem, int offset, int length) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public double getDoubleAtAbsIndex(int elemIndex) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long getLongAtAbsIndex(int elemIndex) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Utf8Sequence getVarcharAt(int index) {
                    return values[index];
                }

                @Override
                public int length() {
                    return values.length;
                }
            };
            flatViewLength = values.length;
            type = ColumnType.encodeArrayType(ColumnType.VARCHAR, 1, false);
        }
    }
}
