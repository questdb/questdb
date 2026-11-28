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

import io.questdb.cairo.TableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.MethodModel;

/**
 * Every row that {@link TableWriter} appends goes through {@link TableWriter#newRow(long)}, which calls the private
 * newRow0(). The HotSpot C2 JIT inlines a method into a hot call site only while its bytecode is at most
 * FreqInlineSize bytes long. When newRow0() grows past that, C2 compiles a call for every row, and appends to
 * non-WAL tables get about 1 ns per row slower.
 */
public class TableWriterNewRowInliningTest {
    // The default of the HotSpot flag FreqInlineSize on x86-64 and AArch64
    private static final int FREQ_INLINE_SIZE = 325;

    @Test
    public void testNewRow0FitsFreqInlineSize() throws IOException {
        final int size = codeLength(TableWriter.class, "newRow0", "(J)Lio/questdb/cairo/TableWriter$Row;");
        Assert.assertTrue(
                "TableWriter.newRow0() has " + size + " bytes of bytecode, more than the " + FREQ_INLINE_SIZE
                        + " the JIT inlines into newRow(). Move code that rarely runs into helpers, as"
                        + " newRowOpenPartition() and newRowSwitchPartition() do.",
                size <= FREQ_INLINE_SIZE
        );
    }

    private static int codeLength(Class<?> clazz, String methodName, String descriptor) throws IOException {
        try (InputStream in = clazz.getResourceAsStream(clazz.getSimpleName() + ".class")) {
            Assert.assertNotNull(in);
            for (MethodModel method : ClassFile.of().parse(in.readAllBytes()).methods()) {
                if (method.methodName().equalsString(methodName) && method.methodType().equalsString(descriptor)) {
                    return method.findAttribute(Attributes.code()).orElseThrow().codeLength();
                }
            }
        }
        throw new AssertionError("no method " + methodName + descriptor + " in " + clazz.getName());
    }
}
