/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EmbeddedApiTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testReadWrite() throws Exception {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(temp.getRoot().getAbsolutePath());

        TestUtils.assertMemoryLeak(() -> {
            // the write part
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
                try (SqlCompiler compiler = new SqlCompiler(engine)) {

                    compiler.compile("create table abc (a int, b byte, c short, d long, e float, g double, h date, i symbol, j string, k boolean, ts timestamp) timestamp(ts)", ctx);

                    try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "abc")) {
                        for (int i = 0; i < 10; i++) {
                            TableWriter.Row row = writer.newRow(Os.currentTimeMicros());
                            row.putInt(0, 123);
                            row.putByte(1, (byte) 1111);
                            row.putShort(2, (short) 222);
                            row.putLong(3, 333);
                            row.putFloat(4, 4.44f);
                            row.putDouble(5, 5.55);
                            row.putDate(6, System.currentTimeMillis());
                            row.putSym(7, "xyz");
                            row.putStr(8, "abc");
                            row.putBool(9, true);
                            row.append();
                        }
                        writer.commit();
                    }

                    try (RecordCursorFactory factory = compiler.compile("abc", ctx).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                // access 'record' instance for field values
                            }
                        }
                    }
                }
            }
        });
    }
}
