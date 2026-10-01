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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CompiledFilterTimestampPrecisionTest extends AbstractCairoTest {
    @Test
    public void testMixedPrecisionLiteralFallsBackWithoutTruncation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String bound = "'2020-01-01T00:00:00.000000001Z'";
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other<" + bound, "id\n1\n3\n", false);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE " + bound + ">other", "id\n1\n3\n", false);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other=" + bound, "id\n", false);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other>" + bound, "id\n2\n", false);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other<=" + bound, "id\n1\n3\n", false);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other>=" + bound, "id\n2\n", false);
            // The designated column still uses its existing intrinsic tick precision.
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE ts<" + bound, "id\n", false);
        });
    }

    @Test
    public void testSamePrecisionComparisonsStillCompile() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE other<'2020-01-01T00:00:00.000001Z'",
                    "id\n1\n3\n", true);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE nano<'2020-01-01T00:00:00.000000001Z'",
                    "id\n1\n", true);
            assertAllModes("SELECT id FROM jit_timestamp_precision WHERE nano='2020-01-01T00:00:00.000000001Z'",
                    "id\n3\n", true);
        });
    }

    private void assertAllModes(String sql, String expected, boolean isSamePrecision) throws Exception {
        final int previousMode = sqlExecutionContext.getJitMode();
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertFalse(factory.usesCompiledFilter());
                assertResult(factory, expected);
            }
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(isSamePrecision && JitUtil.isJitSupported(), factory.usesCompiledFilter());
                assertResult(factory, expected);
            }
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertResult(factory, expected);
            }
        } finally {
            sqlExecutionContext.setJitMode(previousMode);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp()
                .sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE jit_timestamp_precision(id INT,ts TIMESTAMP,other TIMESTAMP,nano TIMESTAMP_NS) TIMESTAMP(ts)");
        execute("""
                INSERT INTO jit_timestamp_precision VALUES
                (1,'2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','2020-01-01T00:00:00.000000000Z'),
                (2,'2020-01-01T00:00:01Z','2020-01-01T00:00:00.000001Z','2020-01-01T00:00:00.000000002Z'),
                (3,'2020-01-01T00:00:02Z','2020-01-01T00:00:00Z','2020-01-01T00:00:00.000000001Z'),
                (4,'2020-01-01T00:00:03Z',null,null)
                """);
    }
}
