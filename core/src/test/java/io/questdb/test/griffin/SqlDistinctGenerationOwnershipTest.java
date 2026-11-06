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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlDistinctGenerationOwnershipTest extends AbstractCairoTest {
    private static final String PAGE_TOO_SMALL = "page size is too small to fit a single key";

    @Test
    public void testConstructorFailureClosesInputsExactlyOnce() throws Exception {
        assertConstructorFailures(false);
    }

    @Test
    public void testConstructorFailurePreservesPrimaryWhenInputCloseFails() throws Exception {
        assertConstructorFailures(true);
    }

    private static void assertConstructorFailures(boolean isInputCloseFailing) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 64);
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0; i < 2; i++) {
                    final OwnershipFixture.TableSpec spec = fixture.table(
                            "ts", ColumnType.TIMESTAMP_MICRO,
                            "a", ColumnType.LONG256,
                            "b", ColumnType.LONG256,
                            "c", ColumnType.LONG256
                    ).timestamp(0);
                    spec.isRandomAccess = i == 1;
                    if (isInputCloseFailing) {
                        spec.closeFailure = new RuntimeException("distinct input close");
                    }
                    fixture.assertCompileFails(compiler, sqlExecutionContext,
                            "SELECT DISTINCT a, b, c, row_number() OVER () rn FROM owned_table(" + i + ") LIMIT owned_long(1), owned_long(3)", PAGE_TOO_SMALL);
                    Assert.assertEquals(1, fixture.tableCount());
                    Assert.assertTrue(fixture.longCount() >= 2);
                    fixture.assertCompileFails(compiler, sqlExecutionContext,
                            "SELECT DISTINCT ts, a, b, c, row_number() OVER () rn FROM owned_table(" + i + ") LIMIT owned_long(1), owned_long(3)", PAGE_TOO_SMALL);
                }
            }
        });
    }
}
