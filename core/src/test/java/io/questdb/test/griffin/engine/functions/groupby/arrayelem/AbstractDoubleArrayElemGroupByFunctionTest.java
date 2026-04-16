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

package io.questdb.test.griffin.engine.functions.groupby.arrayelem;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public abstract class AbstractDoubleArrayElemGroupByFunctionTest extends AbstractCairoTest {

    protected abstract String funcName();

    // --- shared tests: identical expected output for all 4 functions ---

    @Test
    public void testFirstRowAllNanSubsequentFinite() throws Exception {
        assertGroupBy("[1.0,2.0,3.0]",
                "ARRAY[null, null, null]",
                "ARRAY[1.0, 2.0, 3.0]"
        );
    }

    @Test
    public void testNullRowsInterleavedWithNanRows() throws Exception {
        assertGroupBy("[2.0,1.0]",
                "null",
                "ARRAY[null, 1.0]",
                "null",
                "ARRAY[2.0, null]",
                "null"
        );
    }

    @Test
    public void testSingleArgIdentityFromSubquery() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "arr\n[1.0,2.0]\n",
                "SELECT " + funcName() + "(arr) arr FROM (SELECT ARRAY[1.0, 2.0] arr)",
                null, false, true
        ));
    }

    protected void assertGroupBy(String expected, String... rows) throws Exception {
        assertGroupByTyped("DOUBLE[]", expected, rows);
    }

    protected void assertGroupByTyped(String columnType, String expected, String... rows) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr " + columnType + ")");
            for (String row : rows) {
                execute("INSERT INTO tab VALUES (" + row + ")");
            }
            assertQueryNoLeakCheck(
                    "arr\n" + expected + "\n",
                    "SELECT " + funcName() + "(arr) arr FROM tab",
                    null, false, true
            );
        });
    }

    protected void assertKeyedGroupBy(String expected, String[][] groups) throws Exception {
        assertKeyedGroupByTyped("DOUBLE[]", expected, groups);
    }

    protected void assertKeyedGroupByTyped(String columnType, String expected, String[][] groups) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp INT, arr " + columnType + ")");
            for (String[] group : groups) {
                for (int i = 1; i < group.length; i++) {
                    execute("INSERT INTO tab VALUES (" + group[0] + ", " + group[i] + ")");
                }
            }
            assertQueryNoLeakCheck(
                    expected,
                    "SELECT grp, " + funcName() + "(arr) arr FROM tab ORDER BY grp",
                    null, true, true
            );
        });
    }

    protected void assertSampleBy(String expected, String[][] timestampedRows) throws Exception {
        assertSampleByTyped("DOUBLE[]", expected, timestampedRows);
    }

    protected void assertSampleByTyped(String columnType, String expected, String[][] timestampedRows) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP NOT NULL, arr " + columnType + ") TIMESTAMP(ts) PARTITION BY DAY");
            for (String[] row : timestampedRows) {
                execute("INSERT INTO tab VALUES ('" + row[0] + "', " + row[1] + ")");
            }
            assertQueryNoLeakCheck(
                    expected,
                    "SELECT ts, " + funcName() + "(arr) arr FROM tab SAMPLE BY 1h",
                    "ts", true, true
            );
        });
    }
}
