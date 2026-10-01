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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalArrayAccessTest extends AbstractCairoTest {
    @Test
    public void testScalarSubscriptsNegativeNullAndConsecutiveDimensions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,a[1],a[2],a[-1],a[99],a[NULL::LONG],m[1,2],m[2][1],m[-1][-1] FROM lp_array_access ORDER BY id",
                    """
                            id	[]	[]1	[]2	[]3	[]4	[]5	[]6	[]7
                            1	10.0	20.0	30.0	null	null	2.0	3.0	4.0
                            2	-1.0	5.0	5.0	null	null	6.0	7.0	8.0
                            3	null	null	null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT a[2]+1.0 result FROM lp_array_access WHERE a[1]>0 ORDER BY result",
                    """
                            result
                            21.0
                            """
            );
            assertQueryRows(
                    "SELECT renamed[-1] FROM (SELECT a renamed FROM lp_array_access) WHERE renamed[1]>0",
                    """
                            []
                            30.0
                            """
            );
            assertQueryRows(
                    "SELECT ARRAY[10.0,20.0][2],ARRAY[10.0,20.0][NULL::LONG],ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]][2][1]",
                    """
                            []	[]1	[]2
                            20.0	null	3.0
                            """
            );
        });
    }

    @Test
    public void testSlicesKeepDimensionsAndDriveDependentUnnest() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,a[1:3],a[1:2],a[2:],a[-2:],a[-3:-1],m[1:3,1],m[2,1:3],m[1:3,1:3] FROM lp_array_access ORDER BY id",
                    """
                            id	[]	[]1	[]2	[]3	[]4	[]5	[]6	[]7
                            1	[10.0,20.0]	[10.0]	[20.0,30.0]	[20.0,30.0]	[10.0,20.0]	[1.0,3.0]	[3.0,4.0]	[[1.0,2.0],[3.0,4.0]]
                            2	[-1.0,5.0]	[-1.0]	[5.0]	[-1.0,5.0]	[-1.0]	[5.0,7.0]	[7.0,8.0]	[[5.0,6.0],[7.0,8.0]]
                            3	null	null	null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,a[NULL::INT:3],a[1:NULL::INT] FROM lp_array_access ORDER BY id",
                    """
                            id	[]	[]1
                            1	null	null
                            2	null	null
                            3	null	null
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_array_access t,UNNEST(t.a[2:]) u ORDER BY t.id,u.value",
                    """
                            id	value
                            1	20.0
                            1	30.0
                            2	5.0
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_array_access t,UNNEST(t.m[2]) u ORDER BY t.id,u.value",
                    """
                            id	value
                            1	3.0
                            1	4.0
                            2	7.0
                            2	8.0
                            """
            );
            assertQueryRows("SELECT * FROM UNNEST(ARRAY[1.0,2.0,3.0][2:]) u", """
                    value
                    2.0
                    3.0
                    """);
        });
    }

    @Test
    public void testRuntimeIndexesAndSliceBoundsRebindAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            bindVariableService.setInt(1, 3);
            final String sql = "SELECT id,a[$1],a[$1:$2],m[2][$1] FROM lp_array_access ORDER BY id";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_array_access", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                }
                for (int index : new int[]{1, 2, -1, Numbers.INT_NULL}) {
                    bindVariableService.setInt(0, index);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                         RecordCursorFactory baseline = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(retained, print(baseline));
                    }
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testCompileDiagnosticsAndDiscardedNativeChildrenRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT a[:2] FROM lp_array_access").noLeakCheck().fails(9, "undefined bind variable: :2");
            assertQuery("SELECT a[1,2] FROM lp_array_access").noLeakCheck().fails(9, "too many array access arguments [nDims=1, nArgs=2]");
            assertQuery("SELECT a[0:2] FROM lp_array_access").noLeakCheck().fails(10, "array slice bounds must be non-zero [dim=1, lowerBound=0, upperBound=2]");
            assertQuery("SELECT a[2147483648L] FROM lp_array_access").noLeakCheck().fails(9, "int overflow on array index [dim=1, index=2147483648]");
            assertQuery("SELECT ARRAY[1.0,2.0][1,2]").noLeakCheck().fails(22, "too many array access arguments [nDims=1, nArgs=2]");
            assertQueryRows("SELECT ARRAY[1.0,2.0][NULL::LONG]", """
                    []
                    null
                    """);
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_array_access(unused INT,id INT,a DOUBLE[],m DOUBLE[][])");
        execute("INSERT INTO lp_array_access VALUES(91,1,ARRAY[10.0,20.0,30.0],ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]]),"
                + "(92,2,ARRAY[-1.0,5.0],ARRAY[ARRAY[5.0,6.0],ARRAY[7.0,8.0]]),(93,3,NULL,NULL)");
    }

    private String print(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
            return sink.toString();
        }
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
