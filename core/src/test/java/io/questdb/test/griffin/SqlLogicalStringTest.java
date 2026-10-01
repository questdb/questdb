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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalStringTest extends AbstractCairoTest {
    @Test
    public void testStringTransformsProjectionPredicatesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,lower(label) lo,upper(label) hi,to_lowercase(label) lo_alias,to_uppercase(label) hi_alias FROM lp_string ORDER BY id",
                    """
                            id	lo	hi	lo_alias	hi_alias
                            1	  abc  	  ABC  	  abc  	  ABC \s
                            2	abc	ABC	abc	ABC
                            3			\t
                            4			\t
                            5	  äö  	  ÄÖ  	  äö  	  ÄÖ \s
                            """
            );
            assertQueryRows(
                    "SELECT id,trim(label) t,ltrim(label) lt,rtrim(label) rt FROM lp_string ORDER BY id",
                    """
                            id	t	lt	rt
                            1	AbC	AbC  	  AbC
                            2	abc	abc	abc
                            3		\t
                            4		\t
                            5	Äö	Äö  	  Äö
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_string WHERE lower(trim(label))='abc' ORDER BY id",
                    """
                            id
                            1
                            2
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_string WHERE upper(label)=null ORDER BY id",
                    """
                            id
                            3
                            """
            );
            assertQueryRows(
                    "SELECT id,lower('  ''AbC''  ') lo,trim('  ''AbC''  ') t,ltrim('  ''AbC''  ') lt,trim(CAST(null AS STRING)) n FROM lp_string ORDER BY id",
                    """
                            id	lo	t	lt	n
                            1	  'abc'  	'AbC'	'AbC'  \t
                            2	  'abc'  	'AbC'	'AbC'  \t
                            3	  'abc'  	'AbC'	'AbC'  \t
                            4	  'abc'  	'AbC'	'AbC'  \t
                            5	  'abc'  	'AbC'	'AbC'  \t
                            """
            );
        });
    }

    @Test
    public void testStringTransformGroupingOrderingAndDerivedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT lower(trim(label)) value,count() FROM lp_string ORDER BY value",
                    """
                            value	count
                            	1
                            	1
                            abc	2
                            äö	1
                            """
            );
            assertQueryRows(
                    "SELECT id,upper(label) value FROM lp_string ORDER BY value,id",
                    """
                            id	value
                            3\t
                            4\t
                            1	  ABC \s
                            5	  ÄÖ \s
                            2	ABC
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT id,lower(trim(label)) value FROM lp_string) q WHERE value='abc' ORDER BY id",
                    """
                            id
                            1
                            2
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN id>2 THEN trim(label) ELSE upper(label) END value FROM lp_string ORDER BY id",
                    """
                            id	value
                            1	  ABC \s
                            2	ABC
                            3\t
                            4\t
                            5	Äö
                            """
            );
        });
    }

    @Test
    public void testStringRuntimeConstantsRetainSelectedTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setStr(0, "  ABC  ");
            assertQueryRows(
                    "SELECT id,lower(trim($1)) value FROM lp_string WHERE lower(trim(label))=lower(trim($1)) ORDER BY id",
                    """
                            id	value
                            1	abc
                            2	abc
                            """
            );
            bindVariableService.setStr(0, null);
            assertQueryRows(
                    "SELECT id,upper($1) value FROM lp_string WHERE trim(label)=trim($1) ORDER BY id",
                    """
                            id	value
                            3\t
                            """
            );
        });
    }

    @Test
    public void testStringClosuresSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            final String expected = """
                    id	value
                    1	ABC
                    2	ABC
                    """;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    final String sql = "SELECT id,upper(trim(label)) value FROM lp_string WHERE lower(trim(label))='abc' ORDER BY id";
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, expected);
                    }
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory next = compiler.compile("SELECT count() FROM lp_string", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(next);
                    }
                    compiler.clear();
                }
                assertResult(retained, expected);
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_string(id INT,unused LONG,label STRING)");
        execute("INSERT INTO lp_string VALUES (1,1,'  AbC  '),(2,2,'abc'),(3,3,null),(4,4,''),(5,5,'  Äö  ')");
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
