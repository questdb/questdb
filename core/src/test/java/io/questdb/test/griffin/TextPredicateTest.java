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
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TextPredicateTest extends AbstractCairoTest {
    @Test
    public void testVarcharAndMixedComparisonsKeepAliasesNullsAndUnicode() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,v=s,v!=s,s<>v,v<s,v<=s,v>s,v>=s,s<v,s<=v,s>v,s>=v,v=v,v<v FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7	column8	column9	column10	column11	column12
                            1	true	false	false	false	true	false	true	false	true	false	true	true	false
                            2	false	true	true	false	false	true	true	true	true	false	false	true	false
                            3	true	false	false	false	true	false	true	false	true	false	true	true	false
                            4	true	false	false	false	true	false	true	false	true	false	true	true	false
                            5	true	false	false	false	true	false	true	false	true	false	true	true	false
                            6	true	false	false	false	true	false	true	false	true	false	true	true	false
                            7	true	false	false	false	true	false	true	false	true	false	true	true	false
                            8	true	false	false	false	true	false	true	false	true	false	true	true	false
                            """
            );
            assertRowsOnly(
                    "SELECT id,v='abc','abc'=v,v<='abc','abc'>v,v=null,null=v,v<null,v<=null,null>=v FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7	column8
                            1	true	true	true	false	false	false	false	false	false
                            2	true	true	true	false	false	false	false	false	false
                            3	false	false	false	false	false	false	false	false	false
                            4	false	false	true	true	false	false	false	false	false
                            5	false	false	true	true	false	false	false	false	false
                            6	false	false	false	false	true	true	false	true	true
                            7	false	false	true	true	false	false	false	false	false
                            8	false	false	false	false	false	false	false	false	false
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM lp_text_pred WHERE v>='ab' AND v<'b' ORDER BY id",
                    """
                            id
                            1
                            2
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM lp_text_pred WHERE trim(v)=trim(s) OR v=null ORDER BY id",
                    """
                            id
                            1
                            3
                            4
                            5
                            6
                            7
                            8
                            """
            );
            bindVariableService.setVarchar(0, new Utf8String("abc"));
            assertRowsOnly(
                    "SELECT id,v=$1,v!=$1,$1=v,v<$1,$1<=v FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4
                            1	true	false	true	false	true
                            2	true	false	true	false	true
                            3	false	true	false	false	true
                            4	false	true	false	true	false
                            5	false	true	false	true	false
                            6	false	true	false	false	false
                            7	false	true	false	true	false
                            8	false	true	false	false	true
                            """
            );
            bindVariableService.setVarchar(0, null);
            assertRowsOnly("SELECT id FROM lp_text_pred WHERE v=$1 ORDER BY id", """
                    id
                    6
                    """);
        });
    }

    @Test
    public void testLikeSpecializationsRegexAndUnicodeFallback() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    """
                            SELECT id,s LIKE 'ab%',v LIKE 'ab%',s LIKE '%bc',v LIKE '%bc',s LIKE '%b%',v LIKE '%b%',
                                   s ILIKE 'AB%',v ILIKE 'AB%',s ILIKE '%BC',v ILIKE '%BC',s ILIKE '%B%',v ILIKE '%B%',
                                   s LIKE 'a_c',v LIKE 'a_c',s LIKE '%',v LIKE '%%',s LIKE '',v LIKE null,
                                   s NOT LIKE 'ab%',v NOT ILIKE 'ab%'
                            FROM lp_text_pred ORDER BY id
                            """,
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7	column8	column9	column10	column11	column12	column13	column14	column15	column16	column17	column18	column19
                            1	true	true	true	true	true	true	true	true	true	true	true	true	true	true	true	true	false	false	false	false
                            2	false	true	true	true	true	true	true	true	true	true	true	true	false	true	true	true	false	false	true	false
                            3	false	false	false	false	false	false	false	false	false	false	false	false	false	false	true	true	false	false	true	true
                            4	false	false	false	false	true	true	false	false	false	false	true	true	false	false	true	true	false	false	true	true
                            5	false	false	false	false	false	false	false	false	false	false	false	false	false	false	true	true	false	false	true	true
                            6	false	false	false	false	false	false	false	false	false	false	false	false	false	false	false	false	false	false	true	true
                            7	false	false	false	false	true	true	false	false	false	false	true	true	false	false	true	true	false	false	true	true
                            8	false	false	false	false	false	false	false	false	false	false	false	false	false	false	true	true	false	false	true	true
                            """
            );
            assertRowsOnly(
                    "SELECT id,s ILIKE 'Ä%',v ILIKE 'Ä%',s ILIKE '%中',v ILIKE '%中',v ILIKE '%é%',v LIKE '%long-pattern%' FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5
                            1	false	false	false	false	false	false
                            2	false	false	false	false	false	false
                            3	true	true	true	true	true	false
                            4	false	false	false	false	false	false
                            5	false	false	false	false	false	false
                            6	false	false	false	false	false	false
                            7	false	false	false	false	false	false
                            8	false	false	false	false	false	true
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM lp_text_pred WHERE trim(v) LIKE 'ab%' AND length(v)>1 ORDER BY id",
                    """
                            id
                            1
                            2
                            7
                            """
            );
            bindVariableService.setStr(0, "a_c");
            assertRowsOnly(
                    "SELECT id,s LIKE $1,v ILIKE $1 FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1
                            1	true	true
                            2	false	true
                            3	false	false
                            4	false	false
                            5	false	false
                            6	false	false
                            7	false	false
                            8	false	false
                            """
            );
            bindVariableService.setStr(0, null);
            assertRowsOnly(
                    "SELECT id,s LIKE $1,v ILIKE $1 FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1
                            1	false	false
                            2	false	false
                            3	false	false
                            4	false	false
                            5	false	false
                            6	false	false
                            7	false	false
                            8	false	false
                            """
            );
        });
    }

    @Test
    public void testMembershipPrefixAndNullIfWithTypedParameters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,s IN ('abc','äé中',null),v IN ('abc','äé中',null),s NOT IN ('abc',null),v NOT IN ('abc',null),starts_with(s,'ab'),starts_with(v,'ab'),nullif(s,'abc'),nullif(v,'abc') FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2	column3	starts_with	starts_with1	nullif	nullif1
                            1	true	true	false	false	true	true	\t
                            2	false	true	true	false	false	true	Abc\t
                            3	true	true	true	true	false	false	äé中	äé中
                            4	false	false	true	true	false	false	a%b	a%b
                            5	false	false	true	true	false	false	\t
                            6	true	true	false	false	false	false	\t
                            7	false	false	true	true	false	false	 abc 	 abc\s
                            8	false	false	true	true	false	false	xlong-patterny	xlong-patterny
                            """
            );
            assertRowsOnly(
                    "SELECT id,starts_with(s,s),starts_with(v,v),starts_with(s,null),starts_with(v,null),nullif(s,s),nullif(v,v),nullif(s,null),nullif(v,null) FROM lp_text_pred ORDER BY id",
                    """
                            id	starts_with	starts_with1	starts_with2	starts_with3	nullif	nullif1	nullif2	nullif3
                            1	true	true	false	false			abc	abc
                            2	true	true	false	false			Abc	abc
                            3	true	true	false	false			äé中	äé中
                            4	true	true	false	false			a%b	a%b
                            5	true	true	false	false			\t
                            6	false	false	false	false			\t
                            7	true	true	false	false			 abc 	 abc\s
                            8	true	true	false	false			xlong-patterny	xlong-patterny
                            """
            );
            bindVariableService.setStr(0, "abc");
            bindVariableService.setVarchar(1, new Utf8String("äé中"));
            assertRowsOnly(
                    "SELECT id,s IN ($1,$2,null),v IN ($1,$2,null),starts_with(v,$2),nullif(v,$2) FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	starts_with	nullif
                            1	true	true	false	abc
                            2	false	true	false	abc
                            3	true	true	true\t
                            4	false	false	false	a%b
                            5	false	false	false\t
                            6	true	true	false\t
                            7	false	false	false	 abc\s
                            8	false	false	false	xlong-patterny
                            """
            );
            bindVariableService.setStr(0, null);
            bindVariableService.setVarchar(1, null);
            assertRowsOnly(
                    "SELECT id FROM lp_text_pred WHERE v IN ($1,$2,null) OR s IN ($1,$2,null) ORDER BY id",
                    """
                            id
                            6
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM (SELECT id,trim(v) text FROM lp_text_pred) WHERE text IN ('abc','äé中') ORDER BY id",
                    """
                            id
                            1
                            2
                            3
                            7
                            """
            );
        });
    }

    @Test
    public void testDiscardedNativeChildrenAndInvalidPatternRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,trim(v) LIKE '',trim(v) ILIKE null,(CASE WHEN id IN (1,2) THEN s ELSE s END) LIKE null FROM lp_text_pred ORDER BY id",
                    """
                            id	column	column1	column2
                            1	false	false	false
                            2	false	false	false
                            3	false	false	false
                            4	false	false	false
                            5	false	false	false
                            6	false	false	false
                            7	false	false	false
                            8	false	false	false
                            """
            );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile("SELECT trim(v) LIKE 'abc\\' FROM lp_text_pred", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "LIKE pattern must not end with escape character");
                }
                try (RecordCursorFactory ignored = compiler.compile("SELECT trim(v) LIKE s FROM lp_text_pred", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "use constant or bind variable");
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT id FROM lp_text_pred WHERE trim(v) LIKE 'ab%' ORDER BY id", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(factory);
                }
            }
        });
    }

    @Test
    public void testPredicateFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,nullif(trim(v),'abc') result FROM lp_text_pred WHERE trim(v) LIKE '%b%' OR v IN ('äé中',null) ORDER BY id";
            final String expected = """
                    id	result
                    1\t
                    2\t
                    3	äé中
                    4	a%b
                    6\t
                    7\t
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory beforeReset = retained) {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT trim(v) LIKE s FROM lp_text_pred", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "use constant or bind variable");
                    }
                    compiler.clear();
                    assertRowsOnly(retained, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            }
            try (RecordCursorFactory factory = retained) {
                assertRowsOnly(factory, expected);
            }
        });
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_text_pred(unused INT,id INT,s STRING,v VARCHAR)");
        execute("""
                INSERT INTO lp_text_pred VALUES
                (91,1,'abc','abc'),(92,2,'Abc','abc'),(93,3,'äé中','äé中'),
                (94,4,'a%b','a%b'),(95,5,'',''),(96,6,null,null),
                (97,7,' abc ',' abc '),(98,8,'xlong-patterny','xlong-patterny')
                """);
    }
}
