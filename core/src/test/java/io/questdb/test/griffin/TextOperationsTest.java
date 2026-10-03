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

public class TextOperationsTest extends AbstractCairoTest {
    @Test
    public void testStringAndVarcharOperationsKeepTypesNullsAndUnicode() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT id,length(v),length_bytes(v),lower(v),upper(v),to_lowercase(v),to_uppercase(v),
                                   trim(v),ltrim(v),rtrim(v),left(s,n),right(s,n),left(v,n),right(v,n),
                                   substring(s,2,3),substring(v,2,3),replace(s,'a','XY'),replace(v,'a','XY'),
                                   strpos(s,'a'),position(s,'a'),strpos(v,'a'),position(v,'a'),
                                   concat(s,v,id,n),s::varchar,v::string
                            FROM lp_string ORDER BY id
                            """,
                    """
                            id	length	length_bytes	lower	upper	to_lowercase	to_uppercase	trim	ltrim	rtrim	left	right	left1	right1	substring	substring1	replace	replace1	strpos	position	strpos1	position1	concat	cast	cast1
                            1	8	8	  abca  	  ABCA  	  abca  	  ABCA  	Abca	Abca  	  Abca	  A	a  	  A	a  	 Ab	 Ab	  AbcXY  	  AbcXY  	6	6	6	6	  Abca    Abca  13	  Abca  	  Abca \s
                            2	4	10	aé中🙂	AÉ中🙂	aé中🙂	AÉ中🙂	aé中🙂	aé中🙂	aé中🙂	aé	🙂	aé	中🙂	é中\uD83D	é中\uD83D	XYé中🙂	XYé中🙂	1	1	1	1	aé中🙂aé中🙂22	aé中🙂	aé中🙂
                            3	3	3	abc	ABC	abc	ABC	abc	abc	abc	ab	bc	ab	bc	bc	bc	XYbc	XYbc	1	1	1	1	abcabc3-1	abc	abc
                            4	0	0																0	0	0	0	40	\t
                            5	-1	-1																null	null	null	null	5null	\t
                            """
            );
            assertQueryRows(
                    "SELECT id,left(v,-2),right(v,-2),substring(v,0,2),substring(s,1,0),replace(s,'','x'),replace(v,'','x') FROM lp_string ORDER BY id",
                    """
                            id	left	right	substring	substring1	replace	replace1
                            1	  Abca	Abca  	 		  Abca  	  Abca \s
                            2	aé	中🙂	a		aé中🙂	aé中🙂
                            3	a	c	a		abc	abc
                            4					\t
                            5					\t
                            """
            );
            assertQueryRows(
                    "SELECT id,replace(s,s,s),replace(v,v,v),strpos(s,s),strpos(v,v) FROM lp_string ORDER BY id",
                    """
                            id	replace	replace1	strpos	strpos1
                            1	  Abca  	  Abca  	1	1
                            2	aé中🙂	aé中🙂	1	1
                            3	abc	abc	1	1
                            4			1	1
                            5			null	null
                            """
            );
        });
    }

    @Test
    public void testStringConstantsAndRuntimeParameters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT left('aé中'::varchar,2),right('aé中'::varchar,2),substring('aé中'::varchar,2,2),trim('  a  '::varchar),replace('aé中'::varchar,'é','x'),concat('a',null,3),null::varchar FROM lp_string LIMIT 1",
                    """
                            left	right	substring	trim	replace	concat	cast
                            aé	é中	é中	a	ax中	a3\t
                            """
            );
            assertQueryRows(
                    "SELECT left('x''y'::varchar,3),right('x''y'::varchar,3),replace('x''y'::varchar,'x','z'),('x''y'::varchar)::string FROM lp_string LIMIT 1",
                    """
                            left	right	replace	cast
                            x'y	x'y	z'y	x'y
                            """
            );
            bindVariableService.setVarchar(0, new Utf8String("  hé中  "));
            bindVariableService.setInt(1, 2);
            assertQueryRows(
                    "SELECT id,left($1,$2),right(v,$2),replace(v,$1,v),position(v,$1),trim($1),concat(v,$1) FROM lp_string ORDER BY id",
                    """
                            id	left	right	replace	position	trim	concat
                            1	  	  	  Abca  	0	hé中	  Abca    hé中 \s
                            2	  	中🙂	aé中🙂	0	hé中	aé中🙂  hé中 \s
                            3	  	bc	abc	0	hé中	abc  hé中 \s
                            4	  			0	hé中	  hé中 \s
                            5	  			null	hé中	  hé中 \s
                            """
            );
            bindVariableService.setVarchar(0, null);
            assertQueryRows(
                    "SELECT id,left($1,$2),length($1),replace(v,$1,v),position(v,$1),concat(v,$1) FROM lp_string ORDER BY id",
                    """
                            id	left	length	replace	position	concat
                            1		-1		null	  Abca \s
                            2		-1		null	aé中🙂
                            3		-1		null	abc
                            4		-1		null\t
                            5		-1		null\t
                            """
            );
        });
    }

    @Test
    public void testNativeDiscardedChildrenAndFailureRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT left(trim(v),null),right(trim(v),null),substring(trim(v),1,0),
                                   substring(trim(v),null,2),replace(trim(v),'x',null),replace(trim(v),null,trim(v)),
                                   replace(v,'',trim(v)),replace(null::varchar,v,trim(v)),strpos(trim(v),null),position(trim(v),null),
                                   left(CASE WHEN id IN (1,2) THEN s ELSE s END,null),
                                   substring(s,CASE WHEN id IN (1,2) THEN 1 ELSE 2 END,0),
                                   replace(s,'',CASE WHEN id IN (1,2) THEN s ELSE s END)
                            FROM lp_string
                            """,
                    """
                            left	right	substring	substring1	replace	replace1	replace2	replace3	strpos	position	left1	substring2	replace4
                            						  Abca  		null	null			  Abca \s
                            						aé中🙂		null	null			aé中🙂
                            						abc		null	null			abc
                            								null	null		\t
                            								null	null		\t
                            """
            );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile("SELECT substring(trim(v),1,-1) FROM lp_string", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "negative substring length");
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT length(trim(v)) FROM lp_string ORDER BY id", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(factory);
                }
            }
        });
    }

    @Test
    public void testFiltersDerivedProjectionsAndSameTypeCase() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id FROM lp_string WHERE length(trim(v))>0 AND strpos(s,'a')>0 ORDER BY id",
                    """
                            id
                            1
                            2
                            3
                            """
            );
            assertQueryRows(
                    "SELECT result FROM (SELECT id,left(trim(v),3) result FROM lp_string) WHERE length(result)>0 ORDER BY id",
                    """
                            result
                            Abc
                            aé中
                            abc
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN id IN (1,3) THEN trim(v) ELSE left(v,2) END FROM lp_string ORDER BY id",
                    """
                            id	case
                            1	Abca
                            2	aé
                            3	abc
                            4\t
                            5\t
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE v WHEN 'abc' THEN trim(v) ELSE left(v,2) END FROM lp_string ORDER BY id",
                    """
                            id	switch
                            1	 \s
                            2	aé
                            3	abc
                            4\t
                            5\t
                            """
            );
            assertQueryRows(
                    "SELECT sum(length(trim(v))),max(position(s,'a')) FROM lp_string",
                    """
                            sum	max
                            10	6
                            """
            );
        });
    }

    @Test
    public void testRetainedFactorySurvivesReuseAndCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,replace(trim(v),'a','xy'),concat(left(v,2),right(s,2)) FROM lp_string ORDER BY id";
            final String expected = """
                    id	replace	concat
                    1	Abcxy	   \s
                    2	xyé中🙂	aé🙂
                    3	xybc	abbc
                    4	\t
                    5	\t
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory beforeReset = retained) {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT concat(trim(v),lp_missing_fn(id)) FROM lp_string", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
                    }
                    try (RecordCursorFactory next = compiler.compile("SELECT length(v) FROM lp_string", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(next);
                    }
                    assertResult(retained, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, expected);
            }
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_string(unused INT,id INT,s STRING,v VARCHAR,n INT)");
        execute("""
                INSERT INTO lp_string VALUES
                (91,1,'  Abca  ','  Abca  ',3),
                (92,2,'aé中🙂','aé中🙂',2),
                (93,3,'abc','abc',-1),
                (94,4,'','',0),
                (95,5,null,null,null)
                """);
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
