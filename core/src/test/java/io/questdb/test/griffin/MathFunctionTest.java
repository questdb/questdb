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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MathFunctionTest extends AbstractCairoTest {
    @Test
    public void testConstantsParametersAndNumericPromotion() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT pi(),degrees(0.5),radians(90),sin(0),cos(0),atan2(1,-1),power(2,3),round(1.5) FROM lp_math LIMIT 1",
                    """
                            pi	degrees	radians	sin	cos	atan2	power	round
                            3.141592653589793	28.64788975654116	1.5707963267948966	0.0	1.0	2.356194490192345	8.0	2.0
                            """
            );
            bindVariableService.setDouble(0, 0.5);
            bindVariableService.setInt(1, 2);
            assertQueryRows(
                    "SELECT id,sin($1),power(d,$2),atan2($1,d),degrees($1),radians($1) FROM lp_math ORDER BY id",
                    """
                            id	sin	power	atan2	degrees	radians
                            1	0.479425538604203	0.25	2.356194490192345	28.64788975654116	0.008726646259971648
                            2	0.479425538604203	0.0	1.5707963267948966	28.64788975654116	0.008726646259971648
                            3	0.479425538604203	0.0625	1.1071487177940904	28.64788975654116	0.008726646259971648
                            4	0.479425538604203	6.25	0.19739555984988078	28.64788975654116	0.008726646259971648
                            5	0.479425538604203	1000000.0	4.999999583333395E-4	28.64788975654116	0.008726646259971648
                            6	0.479425538604203	null	null	28.64788975654116	0.008726646259971648
                            """
            );
            assertQueryRows(
                    "SELECT id,sqrt(i),sin(l),cos(f),power(i,l),atan2(f,i) FROM lp_math ORDER BY id",
                    """
                            id	sqrt	sin	cos	power	atan2
                            1	null	-0.1411200080598672	0.8775825618903728	-0.037037037037037035	-2.976443976175166
                            2	0.0	0.0	1.0	1.0	-0.0
                            3	1.4142135623730951	0.9092974268256817	0.9689124217106447	4.0	0.12435499454676144
                            4	1.7320508075688772	0.1411200080598672	-0.8011436155469337	27.0	0.6947382761967033
                            5	1.0	0.8414709848078965	0.562379076290703	1.0	1.5697963271282298
                            6	null	null	null	null	null
                            """
            );
            bindVariableService.setDouble(0, Double.NaN);
            assertQueryRows(
                    "SELECT id,degrees($1),radians($1),sqrt($1),round($1) FROM lp_math ORDER BY id",
                    """
                            id	degrees	radians	sqrt	round
                            1	null	null	null	null
                            2	null	null	null	null
                            3	null	null	null	null
                            4	null	null	null	null
                            5	null	null	null	null
                            6	null	null	null	null
                            """
            );
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,sqrt(CASE WHEN i IN (-3,0,2) THEN abs(d) ELSE 0 END) value FROM lp_math ORDER BY id";
            final String expected = """
                    id	value
                    1	0.7071067811865476
                    2	0.0
                    3	0.5
                    4	0.0
                    5	0.0
                    6	0.0
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory factoryBeforeReset = retained) {
                    assertUnknownFunction(compiler, "SELECT sqrt(d)+lp_missing_fn(i) FROM lp_math");
                    try (RecordCursorFactory factory = compiler.compile("SELECT sin(d) FROM lp_math", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(factory);
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

    @Test
    public void testMathInFiltersAggregatesAndNestedProjections() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id FROM lp_math WHERE sin(d)>0 OR sqrt(abs(d))<1 ORDER BY id",
                    """
                            id
                            1
                            2
                            3
                            4
                            5
                            """
            );
            assertQueryRows(
                    "SELECT sum(sqrt(abs(d))),avg(sin(d)),count() FROM lp_math WHERE cos(d)<1",
                    """
                            sum	avg	count
                            34.41102221295453	0.2983325263215697	4
                            """
            );
            assertQueryRows(
                    "SELECT value FROM (SELECT id,sqrt(abs(d))+sin(d) value FROM lp_math) WHERE value>0 ORDER BY value,id LIMIT 3",
                    """
                            value
                            0.22768124258234457
                            0.747403959254523
                            2.1796109741881464
                            """
            );
            assertQueryRows(
                    "SELECT sign(i) k,sum(round(d)) total FROM lp_math GROUP BY 1 ORDER BY k",
                    """
                            k	total
                            null	null
                            -1	0.0
                            0	0.0
                            1	1003.0
                            """
            );
        });
    }

    @Test
    public void testTrigonometricLogarithmicAndPowerDomains() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT id,acos(d),asin(d),atan(d),atan2(d,f),cos(d),cot(d),degrees(d),exp(d),
                                   ln(d),log(d),pi(),power(d,2.0),radians(d),sin(d),sqrt(d),tan(d)
                            FROM lp_math ORDER BY id
                            """,
                    """
                            id	acos	asin	atan	atan2	cos	cot	degrees	exp	ln	log	pi	power	radians	sin	sqrt	tan
                            1	2.0943951023931957	-0.5235987755982989	-0.4636476090008061	-2.356194490192345	0.8775825618903728	-1.830487721712452	-28.64788975654116	0.6065306597126334	null	null	3.141592653589793	0.25	-0.008726646259971648	-0.479425538604203	null	-0.5463024898437905
                            2	1.5707963267948966	-0.0	-0.0	-3.141592653589793	1.0	null	-0.0	1.0	null	null	3.141592653589793	0.0	-0.0	-0.0	-0.0	-0.0
                            3	1.318116071652818	0.25268025514207865	0.24497866312686414	0.7853981633974483	0.9689124217106447	3.91631736464594	14.32394487827058	1.2840254166877414	-1.3862943611198906	-0.6020599913279624	3.141592653589793	0.0625	0.004363323129985824	0.24740395925452294	0.5	0.25534192122103627
                            4	null	null	1.1902899496825317	0.7853981633974483	-0.8011436155469337	-1.3386481283041514	143.2394487827058	12.182493960703473	0.9162907318741551	0.3979400086720376	3.141592653589793	6.25	0.04363323129985824	0.5984721441039564	1.5811388300841898	-0.7470222972386603
                            5	null	null	1.5697963271282298	0.7853981633974483	0.562379076290703	0.6801221323348698	57295.77951308232	null	6.907755278982137	3.0	3.141592653589793	1000000.0	17.453292519943297	0.8268795405320025	31.622776601683793	1.4703241557027185
                            6	null	null	null	null	null	null	null	null	null	null	3.141592653589793	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,ln(-abs(d)),sqrt(-abs(d)),acos(d+2),cot(d-d),power(d,0.5),round(exp(d)) FROM lp_math ORDER BY id",
                    """
                            id	ln	sqrt	acos	cot	power	round
                            1	null	null	null	null	null	1.0
                            2	null	-0.0	null	null	0.0	1.0
                            3	null	null	null	null	0.5	1.0
                            4	null	null	null	null	1.5811388300841898	12.0
                            5	null	null	null	null	31.622776601683793	null
                            6	null	null	null	null	null	null
                            """
            );
        });
    }

    @Test
    public void testTypedRoundingSignAndRemainder() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT id,ceil(f) cf,ceil(d) cd,ceiling(f) cef,ceiling(d) ced,floor(f) ff,floor(d) fd,
                                   sign(b) sb,sign(s) ss,sign(i) si,sign(l) sl,sign(f) sf,sign(d) sd,
                                   i%3 ri,l%3 rl,f%3 rf,d%3 rd,round(d) rounded
                            FROM lp_math ORDER BY id
                            """,
                    """
                            id	cf	cd	cef	ced	ff	fd	sb	ss	si	sl	sf	sd	ri	rl	rf	rd	rounded
                            1	-0.0	-0.0	-0.0	-0.0	-1.0	-1.0	-1	-1	-1	-1	-1.0	-1.0	0	0	-0.5	-0.5	0.0
                            2	-0.0	-0.0	-0.0	-0.0	-0.0	-0.0	0	0	0	0	-0.0	0.0	0	0	-0.0	-0.0	0.0
                            3	1.0	1.0	1.0	1.0	0.0	0.0	1	1	1	1	1.0	1.0	2	2	0.25	0.25	0.0
                            4	3.0	3.0	3.0	3.0	2.0	2.0	1	1	1	1	1.0	1.0	0	0	2.5	2.5	3.0
                            5	1000.0	1000.0	1000.0	1000.0	1000.0	1000.0	1	1	1	1	1.0	1.0	1	1	1.0	1.0	1000.0
                            6	null	null	null	null	null	null	0	0	null	null	null	null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,i%0,l%0,f%0,d%0,i%null,l%null,f%null,d%null FROM lp_math ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7
                            1	null	null	null	null	null	null	null	null
                            2	null	null	null	null	null	null	null	null
                            3	null	null	null	null	null	null	null	null
                            4	null	null	null	null	null	null	null	null
                            5	null	null	null	null	null	null	null	null
                            6	null	null	null	null	null	null	null	null
                            """
            );
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void assertUnknownFunction(SqlCompilerImpl compiler, String sql) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_math(unused INT,id INT,b BYTE,s SHORT,i INT,l LONG,f FLOAT,d DOUBLE)");
        execute("""
                INSERT INTO lp_math VALUES
                (91,1,-3,-3,-3,-3,-0.5,-0.5),
                (92,2,0,0,0,0,-0.0,-0.0),
                (93,3,2,2,2,2,0.25,0.25),
                (94,4,3,3,3,3,2.5,2.5),
                (95,5,1,1,1,1,1000,1000),
                (96,6,0,0,null,null,null,null)
                """);
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
