package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class Issue6583Test extends AbstractCairoTest {

    @Test
    public void testCTASTrailingVarcharCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t_ctas_1 as (" +
                    "select cast('2024-01-01' as timestamp) as x from long_sequence(1) " +
                    "union all " +
                    "select cast('2024-01-02' as varchar) as x from long_sequence(1))");

            assertSql("column\ttype\n" +
                      "x\tVARCHAR\n", 
                      "select \"column\", type from table_columns('t_ctas_1')");
        });
    }

    @Test
    public void testVarcharAndStringUnionInCTAS() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_ctas_2 AS (" +
                "SELECT CAST('legacy_string' AS STRING) as payload FROM long_sequence(1) " +
                "UNION ALL " +
                "SELECT CAST('new_varchar' AS VARCHAR) as payload FROM long_sequence(1)" +
                "), CAST(payload AS VARCHAR)"); 

            assertSql("count\n2\n", "select count() from t_ctas_2");
            assertSql("column\ttype\n" +
                      "payload\tVARCHAR\n", 
                      "select \"column\", type from table_columns('t_ctas_2')");
        });
    }

    @Test
    public void testLongVarcharCasting() throws Exception {
        assertMemoryLeak(() -> {
            String longStr = "this_is_a_very_long_string_that_exceeds_inlining_limits";
            execute("CREATE TABLE t_ctas_3 AS (" +
                "SELECT CAST('" + longStr + "' AS VARCHAR) as x " +
                "FROM long_sequence(1)" +
                "), CAST(x AS VARCHAR)");

            assertSql("x\n" + longStr + "\n", "select x from t_ctas_3");
        });
    }
}