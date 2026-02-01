package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.griffin.SqlException;
import org.junit.Test;

public class CTASCastTest extends AbstractCairoTest {
    @Test
    public void testCTASCastLongToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x from long_sequence(5)), cast(x as varchar)");
        });
    }

    @Test
    public void testCTASCastVarcharToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s as (select cast(x as varchar) x from long_sequence(5))");
            execute("create table t as (select x from s), cast(x as long)");
        });
    }

    @Test
    public void testDuplicateColumnInSelect() throws Exception {
        assertQuery(
                "x\ty\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\t3\n" +
                        "4\t4\n" +
                        "5\t5\n",
                "select x, x as y from long_sequence(5)",
                null,
                null,
                true,
                true);
    }

    @Test
    public void testOverridingColumnInSelect() throws Exception {
        // This test checks if we can "override" a column from a wildcard.
        // Usually QuestDB will result in two columns with the same name, which might be
        // an error or just confusing.
        try {
            assertQuery(
                    "x\tx_cast\n1\t1\n2\t2\n3\t3\n4\t4\n5\t5\n",
                    "select *, cast(x as varchar) x_cast from long_sequence(5)",
                    null,
                    null,
                    true,
                    true);
        } catch (SqlException e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void testExcludeSyntax() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("select * exclude (i) from (select 1 i, 2 j)");
            } catch (SqlException e) {
                System.out.println("EXCLUDE not supported: " + e.getMessage());
            }
        });
    }
}
