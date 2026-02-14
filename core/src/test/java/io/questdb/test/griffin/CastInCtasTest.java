package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastInCtasTest extends AbstractCairoTest {
    @Test
    public void testCastVarcharInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x from long_sequence(5)), cast(x as varchar);");
        });
    }

    @Test
    public void testCastIntToLongInCtas() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select 1 x from long_sequence(5)), cast(x as long);");
            assertSql("x\n1\n1\n1\n1\n1\n", "select x from t");
        });
    }
}
