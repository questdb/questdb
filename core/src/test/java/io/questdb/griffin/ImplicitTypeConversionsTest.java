package io.questdb.griffin;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

/**
 *
 */
public class ImplicitTypeConversionsTest extends AbstractGriffinTest {

    @Test(expected = SqlException.class)
    public void testInsertDoubleAsFloat_CausesUnderflow_And_ReturnsException() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(f float);", sqlExecutionContext);
            executeInsert("insert into tab values (-34028235000000000000000000000000000000.0);");

            String expected = "f\n" +
                    "-3.4028235E38\n";

            assertReader(expected, "tab");
        });
    }

    @Test(expected = SqlException.class)
    public void testInsertDoubleAsFloat_CausesOverflow_And_ReturnsException() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(f float);", sqlExecutionContext);
            executeInsert("insert into tab values (34028235700000000000000000000000000000.0);");

            String expected = "f\n" +
                    "3.4028235E38\n";

            assertReader(expected, "tab");
        });
    }
    
    @Test
    public void testInsertDoubleAsFloat_ReturnsExactValue() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(f float);", sqlExecutionContext);
            executeInsert("insert into tab values (123.4567);");

            String expected = "f\n" +
                    "123.4567\n";

            assertReader(expected, "tab");
        });
    }

    @Test
    public void testInsertDoubleAsFloat_ReturnsApproximateValue() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(f float);", sqlExecutionContext);
            executeInsert("insert into tab values (123.45678);");

            String expected = "f\n" +
                    "123.4568\n";

            assertReader(expected, "tab");
        });
    }
}
