package io.questdb.cutlass.pgwire;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class PGConnectionContextTest extends AbstractPGContextTest {
    @Test
    public void testCursorFetch() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer ignored = createPGServer(1);
                    final Connection connection = getConnection(false, true)
            ) {
                final int totalRows = 12;
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("create table x (a int, b char, c text)");
                    for (int i = 0; i < totalRows; i++) {
                        statement.executeUpdate("insert into x values (" + i + ", 'b', 'cc')");
                    }
                }

                connection.setAutoCommit(false);

                try (Statement statement = connection.createStatement()) {
                    statement.setFetchSize(totalRows / 3);
                    int count = 0;
                    try (ResultSet rs = statement.executeQuery("x")) {
                        while (rs.next()) {
                            Assert.assertEquals(count, rs.getInt(1));
                            Assert.assertEquals("b", rs.getString(2));
                            Assert.assertEquals("cc", rs.getString(3));
                            count++;
                        }
                    }
                    Assert.assertEquals(totalRows, count);
                }
            }
        });
    }
}
