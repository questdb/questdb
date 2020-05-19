package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.security.ReadOnlyCairoSecurityContext;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertStatement;

public class SecurityTest extends AbstractGriffinTest {
    protected static final SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(
            configuration,
            messageBus,
            1)
                    .with(
                            ReadOnlyCairoSecurityContext.INSTANCE,
                            bindVariableService,
                            null);

    @Test
    public void testCreateTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            try {
                assertQuery("count\n1\n", "select count() from balances", null);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("table does not exist"));
            }
        });
    }

    @Test
    public void testInsertDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("count\n0\n", "select count() from balances", null);

            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertStatement insertStatement = cq.getInsertStatement();
            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("count\n1\n", "select count() from balances", null);

            try {
                cq = compiler.compile("insert into balances values (2, 'ZAR', 140.6)", readOnlyExecutionContext);
                insertStatement = cq.getInsertStatement();
                try (InsertMethod method = insertStatement.createMethod(readOnlyExecutionContext)) {
                    method.execute();
                    method.commit();
                }
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQuery("count\n1\n", "select count() from balances", null);
        });
    }

    @Test
    public void testDropTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("drop table balances", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQuery("count\n0\n", "select count() from balances", null);
        });
    }

    @Test
    public void testAlterTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertStatement insertStatement = cq.getInsertStatement();
            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances", null, true);

            try {
                compiler.compile("alter table balances add column newcol int", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQuery("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances", null, true);
        });
    }

    @Test
    public void testRenameTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("rename table balances to newname", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQuery("count\n0\n", "select count() from balances", null);
        });
    }

    @Test
    public void testBackupTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("backup table balances", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
        });
    }

}
