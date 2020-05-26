package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertStatement;

public class SecurityTest extends AbstractGriffinTest {
    protected static final SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(
            configuration,
            messageBus,
            1)
                    .with(
                            new CairoSecurityContextImpl(false,
                                    2),
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

    @Test
    public void testMaxInMemoryRowsWithRandomAccessOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    "sym\td\nVTJW\t0.1985581797355932\nVTJW\t0.21583224269349388\n",
                    "select sym, d from tb1 where d < 0.3 ORDER BY d",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym\td\nVTJW\t0.1985581797355932\nVTJW\t0.21583224269349388\nPEHN\t0.3288176907679504\n",
                        "select sym, d from tb1 where d < 0.34 ORDER BY d",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithoutRandomAccessOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000001000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.3 ORDER BY d1",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\nPEHN\tRQQ\n",
                        "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.34 ORDER BY d1",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    "sym1\tsym2\nVTJW\tIBBT\nVTJW\tGPGW\n",
                    "select distinct sym1, sym2 from tb1 where d < 0.3",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym1\tsym2\nHYRX\tGPGW\nVTJW\tIBBT\nVTJW\tGPGW\n",
                        "select distinct sym1, sym2 from tb1 where d < 0.34",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select ts, sum(d) from tb1 SAMPLE BY 5d FILL(linear)",
                        "ts",
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(none)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(prev)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillValue() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(2.0)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(null)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select ts, d from tb1 LATEST BY d",
                        "ts",
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithImplicitGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(2,2,2,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    "sym2\tcount\nGZ\t509\nRX\t491\n",
                    "select sym2, count() from tb1",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym1\tcount\nPEHN\t265\nCPSW\t231\nHYRX\t262\nVTJW\t242\n",
                        "select sym1, count() from tb1",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

}
