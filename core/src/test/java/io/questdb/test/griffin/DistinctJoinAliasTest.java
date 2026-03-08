/*******************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for DISTINCT queries with various JOIN types and table aliases.
 * These tests cover regression scenarios reported in issues #6058 and #6066
 * to ensure all permutations of joins work correctly with DISTINCT and table prefixes.
 */
public class DistinctJoinAliasTest extends AbstractCairoTest {

    @Test
    public void testAliasesMatchingColumnNames() throws Exception {
        testAliasesMatchingColumnNames(true);
    }

    @Test
    public void testAliasesMatchingColumnNames_columnAliasExprDisabled() throws Exception {
        testAliasesMatchingColumnNames(false);
    }

    @Test
    public void testComplexJoinChainWithTrickyAliases() throws Exception {
        testComplexJoinChainWithTrickyAliases(true);
    }

    @Test
    public void testComplexJoinChainWithTrickyAliases_columnAliasExprDisabled() throws Exception {
        testComplexJoinChainWithTrickyAliases(false);
    }

    @Test
    public void testConfusingMixedAliasScenarios() throws Exception {
        testConfusingMixedAliasScenarios(true);
    }

    @Test
    public void testConfusingMixedAliasScenarios_columnAliasExprDisabled() throws Exception {
        testConfusingMixedAliasScenarios(false);
    }

    @Test
    public void testCrossJoinLeftJoinCombination() throws Exception {
        testCrossJoinLeftJoinCombination(true);
    }

    @Test
    public void testCrossJoinLeftJoinCombination_columnAliasExprDisabled() throws Exception {
        testCrossJoinLeftJoinCombination(false);
    }

    @Test
    public void testCrossJoinWithDistinct() throws Exception {
        testCrossJoinWithDistinct(true);
    }

    @Test
    public void testCrossJoinWithDistinct_columnAliasExprDisabled() throws Exception {
        testCrossJoinWithDistinct(false);
    }

    @Test
    public void testFiveTableJoinWithDistinct() throws Exception {
        testFiveTableJoinWithDistinct(true);
    }

    @Test
    public void testFiveTableJoinWithDistinct_columnAliasExprDisabled() throws Exception {
        testFiveTableJoinWithDistinct(false);
    }

    @Test
    public void testInnerJoinWithAsAliases() throws Exception {
        testInnerJoinWithAsAliases(true);
    }

    @Test
    public void testInnerJoinWithAsAliases_columnAliasExprDisabled() throws Exception {
        testInnerJoinWithAsAliases(false);
    }

    @Test
    public void testInnerJoinWithColumnAliases() throws Exception {
        testInnerJoinWithColumnAliases(true);
    }

    @Test
    public void testInnerJoinWithColumnAliases_columnAliasExprDisabled() throws Exception {
        testInnerJoinWithColumnAliases(false);
    }

    @Test
    public void testInnerJoinWithShortAliases() throws Exception {
        testInnerJoinWithShortAliases(true);
    }

    @Test
    public void testInnerJoinWithShortAliases_columnAliasExprDisabled() throws Exception {
        testInnerJoinWithShortAliases(false);
    }

    @Test
    public void testInnerJoinWithTablePrefixedColumns() throws Exception {
        testInnerJoinWithTablePrefixedColumns(true);
    }

    @Test
    public void testInnerJoinWithTablePrefixedColumns_columnAliasExprDisabled() throws Exception {
        testInnerJoinWithTablePrefixedColumns(false);
    }

    @Test
    public void testLeftJoinWithTablePrefixedColumns() throws Exception {
        testLeftJoinWithTablePrefixedColumns(true);
    }

    @Test
    public void testLeftJoinWithTablePrefixedColumns_columnAliasExprDisabled() throws Exception {
        testLeftJoinWithTablePrefixedColumns(false);
    }

    @Test
    public void testMultiTableJoinWithAliases() throws Exception {
        testMultiTableJoinWithAliases(true);
    }

    @Test
    public void testMultiTableJoinWithAliases_columnAliasExprDisabled() throws Exception {
        testMultiTableJoinWithAliases(false);
    }

    @Test
    public void testNestedJoinsWithDistinct() throws Exception {
        testNestedJoinsWithDistinct(true);
    }

    @Test
    public void testNestedJoinsWithDistinct_columnAliasExprDisabled() throws Exception {
        testNestedJoinsWithDistinct(false);
    }

    @Test
    public void testSelfJoinWithDistinct() throws Exception {
        testSelfJoinWithDistinct(true);
    }

    @Test
    public void testSelfJoinWithDistinct_columnAliasExprDisabled() throws Exception {
        testSelfJoinWithDistinct(false);
    }

    @Test
    public void testSwappedTableAliases() throws Exception {
        testSwappedTableAliases(true);
    }

    @Test
    public void testSwappedTableAliases_columnAliasExprDisabled() throws Exception {
        testSwappedTableAliases(false);
    }

    @Test
    public void testTrickyAliasesTableNamesAsAliases() throws Exception {
        testTrickyAliasesTableNamesAsAliases(true);
    }

    @Test
    public void testTrickyAliasesTableNamesAsAliases_columnAliasExprDisabled() throws Exception {
        testTrickyAliasesTableNamesAsAliases(false);
    }

    private void testAliasesMatchingColumnNames(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table users (" +
                            "  user_id INT," +
                            "  name STRING," +
                            "  email STRING" +
                            ");"
            );
            execute(
                    "create table profiles (" +
                            "  profile_id INT," +
                            "  user_id INT," +
                            "  bio STRING" +
                            ");"
            );

            execute("insert into users values (1, 'Alice', 'alice@test.com');");
            execute("insert into users values (2, 'Bob', 'bob@test.com');");
            execute("insert into profiles values (101, 1, 'Software Developer');");
            execute("insert into profiles values (102, 2, 'Data Analyst');");

            assertQueryNoLeakCheck(
                    "name\temail\tbio\n" +
                            "Alice\talice@test.com\tSoftware Developer\n" +
                            "Bob\tbob@test.com\tData Analyst\n",
                    "select distinct name.name, email.email, bio.bio " +
                            "from users as name " +
                            "inner join users as email on name.user_id = email.user_id " +
                            "inner join profiles as bio on email.user_id = bio.user_id " +
                            "order by name.name"
            );
        });
    }

    private void testComplexJoinChainWithTrickyAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table departments (" +
                            "  dept_id INT," +
                            "  dept_name STRING" +
                            ");"
            );
            execute(
                    "create table employees (" +
                            "  emp_id INT," +
                            "  dept_id INT," +
                            "  emp_name STRING" +
                            ");"
            );
            execute(
                    "create table projects (" +
                            "  proj_id INT," +
                            "  proj_name STRING," +
                            "  dept_id INT" +
                            ");"
            );
            execute(
                    "create table assignments (" +
                            "  assign_id INT," +
                            "  emp_id INT," +
                            "  proj_id INT," +
                            "  role STRING" +
                            ");"
            );
            execute(
                    "create table tasks (" +
                            "  task_id INT," +
                            "  assign_id INT," +
                            "  task_name STRING" +
                            ");"
            );

            execute("insert into departments values (1, 'Engineering');");
            execute("insert into departments values (2, 'Marketing');");
            execute("insert into employees values (101, 1, 'Alice');");
            execute("insert into employees values (102, 2, 'Bob');");
            execute("insert into projects values (201, 'Website', 1);");
            execute("insert into projects values (202, 'Campaign', 2);");
            execute("insert into assignments values (301, 101, 201, 'Lead');");
            execute("insert into assignments values (302, 102, 202, 'Manager');");
            execute("insert into tasks values (401, 301, 'Design');");
            execute("insert into tasks values (402, 302, 'Strategy');");

            final String secondAlias = "emp_name";
            final String thirdAlias = "proj_id";
            final String fourthAlias = "role";

            assertQueryNoLeakCheck(
                    "dept_name\t" + secondAlias + "\t" + thirdAlias + "\t" + fourthAlias + "\n" +
                            "Engineering\tAlice\t201\tLead\n" +
                            "Marketing\tBob\t202\tManager\n",
                    "select distinct employees.dept_name, projects.emp_name, assignments.proj_id, tasks.role " +
                            "from departments as employees " +
                            "inner join employees as projects on employees.dept_id = projects.dept_id " +
                            "inner join projects as assignments on projects.dept_id = assignments.dept_id " +
                            "inner join assignments as tasks on projects.emp_id = tasks.emp_id " +
                            "inner join tasks as departments on tasks.assign_id = departments.assign_id " +
                            "order by employees.dept_name"
            );
        });
    }

    private void testConfusingMixedAliasScenarios(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "  id INT," +
                            "  y_id INT," +
                            "  z_value STRING" +
                            ");"
            );
            execute(
                    "create table y (" +
                            "  id INT," +
                            "  x_id INT," +
                            "  z_value STRING" +
                            ");"
            );
            execute(
                    "create table z (" +
                            "  id INT," +
                            "  x_id INT," +
                            "  y_value STRING" +
                            ");"
            );

            execute("insert into x values (1, 10, 'X1');");
            execute("insert into x values (2, 20, 'X2');");
            execute("insert into y values (10, 1, 'Y1');");
            execute("insert into y values (20, 2, 'Y2');");
            execute("insert into z values (100, 1, 'Z1');");
            execute("insert into z values (200, 2, 'Z2');");

            final String secondAlias = columnAliasExprEnabled ? "z_value_2" : "z_value1";
            assertQueryNoLeakCheck(
                    "z_value\t" + secondAlias + "\ty_value\n" +
                            "X1\tY1\tZ1\n" +
                            "X2\tY2\tZ2\n",
                    "select distinct z.z_value, y.z_value, x.y_value " +
                            "from x as z " +
                            "inner join y as y on z.y_id = y.id " +
                            "inner join z as x on z.id = x.x_id " +
                            "order by z.z_value"
            );
        });
    }

    private void testCrossJoinLeftJoinCombination(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table historical (" +
                            "  account_id INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts);"
            );
            execute(
                    "create table metrics (" +
                            "  metric SYMBOL" +
                            ");"
            );
            execute(
                    "create table current (" +
                            "  account_id INT," +
                            "  metric SYMBOL" +
                            ");"
            );

            execute("insert into historical values (1, '1970-01-01T00:00:00');");
            execute("insert into historical values (2, '1970-01-01T00:00:01');");
            execute("insert into metrics values ('cpu');");
            execute("insert into metrics values ('memory');");
            execute("insert into current values (1, 'cpu');");

            assertQueryNoLeakCheck(
                    "account_id\tmetric\n" +
                            "1\tmemory\n" +
                            "2\tcpu\n" +
                            "2\tmemory\n",
                    "select distinct historical.account_id, metrics.metric " +
                            "from historical " +
                            "cross join metrics " +
                            "left join current on historical.account_id = current.account_id and metrics.metric = current.metric " +
                            "where current.account_id is null"
            );
        });
    }

    private void testCrossJoinWithDistinct(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table metrics (" +
                            "  metric SYMBOL" +
                            ");"
            );
            execute(
                    "create table accounts (" +
                            "  account_id INT" +
                            ");"
            );

            execute("insert into metrics values ('cpu');");
            execute("insert into metrics values ('memory');");
            execute("insert into accounts values (1);");
            execute("insert into accounts values (2);");

            assertQueryNoLeakCheck(
                    "account_id\tmetric\n" +
                            "1\tcpu\n" +
                            "1\tmemory\n" +
                            "2\tcpu\n" +
                            "2\tmemory\n",
                    "select distinct accounts.account_id, metrics.metric " +
                            "from accounts " +
                            "cross join metrics " +
                            "order by account_id, metric"
            );
        });
    }

    private void testFiveTableJoinWithDistinct(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table customers (" +
                            "  customer_id INT," +
                            "  name STRING" +
                            ");"
            );
            execute(
                    "create table orders (" +
                            "  order_id INT," +
                            "  customer_id INT," +
                            "  order_date STRING" +
                            ");"
            );
            execute(
                    "create table order_items (" +
                            "  item_id INT," +
                            "  order_id INT," +
                            "  product_id INT," +
                            "  quantity INT" +
                            ");"
            );
            execute(
                    "create table products (" +
                            "  product_id INT," +
                            "  product_name STRING," +
                            "  category_id INT" +
                            ");"
            );
            execute(
                    "create table categories (" +
                            "  category_id INT," +
                            "  category_name STRING" +
                            ");"
            );

            execute("insert into customers values (1, 'Alice');");
            execute("insert into customers values (2, 'Bob');");
            execute("insert into orders values (101, 1, '2024-01-01');");
            execute("insert into orders values (102, 2, '2024-01-02');");
            execute("insert into order_items values (1, 101, 201, 2);");
            execute("insert into order_items values (2, 102, 202, 1);");
            execute("insert into products values (201, 'Laptop', 1);");
            execute("insert into products values (202, 'Mouse', 1);");
            execute("insert into categories values (1, 'Electronics');");

            assertQueryNoLeakCheck(
                    "name\tproduct_name\tcategory_name\n" +
                            "Alice\tLaptop\tElectronics\n" +
                            "Bob\tMouse\tElectronics\n",
                    "select distinct customers.name, products.product_name, categories.category_name " +
                            "from customers " +
                            "inner join orders on customers.customer_id = orders.customer_id " +
                            "inner join order_items on orders.order_id = order_items.order_id " +
                            "inner join products on order_items.product_id = products.product_id " +
                            "inner join categories on products.category_id = categories.category_id " +
                            "order by name, product_name"
            );
        });
    }

    private void testInnerJoinWithAsAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");

            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\n" +
                            "air\t1\n",
                    "select distinct samples_tbl.sensor_id, sensors_tbl.apptype " +
                            "from samples as samples_tbl " +
                            "inner join sensors as sensors_tbl on sensors_tbl.sensor_id = samples_tbl.sensor_id"
            );
        });
    }

    private void testInnerJoinWithColumnAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");

            assertQueryNoLeakCheck(
                    "sensor_name\tapplication_type\n" +
                            "air\t1\n",
                    "select distinct samples.sensor_id as sensor_name, sensors.apptype as application_type " +
                            "from samples " +
                            "inner join sensors on sensors.sensor_id = samples.sensor_id"
            );
        });
    }

    private void testInnerJoinWithShortAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");

            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\n" +
                            "air\t1\n",
                    "select distinct sa.sensor_id, se.apptype " +
                            "from samples sa " +
                            "inner join sensors se on se.sensor_id = sa.sensor_id"
            );
        });
    }

    private void testInnerJoinWithTablePrefixedColumns(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('water', 2, '1970-02-02T10:10:01');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");
            execute("insert into samples values ('air', '1970-02-02T10:10:01');");

            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\n" +
                            "air\t1\n",
                    "select distinct samples.sensor_id, sensors.apptype " +
                            "from samples " +
                            "inner join sensors on sensors.sensor_id = samples.sensor_id"
            );
        });
    }

    private void testLeftJoinWithTablePrefixedColumns(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('water', 2, '1970-02-02T10:10:01');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");
            execute("insert into samples values ('soil', '1970-02-02T10:10:02');");

            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\n" +
                            "air\t1\n" +
                            "soil\tnull\n",
                    "select distinct samples.sensor_id, sensors.apptype " +
                            "from samples " +
                            "left join sensors on sensors.sensor_id = samples.sensor_id"
            );
        });
    }

    private void testMultiTableJoinWithAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table users (" +
                            "  user_id INT," +
                            "  name STRING" +
                            ");"
            );
            execute(
                    "create table orders (" +
                            "  order_id INT," +
                            "  user_id INT," +
                            "  product_id INT" +
                            ");"
            );
            execute(
                    "create table products (" +
                            "  product_id INT," +
                            "  product_name STRING" +
                            ");"
            );

            execute("insert into users values (1, 'Alice');");
            execute("insert into users values (2, 'Bob');");
            execute("insert into orders values (101, 1, 201);");
            execute("insert into orders values (102, 1, 202);");
            execute("insert into orders values (103, 2, 201);");
            execute("insert into products values (201, 'Laptop');");
            execute("insert into products values (202, 'Mouse');");

            assertQueryNoLeakCheck(
                    "name\tproduct_name\n" +
                            "Alice\tLaptop\n" +
                            "Alice\tMouse\n" +
                            "Bob\tLaptop\n",
                    "select distinct users.name, products.product_name " +
                            "from users " +
                            "inner join orders on users.user_id = orders.user_id " +
                            "inner join products on orders.product_id = products.product_id " +
                            "order by name, product_name"
            );
        });
    }

    private void testNestedJoinsWithDistinct(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table a (" +
                            "  id INT," +
                            "  value STRING" +
                            ");"
            );
            execute(
                    "create table b (" +
                            "  id INT," +
                            "  a_id INT," +
                            "  value STRING" +
                            ");"
            );
            execute(
                    "create table c (" +
                            "  id INT," +
                            "  b_id INT," +
                            "  value STRING" +
                            ");"
            );

            execute("insert into a values (1, 'A1');");
            execute("insert into a values (2, 'A2');");
            execute("insert into b values (10, 1, 'B1');");
            execute("insert into b values (20, 2, 'B2');");
            execute("insert into c values (100, 10, 'C1');");
            execute("insert into c values (200, 20, 'C2');");

            final String secondAlias = columnAliasExprEnabled ? "value_2" : "value1";
            final String thirdAlias = columnAliasExprEnabled ? "value_3" : "value2";
            assertQueryNoLeakCheck(
                    "value\t" + secondAlias + "\t" + thirdAlias + "\n" +
                            "A1\tB1\tC1\n" +
                            "A2\tB2\tC2\n",
                    "select distinct a.value, b.value, c.value " +
                            "from a " +
                            "inner join b on a.id = b.a_id " +
                            "inner join c on b.id = c.b_id"
            );
        });
    }

    private void testSelfJoinWithDistinct(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  parent_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );

            execute("insert into sensors values ('air1', 'main', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('air2', 'main', 1, '1970-02-02T10:10:01');");
            execute("insert into sensors values ('main', null, 2, '1970-02-02T10:10:02');");

            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\n" +
                            "air1\t2\n" +
                            "air2\t2\n",
                    "select distinct s1.sensor_id, s2.apptype " +
                            "from sensors s1 " +
                            "inner join sensors s2 on s1.parent_id = s2.sensor_id"
            );
        });
    }

    private void testSwappedTableAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  type STRING," +
                            "  location_id INT" +
                            ");"
            );
            execute(
                    "create table samples (" +
                            "  sample_id INT," +
                            "  sensor_id SYMBOL," +
                            "  value DOUBLE" +
                            ");"
            );

            execute("insert into sensors values ('temp1', 'temperature', 1);");
            execute("insert into sensors values ('hum1', 'humidity', 1);");
            execute("insert into samples values (1, 'temp1', 25.5);");
            execute("insert into samples values (2, 'hum1', 60.0);");

            assertQueryNoLeakCheck(
                    "sensor_id\ttype\tvalue\n" +
                            "hum1\thumidity\t60.0\n" +
                            "temp1\ttemperature\t25.5\n",
                    "select distinct sensors.sensor_id, samples.type, sensors.value " +
                            "from sensors as samples " +
                            "inner join samples as sensors on samples.sensor_id = sensors.sensor_id " +
                            "order by sensors.sensor_id"
            );
        });
    }

    private void testTrickyAliasesTableNamesAsAliases(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table table1 (" +
                            "  id INT," +
                            "  value STRING" +
                            ");"
            );
            execute(
                    "create table table2 (" +
                            "  id INT," +
                            "  table1_id INT," +
                            "  value STRING" +
                            ");"
            );
            execute(
                    "create table table3 (" +
                            "  id INT," +
                            "  table2_id INT," +
                            "  value STRING" +
                            ");"
            );

            execute("insert into table1 values (1, 'A1');");
            execute("insert into table1 values (2, 'A2');");
            execute("insert into table2 values (10, 1, 'B1');");
            execute("insert into table2 values (20, 2, 'B2');");
            execute("insert into table3 values (100, 10, 'C1');");
            execute("insert into table3 values (200, 20, 'C2');");

            final String secondAlias = columnAliasExprEnabled ? "value_2" : "value1";
            final String thirdAlias = columnAliasExprEnabled ? "value_3" : "value2";
            assertQueryNoLeakCheck(
                    "value\t" + secondAlias + "\t" + thirdAlias + "\n" +
                            "A1\tB1\tC1\n" +
                            "A2\tB2\tC2\n",
                    "select distinct table2.value, table3.value, table1.value " +
                            "from table1 as table2 " +
                            "inner join table2 as table3 on table2.id = table3.table1_id " +
                            "inner join table3 as table1 on table3.id = table1.table2_id " +
                            "order by table2.value"
            );
        });
    }
}