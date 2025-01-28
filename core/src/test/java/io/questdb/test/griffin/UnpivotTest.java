/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlException;
import org.junit.Test;

public class UnpivotTest extends AbstractSqlParserTest {

    public static String ddlMonthlySales = "CREATE TABLE monthly_sales\n" +
            "    (empid INT, dept SYMBOL, Jan INT, Feb INT, Mar INT, Apr INT, May INT, Jun INT);\n";
    public static String dmlMonthlySales =   "INSERT INTO monthly_sales VALUES\n" +
            "    (1, 'electronics', 1, 2, 3, 4, 5, 6),\n" +
            "    (2, 'clothes', 10, 20, 30, 40, 50, 60),\n" +
            "    (3, 'cars', 100, 200, 300, 400, 500, 600);";


    @Test
    public void testBasicUnpivot() throws Exception {
       assertMemoryLeak(() -> {
           execute(ddlMonthlySales);
           execute(dmlMonthlySales);

           String unpivot = "FROM monthly_sales UNPIVOT (\n" +
                   "    sales\n" +
                   "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                   ");";

           String target = "SELECT empid, dept, 'jan' as month, jan as sales FROM monthly_sales\n" +
                   "UNION\n" +
                   "SELECT empid, dept, 'feb' as month, feb as sales FROM monthly_sales\n" +
                   "UNION \n" +
                   "SELECT empid, dept, 'mar' as month, mar as sales FROM monthly_sales\n" +
                   "UNION\n" +
                   "SELECT empid, dept, 'apr' as month, apr as sales FROM monthly_sales\n" +
                   "UNION\n" +
                   "SELECT empid, dept, 'may' as month, may as sales FROM monthly_sales\n" +
                   "UNION\n" +
                   "SELECT empid, dept, 'jun' as month, jun as sales FROM monthly_sales;";

           assertSql("", target);
//           assertSql("", unpivot);
       });
    }
}
