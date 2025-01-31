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
import io.questdb.griffin.model.QueryModel;
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

           String unpivot = "monthly_sales UNPIVOT (\n" +
                   "    sales\n" +
                   "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                   ");";

           assertPlanNoLeakCheck(unpivot, "Unpivot\n" +
                   "  into: sales\n" +
                   "  for: month\n" +
                   "  in: [Jan,Feb,Mar,Apr,May,Jun]\n" +
                   "    PageFrame\n" +
                   "        Row forward scan\n" +
                   "        Frame forward scan on: monthly_sales\n");

           assertSql("empid\tdept\tmonth\tsales\n" +
                   "1\telectronics\tJan\t1\n" +
                   "1\telectronics\tFeb\t2\n" +
                   "1\telectronics\tMar\t3\n" +
                   "1\telectronics\tApr\t4\n" +
                   "1\telectronics\tMay\t5\n" +
                   "1\telectronics\tJun\t6\n" +
                   "2\tclothes\tJan\t10\n" +
                   "2\tclothes\tFeb\t20\n" +
                   "2\tclothes\tMar\t30\n" +
                   "2\tclothes\tApr\t40\n" +
                   "2\tclothes\tMay\t50\n" +
                   "2\tclothes\tJun\t60\n" +
                   "3\tcars\tJan\t100\n" +
                   "3\tcars\tFeb\t200\n" +
                   "3\tcars\tMar\t300\n" +
                   "3\tcars\tApr\t400\n" +
                   "3\tcars\tMay\t500\n" +
                   "3\tcars\tJun\t600\n", unpivot);
       });
    }
}



