/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class GroupByTest extends AbstractGriffinTest  {
    @Test
    public void testAllConstant() throws Exception {
        String[] functions = {"min", "max", "avg", "last", "first", "sum"};
        String[] values = {"0", "0.0", "cast(0.0 as float)", "0L", "null", "false",
                "'1'", "cast(0 as byte)", "cast(0 as short)",
                "'2021-01-01'", "to_date('2021-01-01', 'yyyy-mm-dd')", "to_timestamp('2021-01-01', 'yyyy-mm-dd')"};
        String[] expected = {"0", "0.0", "0.0000", "0", "NaN", "false",
                "1", "0", "0",
                "2021-01-01", "2021-01-01T00:01:00.000Z", "2021-01-01T00:01:00.000000Z"};

        for(String fn : functions) {
            for(int i=0;i<values.length;i++) {
                String val = values[i];
                if((val.contains("-") && !val.contains("(")) && (fn.equals("avg") || fn.equals("sum") || fn.equals("min") || fn.equals("max"))) {
                    //string not defined
                    continue;
                }
                if(val.contains("-") && (fn.equals("avg") || fn.equals("sum"))) {
                    //date not defined
                    continue;
                }
                if((val.equals("'1'") || val.contains("-")) && (fn.equals("avg") || fn.equals("sum"))) {
                    // char not defined
                    continue;
                }
                String exp = expected[i];
                if((fn.equals("min") || fn.equals("max") || fn.equals("avg") || fn.equals("sum")) && exp.equals("false")) {
                    // boolean gets converted to double here
                    exp = "0.0";
                } else if(fn.equals("avg") && !exp.equals("NaN")) {
                    // avg always returns double
                    exp = "0.0";
                }

                System.out.println(fn + "/" + val + "/" + exp);
                assertSql(
                        "select "+ fn + "(" + val + ")",
                        fn + "\n" +
                                exp + "\n"
                );

                assertSql(
                        "select key,"+ fn + "(x) from (select " + val + " x, 'a' key)",
                        "key\t" + fn + "\n"
                                + "a\t" + exp + "\n"
                );
            }
        }
    }
}
