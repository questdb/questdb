/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LastNotNullSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNotKeyed() throws Exception {
        // last_not_null skips trailing NULL symbols and returns the last
        // non-null value in scan order.
        assertQuery(
                """
                        sym
                        bb
                        """,
                "select last_not_null(s) sym from tab",
                "create table tab as (select (case x when 2 then 'aa' when 4 then 'bb' end)::symbol s from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testNotKeyedConstantOverEmpty() throws Exception {
        // last_not_null(constant) over a WHERE-folded empty table must return NULL:
        // setEmpty stores VALUE_IS_NULL on the group-by state, and SymbolConstant.valueOf
        // must honour that key. LastNotNullSymbolGroupByFunction extends
        // FirstSymbolGroupByFunction and inherits the same VALUE_IS_NULL read-back, so
        // before the fix the constant was returned verbatim.
        assertQuery(
                """
                        a0

                        """,
                "select last_not_null(('0.83055')::symbol) a0 from tab where 1 = 0",
                "create table tab as (select rnd_int() a from long_sequence(10))",
                null,
                false,
                true
        );
    }
}
