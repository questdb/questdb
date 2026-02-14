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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class TrimVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyOrNullTrimSpace() throws Exception {
        assertQuery(
                "t1\tt2\tt3\tt4\n" +
                        "\t\t\t\n",
                "select trim(''::varchar) t1, trim(' '::varchar) t2, trim('       '::varchar) t3, trim(null::varchar) t4",
                true
        );
    }

    @Test
    public void testNotTrimSpace() throws Exception {
        assertQuery(
                "t1\tt2\tt3\n" +
                        "a b c\tzzz\t()  /  {}\n",
                "select trim('a b c'::varchar) t1, trim('zzz'::varchar) t2, trim('()  /  {}'::varchar) t3",
                true
        );
    }

    @Test
    public void testTrimSpace() throws Exception {
        assertQuery(
                "t1\tt2\tt3\tt4\n" +
                        "abc\tabc\tabc\ta b c\n",
                "select trim('    abc     '::varchar) t1, trim('abc     '::varchar) t2, trim('     abc'::varchar) t3, trim(' a b c '::varchar) t4",
                true
        );
    }
}
