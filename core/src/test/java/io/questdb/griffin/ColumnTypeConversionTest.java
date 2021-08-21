/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import org.junit.Test;

public class ColumnTypeConversionTest extends AbstractGriffinTest {
    @Test
    public void charColumnImplicitlyCastToString() throws Exception {
        assertQuery(
                "replace\n" +
                        "a<>c\n" +
                        "<>bc\n" +
                        "<>bc\n" +
                        "ab<>\n" +
                        "a<>c\n" +
                        "ab<>\n" +
                        "ab<>\n" +
                        "<>bc\n" +
                        "ab<>\n" +
                        "a<>c\n",
                "select replace('abc', c, '<>') from t1",
                "create table t1 as (" +
                        "select cast(rnd_str('a', 'b', 'c') as char) c," +
                        "rnd_str('aaaa', 'bbb', 'cccc') s " +
                        "from long_sequence(10)" +
                        ")",
                null,
                false,
                false

        );
    }
}
