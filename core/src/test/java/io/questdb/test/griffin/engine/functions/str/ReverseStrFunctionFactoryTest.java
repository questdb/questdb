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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ReverseStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstants() throws Exception {
        assertQuery("select reverse('abc')").expectSize().returns("reverse\ncba\n");
        assertQuery("select reverse('Hello, World!')").expectSize().returns("reverse\n!dlroW ,olleH\n");
        assertQuery("select reverse('a')").expectSize().returns("reverse\na\n");
        // empty string stays empty
        assertQuery("select reverse('')").expectSize().returns("reverse\n\n");
        // Unicode (BMP) characters
        assertQuery("select reverse('café')").expectSize().returns("reverse\néfac\n");
        // VARCHAR overload
        assertQuery("select reverse('abc'::varchar)").expectSize().returns("reverse\ncba\n");
    }

    @Test
    public void testNullAndColumn() throws Exception {
        assertQuery("select s, reverse(s) from t")
                .ddl("create table t (s string)",
                        "insert into t values ('abc'), ('Hello'), (null), ('')")
                .expectSize()
                .returns(
                        "s\treverse\n" +
                                "abc\tcba\n" +
                                "Hello\tolleH\n" +
                                "\t\n" +
                                "\t\n"
                );
    }
}
