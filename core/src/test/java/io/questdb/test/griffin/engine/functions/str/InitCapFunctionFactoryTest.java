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

public class InitCapFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstants() throws Exception {
        // first letter of each word to upper, the rest to lower; words are alphanumeric runs
        assertQuery("select initcap('hello world')").expectSize().returns("initcap\nHello World\n");
        assertQuery("select initcap('hELLO wORLD')").expectSize().returns("initcap\nHello World\n");
        // non-alphanumeric characters (apostrophe, hyphen, underscore) act as word separators
        assertQuery("select initcap('o''reilly-style_name')").expectSize().returns("initcap\nO'Reilly-Style_Name\n");
        // a leading digit is the first character of the word, so the following letters are lowercased
        assertQuery("select initcap('123ABC def')").expectSize().returns("initcap\n123abc Def\n");
        // Unicode letters are handled
        assertQuery("select initcap('CAFÉ crème')").expectSize().returns("initcap\nCafé Crème\n");
        // empty string stays empty
        assertQuery("select initcap('')").expectSize().returns("initcap\n\n");
        // VARCHAR overload
        assertQuery("select initcap('hELLO wORLD'::varchar)").expectSize().returns("initcap\nHello World\n");
    }

    @Test
    public void testNullAndColumn() throws Exception {
        // exercises the per-row path over a column, including NULL and repeated separators
        assertQuery("select s, initcap(s) from t")
                .ddl("create table t (s string)",
                        "insert into t values ('hELLO wORLD'), ('multi   space'), (null), ('')")
                .expectSize()
                .returns(
                        "s\tinitcap\n" +
                                "hELLO wORLD\tHello World\n" +
                                "multi   space\tMulti   Space\n" +
                                "\t\n" +
                                "\t\n"
                );
    }
}
