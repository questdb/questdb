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

public class TranslateFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstants() throws Exception {
        // 'from' longer than 'to': the extra 'from' chars (here '3') are removed
        assertQuery("select translate('12345', '143', 'ax')").expectSize().returns("translate\na2x5\n");
        // positional mapping e->i, l->p
        assertQuery("select translate('hello', 'el', 'ip')").expectSize().returns("translate\nhippo\n");
        // empty 'to' deletes every matched character
        assertQuery("select translate('abcdef', 'abc', '')").expectSize().returns("translate\ndef\n");
        // empty 'from' leaves the input unchanged
        assertQuery("select translate('hello', '', 'xyz')").expectSize().returns("translate\nhello\n");
        // Unicode characters are handled
        assertQuery("select translate('café', 'é', 'e')").expectSize().returns("translate\ncafe\n");
        // duplicate char in 'from' uses the first mapping
        assertQuery("select translate('aaa', 'aa', 'xy')").expectSize().returns("translate\nxxx\n");
        // VARCHAR overload
        assertQuery("select translate('12345'::varchar, '143'::varchar, 'ax'::varchar)").expectSize().returns("translate\na2x5\n");
    }

    @Test
    public void testNullAndColumn() throws Exception {
        // per-row path over a column, including NULL and empty input
        assertQuery("select s, translate(s, 'lo', 'LO') from t")
                .ddl("create table t (s string)",
                        "insert into t values ('hello'), ('world'), (null), ('')")
                .expectSize()
                .returns(
                        "s\ttranslate\n" +
                                "hello\theLLO\n" +
                                "world\twOrLd\n" +
                                "\t\n" +
                                "\t\n"
                );
    }
}
