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

package io.questdb.test.griffin;

import io.questdb.griffin.QueryVariablesText;
import io.questdb.griffin.SqlParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The text the parser reads a client's variables from: blanks up to
 * {@link SqlParser#QUERY_VARIABLES_POSITION}, then the variables.
 */
public class QueryVariablesTextTest {
    private static final int AT = SqlParser.QUERY_VARIABLES_POSITION;

    @Test
    public void testBlanksAheadOfVariables() {
        final QueryVariablesText text = new QueryVariablesText();
        text.of("@a := 1");
        Assert.assertEquals(AT + 7, text.length());
        Assert.assertEquals(' ', text.charAt(0));
        Assert.assertEquals(' ', text.charAt(AT - 1));
        Assert.assertEquals('@', text.charAt(AT));
        Assert.assertEquals('1', text.charAt(AT + 6));
    }

    @Test
    public void testHoldsCopyOfVariables() {
        final StringSink variables = new StringSink();
        variables.put("@a := 1");
        final QueryVariablesText text = new QueryVariablesText();
        text.of(variables);
        variables.clear();
        variables.put("@b := 22");
        TestUtils.assertEquals("@a := 1", text.getVariables());
        // and holds only the last variables it was set to
        text.of(variables);
        TestUtils.assertEquals("@b := 22", text.getVariables());
        Assert.assertEquals(AT + 8, text.length());
    }

    @Test
    public void testStringsLeaveBlanksOut() {
        final QueryVariablesText text = new QueryVariablesText();
        text.of("@a := 1");
        TestUtils.assertEquals("@a := 1", text.toString());
        TestUtils.assertEquals(":=", text.subSequence(AT + 3, AT + 5));
        // the blanks in a sequence are left out
        TestUtils.assertEquals("@a", text.subSequence(0, AT + 2));
        TestUtils.assertEquals("", text.subSequence(5, 10));
    }
}
