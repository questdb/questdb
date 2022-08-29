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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RegexpReplaceStrFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testNullRegex() throws Exception {
        assertFailure(
                "[29] NULL regex",
                "select regexp_replace('abc', null, 'def'))"
        );
    }

    @Test
    public void testNullReplacement() throws Exception {
        assertFailure(
                "[34] NULL replacement",
                "select regexp_replace('abc', 'a', null)"
        );
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertFailure(
                "[35] Dangling meta character '*' near index 3\n" +
                        "XJ**\n" +
                        "   ^",
                "select regexp_replace('a b c', 'XJ**', ' ')"
        );
    }

    @Test
    public void testWhenChainedCallsExceedsMaxLengthExceptionIsThrown() throws Exception {
        assertFailure(
                "[-1] breached memory limit set for regexp_replace(SSS) [maxLength=1048576]",
                "select regexp_replace(regexp_replace(regexp_replace(regexp_replace( 'aaaaaaaaaaaaaaaaaaaa', 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa')"
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "regexp_replace\n" +
                    "example1.com\n" +
                    "http://example3.com\n" +
                    "example2.com\n" +
                    "\n" +
                    "example2.com\n";
            compiler.compile("create table x as (select rnd_str('https://example1.com/abc','https://example2.com/def','http://example3.com',null) url from long_sequence(5))", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("select regexp_replace(url, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') from x", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    private void assertFailure(CharSequence expectedMsg, CharSequence sql) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                printer.print(cursor, factory.getMetadata(), true, sink);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(expectedMsg, e.getMessage());
            }
        });
    }
}