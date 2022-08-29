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
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RegexpReplaceStrFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final RecordCursorFactory factory = compiler.compile("select regexp_replace('abc', null, 'def'))", sqlExecutionContext).getRecordCursorFactory();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.hasNext();
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(29, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "NULL regex");
            }
        });
    }

    @Test
    public void testNullReplacement() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final RecordCursorFactory factory = compiler.compile("select regexp_replace('abc', 'a', null)", sqlExecutionContext).getRecordCursorFactory();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.hasNext();
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "NULL replacement");
            }
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final RecordCursorFactory factory = compiler.compile("select regexp_replace('a b c', 'XJ**', ' ')", sqlExecutionContext).getRecordCursorFactory();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.hasNext();
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "Dangling meta character");
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "regexp_replace\n" +
                    "example1.com\n" +
                    "example1.com\n" +
                    "example2.com\n" +
                    "example2.com\n" +
                    "example2.com\n";
            compiler.compile("create table x as (select rnd_str('https://example1.com/abc','https://example2.com/def') url from long_sequence(5))", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("select regexp_replace(url, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') from x", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }
}