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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.engine.functions.regex.RegexpReplaceVarcharFunctionFactory.canSkipUtf8Decoding;

public class RegexpReplaceVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCanSkipUtf8Decoding() {
        Assert.assertTrue(canSkipUtf8Decoding("^https?://(?:www\\.)?([^/]+)/.*$"));
        Assert.assertTrue(canSkipUtf8Decoding("^([^/]+)$"));
        Assert.assertTrue(canSkipUtf8Decoding("what a test"));

        Assert.assertFalse(canSkipUtf8Decoding("what a тест"));
        Assert.assertFalse(canSkipUtf8Decoding("[^abc]"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\x]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\D]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\d]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\B]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\b]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\S]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\s]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\W]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\w]+"));
        Assert.assertFalse(canSkipUtf8Decoding("[\\p{Lower}]+"));
    }

    @Test
    public void testNonExistingGroupIndex() throws Exception {
        assertFailure(
                "no group 11",
                "select regexp_replace('abc'::varchar, '^https?://(?:www\\.)?([^/]+)/.*$', '$11')"
        );
    }

    @Test
    public void testNullRegex() throws Exception {
        assertQuery("select regexp_replace('abc'::varchar, null, 'def')")
                .expectSize()
                .returns("""
                        regexp_replace
                        
                        """);
    }

    @Test
    public void testNullReplacement() throws Exception {
        assertQuery("select regexp_replace('abc'::varchar, 'a', null)")
                .expectSize()
                .returns("""
                        regexp_replace
                        
                        """);
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertFailure(
                "Dangling meta character '*'",
                "select regexp_replace('a b c'::varchar, 'XJ**', ' ')"
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('https://example1.com/abc','https://example2.com/def','http://example3.com',null) url from long_sequence(5))");

            assertQuery("select regexp_replace(url, 'example', 'foobar') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            https://foobar1.com/abc
                            http://foobar3.com
                            https://foobar2.com/def
                            
                            https://foobar2.com/def
                            """);
        });
    }

    @Test
    public void testSingleGroupAsciiStable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('https://example1.com/abc','https://example2.com/def','http://example3.com','http://example4.com?q=форсаж','фубар',null) url from long_sequence(20))");

            assertQuery("select regexp_replace(url, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            example1.com
                            example1.com
                            example2.com
                            
                            
                            
                            http://example3.com
                            example2.com
                            example1.com
                            фубар
                            фубар
                            http://example3.com
                            фубар
                            example2.com
                            example2.com
                            example1.com
                            example1.com
                            example2.com
                            http://example4.com?q=форсаж
                            фубар
                            """);

            assertQuery("select regexp_replace(url, '^https?://(?:www\\.)?([^/]+)/.*$', '$0') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            https://example1.com/abc
                            https://example1.com/abc
                            https://example2.com/def
                            
                            
                            
                            http://example3.com
                            https://example2.com/def
                            https://example1.com/abc
                            фубар
                            фубар
                            http://example3.com
                            фубар
                            https://example2.com/def
                            https://example2.com/def
                            https://example1.com/abc
                            https://example1.com/abc
                            https://example2.com/def
                            http://example4.com?q=форсаж
                            фубар
                            """);
        });
    }

    @Test
    public void testSingleGroupAsciiUnstable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('https://example1.com/abc','https://example2.com/def','http://example3.com','http://example4.com?q=форсаж','фубар',null) url from long_sequence(20))");

            assertQuery("select regexp_replace(concat(url, '_'), '^https?://(?:www\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            example1.com
                            example1.com
                            example2.com
                            _
                            _
                            _
                            http://example3.com_
                            example2.com
                            example1.com
                            фубар_
                            фубар_
                            http://example3.com_
                            фубар_
                            example2.com
                            example2.com
                            example1.com
                            example1.com
                            example2.com
                            http://example4.com?q=форсаж_
                            фубар_
                            """);

            assertQuery("select regexp_replace(concat(url, '_'), '^https?://(?:www\\.)?([^/]+)/.*$', '$0') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            https://example1.com/abc_
                            https://example1.com/abc_
                            https://example2.com/def_
                            _
                            _
                            _
                            http://example3.com_
                            https://example2.com/def_
                            https://example1.com/abc_
                            фубар_
                            фубар_
                            http://example3.com_
                            фубар_
                            https://example2.com/def_
                            https://example2.com/def_
                            https://example1.com/abc_
                            https://example1.com/abc_
                            https://example2.com/def_
                            http://example4.com?q=форсаж_
                            фубар_
                            """);
        });
    }

    @Test
    public void testSingleGroupStable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('https://пример.com/abc','https://example2.com/def','http://пример.com','http://пример.com?q=форсаж','фубар',null) url from long_sequence(20))");

            assertQuery("select regexp_replace(url, '^https?://(?:пример\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            com
                            com
                            example2.com
                            
                            
                            
                            http://пример.com
                            example2.com
                            com
                            фубар
                            фубар
                            http://пример.com
                            фубар
                            example2.com
                            example2.com
                            com
                            com
                            example2.com
                            http://пример.com?q=форсаж
                            фубар
                            """);

            assertQuery("select regexp_replace(url, '^https?://(?:пример\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            com
                            com
                            example2.com
                            
                            
                            
                            http://пример.com
                            example2.com
                            com
                            фубар
                            фубар
                            http://пример.com
                            фубар
                            example2.com
                            example2.com
                            com
                            com
                            example2.com
                            http://пример.com?q=форсаж
                            фубар
                            """);
        });
    }

    @Test
    public void testSingleGroupUnstable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar('https://пример.com/abc','https://example2.com/def','http://пример.com','http://пример.com?q=форсаж','фубар',null) url from long_sequence(20))");

            assertQuery("select regexp_replace(concat(url, '_'), '^https?://(?:пример\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            com
                            com
                            example2.com
                            _
                            _
                            _
                            http://пример.com_
                            example2.com
                            com
                            фубар_
                            фубар_
                            http://пример.com_
                            фубар_
                            example2.com
                            example2.com
                            com
                            com
                            example2.com
                            http://пример.com?q=форсаж_
                            фубар_
                            """);

            assertQuery("select regexp_replace(concat(url, '_'), '^https?://(?:пример\\.)?([^/]+)/.*$', '$1') from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            regexp_replace
                            com
                            com
                            example2.com
                            _
                            _
                            _
                            http://пример.com_
                            example2.com
                            com
                            фубар_
                            фубар_
                            http://пример.com_
                            фубар_
                            example2.com
                            example2.com
                            com
                            com
                            example2.com
                            http://пример.com?q=форсаж_
                            фубар_
                            """);
        });
    }

    @Test
    public void testWhenChainedCallsExceedsMaxLengthExceptionIsThrown() throws Exception {
        assertFailure(
                "breached memory limit set for regexp_replace(SSS) [maxLength=1048576]",
                "select regexp_replace(regexp_replace(regexp_replace(regexp_replace('aaaaaaaaaaaaaaaaaaaa'::varchar, 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa')"
        );
    }

    private void assertFailure(CharSequence expectedMsg, CharSequence sql) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final RecordCursorFactory factory = select(sql);
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                println(factory, cursor);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), expectedMsg);
            }
        });
    }
}
