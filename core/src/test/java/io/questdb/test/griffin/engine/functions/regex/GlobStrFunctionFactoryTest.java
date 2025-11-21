/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.GlobStrFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class GlobStrFunctionFactoryTest {

    @Test
    public void testConvertGlobPatternBackslashes() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("path\\to\\file*.txt", sink, 0);
        Assert.assertEquals("^path\\\\to\\\\file.*\\.txt$", sink.toString());
    }

    // Bracket expressions with negation
    @Test
    public void testConvertGlobPatternBracketWithNegation() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file[!abc].txt", sink, 0);
        Assert.assertEquals("^file[^abc]\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternCaretEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file^name.txt", sink, 0);
        Assert.assertEquals("^file\\^name\\.txt$", sink.toString());
    }

    // Path patterns
    @Test
    public void testConvertGlobPatternComplexPath() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("/var/log/app_*.log", sink, 0);
        Assert.assertEquals("^/var/log/app_.*\\.log$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternCurlyBracesEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file{1,2}.txt", sink, 0);
        Assert.assertEquals("^file\\{1,2\\}\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternDollarEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file$name.txt", sink, 0);
        Assert.assertEquals("^file\\$name\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternEmptyBracket() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file[].txt", sink, 0);
        Assert.assertEquals("^file[]\\.txt$", sink.toString());
    }

    // Edge cases: empty and special patterns
    @Test
    public void testConvertGlobPatternEmptyString() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("", sink, 0);
        Assert.assertEquals("^$", sink.toString());
    }

    // Escaped characters handling
    @Test
    public void testConvertGlobPatternEscapedCharacters() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file\\\\.txt", sink, 0);
        Assert.assertEquals("^file\\\\\\\\\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternMixedSlashesAndBackslashes() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("path/to\\data/file*.txt", sink, 0);
        Assert.assertEquals("^path/to\\\\data/file.*\\.txt$", sink.toString());
    }

    // Nested patterns with multiple wildcards
    @Test
    public void testConvertGlobPatternMultipleConsecutiveAsterisks() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file***.txt", sink, 0);
        Assert.assertEquals("^file.*.*.*\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternMultipleUnmatchedBrackets() {
        StringSink sink = new StringSink();
        try {
            GlobStrFunctionFactory.convertGlobPatternToRegex("file[abc[def.txt", sink, 0);
            Assert.fail("Expected SqlException for unmatched brackets");
        } catch (SqlException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "unbalanced bracket"));
        }
    }

    @Test
    public void testConvertGlobPatternNegationInsideBracket() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("[!xyz]", sink, 0);
        Assert.assertEquals("^[^xyz]$", sink.toString());
    }

    // Negation handling
    @Test
    public void testConvertGlobPatternNegationOutsideBracket() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file!abc.txt", sink, 0);
        Assert.assertEquals("^file!abc\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternNestedBracketNegation() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file[!a-zA-Z0-9].txt", sink, 0);
        Assert.assertEquals("^file[^a-zA-Z0-9]\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternOnlyAsterisk() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("*", sink, 0);
        Assert.assertEquals("^.*$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternOnlyQuestionMark() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("?", sink, 0);
        Assert.assertEquals("^.$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternParenthesesEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file(name).txt", sink, 0);
        Assert.assertEquals("^file\\(name\\)\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternPipeEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file|name.txt", sink, 0);
        Assert.assertEquals("^file\\|name\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternPlusEscaped() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file+name.txt", sink, 0);
        Assert.assertEquals("^file\\+name\\.txt$", sink.toString());
    }

    // Basic glob patterns
    @Test
    public void testConvertGlobPatternSimpleAsterisk() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("*.txt", sink, 0);
        Assert.assertEquals("^.*\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternSimpleBracket() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file[abc].txt", sink, 0);
        Assert.assertEquals("^file[abc]\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternSimpleQuestionMark() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file?.txt", sink, 0);
        Assert.assertEquals("^file.\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternSingleCharacter() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("a", sink, 0);
        Assert.assertEquals("^a$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternSlashes() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("path/to/file*.txt", sink, 0);
        Assert.assertEquals("^path/to/file.*\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternSpecialRegexChars() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file.^$+{}()|txt", sink, 0);
        Assert.assertEquals("^file\\.\\^\\$\\+\\{\\}\\(\\)\\|txt$", sink.toString());
    }

    // Unicode and special characters
    @Test
    public void testConvertGlobPatternUnicodeCharacters() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("файл*.txt", sink, 0);
        Assert.assertEquals("^файл.*\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternUnlimitedDirectory() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("**/file.txt", sink, 0);
        Assert.assertEquals("^.*/file\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternUnmatchedCloseBracket() {
        StringSink sink = new StringSink();
        try {
            GlobStrFunctionFactory.convertGlobPatternToRegex("]abc.txt", sink, 0);
            Assert.fail("Expected SqlException for unmatched closing bracket");
        } catch (SqlException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "unbalanced bracket"));
        }
    }

    // Malformed patterns: unmatched brackets
    @Test
    public void testConvertGlobPatternUnmatchedOpenBracket() {
        StringSink sink = new StringSink();
        try {
            GlobStrFunctionFactory.convertGlobPatternToRegex("file[abc.txt", sink, 0);
            Assert.fail("Expected SqlException for unmatched bracket");
        } catch (SqlException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "unbalanced bracket"));
        }
    }

    @Test
    public void testConvertGlobPatternWithDot() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("file.name.txt", sink, 0);
        Assert.assertEquals("^file\\.name\\.txt$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternWithNumbers() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("data_2024_*.csv", sink, 0);
        Assert.assertEquals("^data_2024_.*\\.csv$", sink.toString());
    }

    @Test
    public void testConvertGlobPatternWithWhitespace() throws SqlException {
        StringSink sink = new StringSink();
        GlobStrFunctionFactory.convertGlobPatternToRegex("my file *.txt", sink, 0);
        Assert.assertEquals("^my file .*\\.txt$", sink.toString());
    }
}
