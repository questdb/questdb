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

package io.questdb.test.cutlass.text;

import io.questdb.cutlass.text.AbstractTextLexer;
import io.questdb.cutlass.text.CsvTextLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CsvTextLexerTest {
    private final StringSink sink = new StringSink();
    private final AbstractTextLexer.Listener listener = this::onFields;

    @Test
    public void testParseAfterClearRollsFirstLineFromBufferStart() throws Exception {
        // the previous import's last line started at offset 4, the first line after clear() starts at offset 0
        TestUtils.assertMemoryLeak(() -> {
            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                lexer.setupLimits(Integer.MAX_VALUE, listener);
                feed("a,1\nbb,2\n", lexer::parse);
                sink.clear();
                lexer.clear();
                lexer.setupLimits(Integer.MAX_VALUE, listener);
                feed("2024-01-01,20", lexer::parse);
                feed("9\n", lexer::parse);
                TestUtils.assertEquals("2024-01-01|209\n", sink);
                Assert.assertEquals(0, lexer.getErrorCount());
            }
        });
    }

    @Test
    public void testParseWholeLinesAfterClearEndsUnterminatedLine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                lexer.setupBeforeExactLines(listener, 2);
                feed("a,1\nbb,2\n", lexer::parseWholeLines);
                sink.clear();
                lexer.clear();
                lexer.setupBeforeExactLines(listener, 2);
                feed("2024-01-01,209", lexer::parseWholeLines);
                TestUtils.assertEquals("2024-01-01|209\n", sink);
                Assert.assertEquals(1, lexer.getLineCount());
                Assert.assertEquals(0, lexer.getErrorCount());
            }
        });
    }

    @Test
    public void testParseWholeLinesDropsUnterminatedLineWithMissingQuote() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                lexer.setupBeforeExactLines(listener, 2);
                feed("\"x,209", lexer::parseWholeLines);
                feed("z,211\n", lexer::parseWholeLines);
                TestUtils.assertEquals("z|211\n", sink);
                Assert.assertEquals(1, lexer.getErrorCount());
            }
        });
    }

    @Test
    public void testParseWholeLinesEndsUnterminatedLongLine() throws Exception {
        // the line is longer than the roll buffer limit, parseWholeLines() never rolls it
        TestUtils.assertMemoryLeak(() -> {
            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                lexer.setupBeforeExactLines(listener, 2);
                final String longValue = "x".repeat(20_000);
                feed(longValue + ",209", lexer::parseWholeLines);
                feed("y,210\n", lexer::parseWholeLines);
                TestUtils.assertEquals(longValue + "|209\ny|210\n", sink);
                Assert.assertEquals(0, lexer.getErrorCount());
            }
        });
    }

    @Test
    public void testParseWholeLinesEndsUnterminatedQuotedLine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                lexer.setupBeforeExactLines(listener, 2);
                feed("\"x\",\"209\"", lexer::parseWholeLines);
                feed("\"y\",\"210\"\n", lexer::parseWholeLines);
                TestUtils.assertEquals("x|209\ny|210\n", sink);
                Assert.assertEquals(0, lexer.getErrorCount());
            }
        });
    }

    private static void feed(String text, BufferParser parser) {
        final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        final long size = bytes.length;
        final long lo = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(lo + i, bytes[i]);
            }
            parser.parse(lo, lo + size);
            // a field that still points at this buffer after the parse shows '@'
            Vect.memset(lo, size, '@');
        } finally {
            Unsafe.free(lo, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void onFields(long line, ObjList<DirectUtf8String> fields, int hi) {
        for (int i = 0; i < hi; i++) {
            if (i > 0) {
                sink.put('|');
            }
            sink.put(fields.getQuick(i));
        }
        sink.put('\n');
    }

    @FunctionalInterface
    private interface BufferParser {
        void parse(long lo, long hi);
    }
}
