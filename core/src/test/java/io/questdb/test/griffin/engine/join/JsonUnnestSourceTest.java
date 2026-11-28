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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.join.JsonUnnestSource;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * JsonUnnestSource.init() returns no element for a document that opens with an empty array, '[' and ']' with only
 * JSON whitespace before and between them, without a native parse: a negative LIMIT ahead of UNNEST reads back
 * through every empty array, see UnnestRecordCursorFactory.getFanOut(). Every other document goes to simdjson.
 */
public class JsonUnnestSourceTest extends AbstractCairoTest {

    @Test
    public void testDocumentsThatDoNotOpenWithEmptyArrayGoToSimdjson() throws Exception {
        assertMemoryLeak(() -> {
            final DocumentFunction document = new DocumentFunction();
            try (JsonUnnestSource source = newSource(document)) {
                long parseCount = 0;
                // JSON whitespace around the elements
                Assert.assertEquals(2, init(source, document, "[1, 2]"));
                Assert.assertEquals(++parseCount, source.getNativeParseCount());
                Assert.assertEquals(2, source.getInt(0, 1));
                Assert.assertEquals(1, init(source, document, " \t\n\r[ \t\n\r3 \t\n\r] \t\n\r"));
                Assert.assertEquals(++parseCount, source.getNativeParseCount());
                Assert.assertEquals(3, source.getInt(0, 0));
                // simdjson skips a UTF-8 BOM, the check does not
                Assert.assertEquals(3, init(source, document, "\uFEFF[4, 5, 6]"));
                Assert.assertEquals(++parseCount, source.getNativeParseCount());
                Assert.assertEquals(6, source.getInt(0, 2));
                Assert.assertEquals(0, init(source, document, "\uFEFF[]"));
                Assert.assertEquals(++parseCount, source.getNativeParseCount());
                // JSON counts no other character as whitespace
                final String[] notWhitespace = {"\u000b", "\f", "\u00a0", "\u0000", "\u2028", "x"};
                for (String c : notWhitespace) {
                    init(source, document, "[" + c + "]");
                    Assert.assertEquals(++parseCount, source.getNativeParseCount());
                    init(source, document, c + "[]");
                    Assert.assertEquals(++parseCount, source.getNativeParseCount());
                }
                // documents that unnest into no element but open with no empty array
                final String[] noElement = {"[", "[ ", "]", "   ", "{}", "null", "1", "\"[]\"", "not json"};
                for (String doc : noElement) {
                    Assert.assertEquals(doc, 0, init(source, document, doc));
                    Assert.assertEquals(doc, ++parseCount, source.getNativeParseCount());
                }
                // more elements than the results buffer holds parse twice, the second time into a larger buffer
                final StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < 40; i++) {
                    sb.append(i == 0 ? "" : ",").append(i);
                }
                Assert.assertEquals(40, init(source, document, sb.append(']').toString()));
                parseCount += 2;
                Assert.assertEquals(parseCount, source.getNativeParseCount());
                Assert.assertEquals(39, source.getInt(0, 39));
            }
        });
    }

    @Test
    public void testDocumentsThatOpenWithEmptyArrayNeedNoParse() throws Exception {
        assertMemoryLeak(() -> {
            final DocumentFunction document = new DocumentFunction();
            try (JsonUnnestSource source = newSource(document)) {
                // simdjson counts no element in an empty array, whatever follows it, or rejects the document
                final String[] emptyArrays = {
                        "[]",
                        "[ ]",
                        " \t\n\r[ \t\n\r] \t\n\r",
                        "[" + " ".repeat(1_000) + "]",
                        "[]x",
                        "[] [1, 2]",
                        "[][1]",
                        "[]]",
                        "[]]]]",
                        "[],1]",
                        "[] \"open string",
                        "[]\uD83D\uDE00",
                        "[]" + "[".repeat(10_000),
                        "[]" + "1".repeat(10_000)
                };
                for (String doc : emptyArrays) {
                    // a document with elements first, so that the empty array has to reset the source
                    Assert.assertEquals(1, init(source, document, "[7]"));
                    Assert.assertEquals(7, source.getInt(0, 0));
                    final long parseCount = source.getNativeParseCount();
                    Assert.assertEquals(doc, 0, init(source, document, doc));
                    Assert.assertEquals(doc, parseCount, source.getNativeParseCount());
                    Assert.assertEquals(Numbers.INT_NULL, source.getInt(0, 0));
                }
                // NULL and an empty document take no parse either
                final long parseCount = source.getNativeParseCount();
                document.value = null;
                Assert.assertEquals(0, source.init(null));
                Assert.assertEquals(0, init(source, document, ""));
                Assert.assertEquals(parseCount, source.getNativeParseCount());
            }
        });
    }

    private static int init(JsonUnnestSource source, DocumentFunction document, String doc) {
        document.value = new Utf8String(doc);
        return source.init(null);
    }

    private static JsonUnnestSource newSource(DocumentFunction document) {
        final ObjList<CharSequence> columnNames = new ObjList<>();
        columnNames.add("v");
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.INT);
        return new JsonUnnestSource(
                document,
                columnNames,
                columnTypes,
                JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
        );
    }

    // the VARCHAR expression of UNNEST, which returns the document of the test for every row
    private static class DocumentFunction extends VarcharFunction {
        private Utf8Sequence value;

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return value;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return value;
        }
    }
}
