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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnionSymbolCastSparseKeyTest extends AbstractUnionSymbolCastTest {
    // A static source dictionary whose keys straddle the translation cache's dense/map boundary.
    // getSymbolCount() bounds the direct-indexed region, so keys below it translate through the
    // array and keys above it through the hash map - one source column, both code paths.
    private static final int MIXED_SYMBOL_COUNT = 3;
    private static final StaticSymbolTable MIXED_TABLE = new StaticSymbolTable() {
        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return MIXED_SYMBOL_COUNT;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return VALUE_NOT_FOUND;
            }
            if ("dense0".contentEquals(value)) {
                return 0;
            }
            if ("dense2".contentEquals(value)) {
                return 2;
            }
            if ("beyond500".contentEquals(value)) {
                return 500;
            }
            return "beyond501".contentEquals(value) ? 501 : VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return switch (key) {
                case 0 -> "dense0";
                case 2 -> "dense2";
                case 500 -> "beyond500";
                case 501 -> "beyond501";
                default -> null;
            };
        }
    };
    // A static source dictionary whose only key is far larger than its cardinality. It proves the
    // per-source translation cache is sized by cardinality (keys actually seen), not by key range,
    // so a direct-indexed array over the raw source key is not a valid substitute here.
    private static final int SPARSE_SOURCE_KEY = 100_000_000;
    private static final StaticSymbolTable SPARSE_TABLE = new StaticSymbolTable() {
        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return 1;
        }

        @Override
        public int keyOf(CharSequence value) {
            return value != null && "sparse".contentEquals(value) ? SPARSE_SOURCE_KEY : VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key == SPARSE_SOURCE_KEY ? "sparse" : null;
        }
    };

    // Source keys either side of the dense/map boundary must translate to the same merged key for
    // the same text, and to distinct keys for distinct text. A boundary that routed a key to the
    // wrong structure would return the "not translated yet" sentinel and re-intern it, handing the
    // same source key two different merged keys on two passes - which the repeated rows catch.
    @Test
    public void testKeysAcrossDenseAndMapRegionsTranslateCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            final String[] texts = {"dense0", "beyond500", "dense2", "beyond501", "dense0", "beyond500"};
            final String[][] rows = new String[texts.length][];
            for (int i = 0; i < texts.length; i++) {
                rows[i] = new String[]{texts[i]};
            }
            try (RecordCursorFactory factory = newFactory(new StaticSymbolCursorFactory(MIXED_TABLE, rows))) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int[] keys = new int[texts.length];
                    for (int i = 0; i < texts.length; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        keys[i] = record.getInt(0);
                        TestUtils.assertEquals(texts[i], symbolTable.valueOf(keys[i]));
                    }
                    Assert.assertFalse(cursor.hasNext());
                    // Repeats collapse onto the cached key in both regions: dense (0) and map (500).
                    Assert.assertEquals(keys[0], keys[4]);
                    Assert.assertEquals(keys[1], keys[5]);
                    // Distinct source text never shares a merged key.
                    Assert.assertNotEquals(keys[0], keys[1]);
                    Assert.assertNotEquals(keys[0], keys[2]);
                    Assert.assertNotEquals(keys[1], keys[3]);
                }
            }
        });
    }

    @Test
    public void testSparseSourceKeyUsesCardinalitySizedTrackedCache() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            try (RecordCursorFactory factory = newFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int resultKey = record.getInt(0);
                    Assert.assertEquals(0, resultKey);
                    TestUtils.assertEquals("sparse", symbolTable.valueOf(resultKey));
                    Assert.assertTrue(tracker.getUsed() > 0);
                    Assert.assertTrue(tracker.getUsed() < 1024 * 1024L);

                    final long used = tracker.getUsed();
                    Assert.assertEquals(resultKey, record.getInt(0));
                    Assert.assertEquals("the cached translation must not allocate", used, tracker.getUsed());
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals("cursor close must release the tracked cache", 0, tracker.getUsed());
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testStringAccessDoesNotInitializeSourceSymbolState() throws Exception {
        assertMemoryLeak(() -> {
            final StaticSymbolCursorFactory base = newSourceFactory();
            try (RecordCursorFactory factory = newFactory(base)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    TestUtils.assertEquals("sparse", cursor.getRecord().getSymA(0));
                    Assert.assertEquals(0, base.getSymbolTableLookupCount());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    private static RecordCursorFactory newFactory() {
        return newFactory(newSourceFactory());
    }

    private static RecordCursorFactory newFactory(StaticSymbolCursorFactory base) {
        final ObjList<Function> functions = functions(new CastStrToSymbolFunctionFactory.Func(new StrColumn(0)));
        return newSymbolProjection(base, functions);
    }

    private static StaticSymbolCursorFactory newSourceFactory() {
        return new StaticSymbolCursorFactory(SPARSE_TABLE, new String[][]{{"sparse"}});
    }
}
