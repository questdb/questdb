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


package io.questdb.test.cairo.types;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FR-031 (F39): code that keys on the physical descriptor must not decide NULL. Java cannot stop a
 * class from naming a sentinel, so this test scans the source of the record-access and page-frame
 * classes whose sites key on the descriptor and fails when a class names more NULL tokens than
 * its recorded baseline. Every token in the baseline is a NULL decision that already sits in
 * physical code; a change may move one out, never add one.
 * <p>
 * The first 19 classes are R8's physical layers, 71 tokens at {@code s10-done}; S14a added the two
 * {@code getNullCount} names of the column-vector descriptor (a PA-3 field, not a decision). The
 * rest are the codecs S14b keys on the descriptor, counted at {@code s10-done}.
 */
public class PhysicalLayerNullScanTest {
    // class -> NULL tokens allowed
    private static final Map<String, Integer> BASELINE = new LinkedHashMap<>();
    // getNullPolicy() and getColumnNullPolicy() are not tokens: reading the NULL policy once at
    // setup, switched on exhaustively, is how physical code takes NULL into account (FR-010)
    private static final Pattern NULL_TOKEN = Pattern.compile(
            "\\b[A-Z0-9_]*_NULL\\b|\\bisNull\\(|NullMemory|\\bgetNull(?!Policy\\()\\w*\\(|\\bNaN\\b|isNaN\\(|setNull\\("
    );
    private static final String ROOT = "src/main/java/io/questdb/";

    @Test
    public void testPhysicalLayerNamesNoNewNullToken() throws IOException {
        final StringBuilder report = new StringBuilder();
        boolean isOver = false;
        for (Map.Entry<String, Integer> e : BASELINE.entrySet()) {
            final int n = countNullTokens(e.getKey());
            if (n > e.getValue()) {
                isOver = true;
                report.append(e.getKey()).append(": ").append(n).append(" NULL tokens, baseline ").append(e.getValue()).append('\n');
            }
        }
        Assert.assertFalse("physical-layer classes name new NULL tokens:\n" + report, isOver);
    }

    private static int countNullTokens(String file) throws IOException {
        Path path = Paths.get(ROOT + file);
        if (!Files.exists(path)) {
            path = Paths.get("core/" + ROOT + file);
        }
        final String source = Files.readString(path)
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//[^\\n]*", "");
        final Matcher matcher = NULL_TOKEN.matcher(source);
        int n = 0;
        while (matcher.find()) {
            n++;
        }
        return n;
    }

    static {
        // R8's physical layers (E4): 71 tokens at s10-done
        BASELINE.put("cairo/RecordSinkFactory.java", 0);
        BASELINE.put("cairo/LoopingRecordSink.java", 0);
        BASELINE.put("cairo/map/OrderedMap.java", 0);
        BASELINE.put("cairo/map/OrderedMapFixedSizeRecord.java", 0);
        BASELINE.put("cairo/map/OrderedMapVarSizeRecord.java", 0);
        BASELINE.put("cairo/map/Unordered4Map.java", 0);
        BASELINE.put("cairo/map/Unordered8Map.java", 0);
        // UUID and LONG128 NULL order, decimal NULLs
        BASELINE.put("griffin/engine/orderby/SortKeyEncoder.java", 11);
        BASELINE.put("griffin/engine/orderby/RecordComparatorCompiler.java", 0);
        BASELINE.put("griffin/engine/orderby/SortKeyMaterializingRecordCursor.java", 0);
        BASELINE.put("griffin/RecordToRowCopierUtils.java", 8);
        BASELINE.put("cairo/idx/CoveringCompressor.java", 6);
        BASELINE.put("cairo/sql/CoveredColumnDecoder.java", 2);
        BASELINE.put("griffin/engine/groupby/GroupByColumnSink.java", 0);
        BASELINE.put("griffin/engine/table/AsyncFilterAtom.java", 0);
        BASELINE.put("griffin/engine/table/AsyncFilterUtils.java", 1);
        // per-getter column-top NULLs (F42)
        BASELINE.put("cairo/sql/PageFrameMemoryRecord.java", 41);
        BASELINE.put("cairo/sql/PageFrameMemoryPool.java", 2);
        // S14a: the NULL count of the column-vector descriptor (PA-3), a name
        BASELINE.put("cairo/sql/PageFrameAddressCache.java", 2);
        BASELINE.put("cairo/sql/ColumnVectorDescriptor.java", 2);
        // the codecs S14b keys on the descriptor
        BASELINE.put("cairo/RecordChain.java", 0);
        BASELINE.put("cairo/map/RecordValueSinkFactory.java", 0);
        BASELINE.put("cairo/wal/WalEventWriter.java", 3);
        BASELINE.put("griffin/UpdateOperatorImpl.java", 1);
        BASELINE.put("griffin/LoopingRecordToRowCopier.java", 0);
        BASELINE.put("cairo/lv/LiveViewSnapshotKeyCodec.java", 0);
        BASELINE.put("cairo/lv/LiveViewInMemoryBuffer.java", 6);
        BASELINE.put("cairo/lv/LiveViewWindow.java", 6);
    }
}
