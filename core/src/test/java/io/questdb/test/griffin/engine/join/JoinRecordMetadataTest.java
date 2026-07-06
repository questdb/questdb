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
import io.questdb.cairo.IndexType;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinRecordMetadataTest extends AbstractCairoTest {
    @Test
    public void testAliasedDottedColumnResolvesViaQualifiedRef() {
        // An aliased side's plain column whose name contains a dot (e.g. a projection named "a.b")
        // must keep the dot as content, not be split into table.column. add() re-quotes it, so it
        // resolves through the qualified, quote-protected reference m."a.b" and is not mis-split
        // into a spurious m.a lookup.
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 2);
        metadata.add("m", "a.b", ColumnType.INT, IndexType.NONE, 0, false, null);
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("m.\"a.b\""));
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("m.a"));
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("a.b"));
    }

    @Test
    public void testComposedOperatorTokenReferenceResolvesViaStrip() {
        // An operator-token column (e.g. a pivot value 'in') is stored bare on its side. A join
        // wildcard builds the composed reference t1."in" from the model alias; the qualified lookup
        // must strip the protective quotes off the column part and resolve against the bare stored
        // name (regression: this asserted "Invalid column" at compile time before the retry).
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 2);
        metadata.add("t1", "in", ColumnType.INT, IndexType.NONE, 0, false, null);
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("t1.\"in\""));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("t1.in"));
        // a genuinely absent quoted column still misses
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("t1.\"and\""));
    }

    @Test
    public void testDuplicateColumnAlias() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 3);
        metadata.add("A", "x", ColumnType.INT, IndexType.NONE, 0, false, null);
        try {
            metadata.add("a", "X", ColumnType.FLOAT, IndexType.NONE, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=X, alias=a]");
        }

        try {
            metadata.add("A", "X", ColumnType.FLOAT, IndexType.NONE, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=X, alias=A]");
        }

        Assert.assertEquals(0, metadata.getColumnIndexQuiet("x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.X"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.X"));
    }

    @Test
    public void testQuoteProtectedDottedAliasDoesNotMisbind() {
        // Regression: SqlUtil.getColumnIndexQuiet resolves a protected alias by stripping the quotes
        // and retrying via the ranged lookup, which JoinRecordMetadata splits on a dot. A whole-quoted
        // dotted alias "a.b" (no column literally named a.b exists) must NOT bind to the unrelated join
        // column a.b (side a, column b) - a real dotted join column is stored re-quoted and matched
        // verbatim first, so a miss here is correct. The operator-token form "in" carries no dot and
        // still resolves via the bare retry.
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 2);
        metadata.add("a", "b", ColumnType.INT, IndexType.NONE, 0, false, null);
        Assert.assertEquals(-1, SqlUtil.getColumnIndexQuiet(metadata, "\"a.b\""));
        // the direct composed reference a.b (side a, column b) still resolves - the guard only affects
        // the strip-retry, not JoinRecordMetadata's own lookup
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.b"));

        JoinRecordMetadata opMetadata = new JoinRecordMetadata(configuration, 1);
        opMetadata.add("t1", "in", ColumnType.INT, IndexType.NONE, 0, false, null);
        Assert.assertEquals(0, SqlUtil.getColumnIndexQuiet(opMetadata, "\"in\""));
    }

    @Test
    public void testRangedLookupHonorsBounds() {
        // The ranged getColumnIndexQuiet(cs, lo, hi) must key the table/column split off the
        // [lo, hi) slice, not the whole string (regression: the dotted branch used 0/length and
        // ignored lo/hi, so a sliced composed reference resolved against the wrong table part).
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 2);
        metadata.add("t1", "in", ColumnType.INT, IndexType.NONE, 0, false, null);
        final String composed = "xxt1.in";
        // slice "t1.in" resolves; the ignored-bounds bug would look up "xxt1.in" and miss
        Assert.assertEquals(0, metadata.getColumnIndexQuiet(composed, 2, composed.length()));
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet(composed, 0, composed.length()));
    }

    @Test
    public void testSimple() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 10);
        metadata.add("a", "x", ColumnType.INT, IndexType.NONE, 0, false, null);
        metadata.add("a", "y", ColumnType.DOUBLE, IndexType.NONE, 0, false, null);
        metadata.add("a", "m", ColumnType.DOUBLE, IndexType.NONE, 0, false, null);
        metadata.add("b", "x", ColumnType.DOUBLE, IndexType.NONE, 0, false, null);
        metadata.add("b", "y", ColumnType.BINARY, IndexType.NONE, 0, false, null);
        metadata.add("b", "z", ColumnType.FLOAT, IndexType.NONE, 0, false, null);
        try {
            metadata.add("b", "y", ColumnType.FLOAT, IndexType.NONE, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=y, alias=b]");
        }

        metadata.add(null, "c.x", ColumnType.STRING, IndexType.NONE, 0, false, null);
        metadata.add(null, "c.vch", ColumnType.VARCHAR, IndexType.NONE, 0, false, null);

        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.x"));
        Assert.assertEquals(1, metadata.getColumnIndexQuiet("a.y"));
        Assert.assertEquals(2, metadata.getColumnIndexQuiet("m"));

        Assert.assertEquals(3, metadata.getColumnIndexQuiet("b.x"));
        Assert.assertEquals(4, metadata.getColumnIndexQuiet("b.y"));
        Assert.assertEquals(5, metadata.getColumnIndexQuiet("b.z"));

        Assert.assertEquals(5, metadata.getColumnIndexQuiet("z"));

        // this one shouldn't exist
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("b.k"));

        // add ambiguity to column names without aliases
        metadata.add(null, "z.m", ColumnType.STRING, IndexType.NONE, 0, false, null);
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("m"));

        Assert.assertEquals(ColumnType.BINARY, metadata.getColumnType("b.y"));
        Assert.assertEquals(ColumnType.INT, metadata.getColumnType("a.x"));
        Assert.assertEquals(ColumnType.VARCHAR, metadata.getColumnType("c.vch"));

        String varcharType = ColumnType.nameOf(ColumnType.VARCHAR);
        String stringType = ColumnType.nameOf(ColumnType.STRING);
        String expected = "a.x:INT\n" +
                "a.y:DOUBLE\n" +
                "a.m:DOUBLE\n" +
                "b.x:DOUBLE\n" +
                "b.y:BINARY\n" +
                "b.z:FLOAT\n" +
                "c.x:" + stringType + "\n" +
                "c.vch:" + varcharType + "\n" +
                "z.m:" + stringType + "\n";

        StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            sink.put(metadata.getColumnName(i));
            sink.put(':');
            sink.put(ColumnType.nameOf(metadata.getColumnType(i)));
            sink.put('\n');
        }

        TestUtils.assertEquals(expected, sink);
    }
}
