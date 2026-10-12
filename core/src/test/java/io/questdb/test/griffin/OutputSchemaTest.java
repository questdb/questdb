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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

public class OutputSchemaTest {
    private static final String[] QUALIFIERS = {null, "a", "B", "zz"};

    @Test
    public void testAddColumnFromKeepsEveryAttribute() {
        final OutputSchema nested = new OutputSchema();
        final OutputSchema source = new OutputSchema();
        source.add(7, "a", ColumnType.SYMBOL, nested, false, "t");
        source.setSymbolTableStatic(0, true);
        source.protectName(0);
        source.add(8, "ts", ColumnType.TIMESTAMP, true);
        source.setTimestampIndex(1);
        final OutputSchema target = new OutputSchema();
        target.add(1, "x", ColumnType.INT, true);
        target.addColumnFrom(source, 0);
        Assert.assertEquals(2, target.getColumnCount());
        Assert.assertEquals(7, target.getColumnId(1));
        Assert.assertEquals("a", target.getColumnName(1));
        Assert.assertEquals(ColumnType.SYMBOL, target.getColumnType(1));
        Assert.assertSame(nested, target.getMetadata(1));
        Assert.assertEquals("t", target.getColumnQualifier(1));
        Assert.assertFalse(target.isVisible(1));
        Assert.assertTrue(target.isSymbolTableStatic(1));
        Assert.assertTrue(target.isNameProtected(1));
        Assert.assertEquals(-1, target.getTimestampIndex());
        target.addColumnsFrom(source, "q");
        Assert.assertEquals(4, target.getColumnCount());
        Assert.assertEquals("q", target.getColumnQualifier(2));
        Assert.assertEquals("q", target.getColumnQualifier(3));
        // a join scope names its inputs' columns bare: the composed reference resolves
        Assert.assertFalse(target.isNameProtected(2));
        Assert.assertTrue(target.isSymbolTableStatic(2));
        Assert.assertTrue(target.isVisible(3));
        Assert.assertEquals(-1, target.getTimestampIndex());
        target.addColumnsFrom(source, null);
        Assert.assertEquals(6, target.getColumnCount());
        Assert.assertEquals("t", target.getColumnQualifier(4));
        Assert.assertFalse(target.isNameProtected(4));
        target.addColumnAs(source, 0, 9, "b", true, "r");
        Assert.assertEquals(9, target.getColumnId(6));
        Assert.assertEquals("b", target.getColumnName(6));
        Assert.assertEquals("r", target.getColumnQualifier(6));
        Assert.assertTrue(target.isVisible(6));
        Assert.assertTrue(target.isSymbolTableStatic(6));
        Assert.assertTrue(target.isNameProtected(6));
        Assert.assertSame(nested, target.getMetadata(6));
    }

    @Test
    public void testLookupAfterMutationsNarrow() {
        assertLookupAfterMutations(4);
    }

    @Test
    public void testLookupAfterMutationsWide() {
        assertLookupAfterMutations(40);
    }

    @Test
    public void testLookupSemanticsNarrow() {
        assertLookupSemantics(0);
    }

    @Test
    public void testLookupSemanticsWide() {
        assertLookupSemantics(40);
    }

    @Test
    public void testRetainReordersAndRelocatesTimestamp() {
        final OutputSchema schema = new OutputSchema();
        schema.add(1, "a", ColumnType.INT, true);
        schema.add(2, "ts", ColumnType.TIMESTAMP, true);
        schema.add(3, "b", ColumnType.LONG, null, false, "t");
        schema.protectName(2);
        schema.add(4, "c", ColumnType.DOUBLE, true);
        schema.setTimestampIndex(1);
        final IntList indexes = new IntList();
        indexes.add(2);
        indexes.add(1);
        schema.retain(indexes);
        Assert.assertEquals(2, schema.getColumnCount());
        Assert.assertEquals(3, schema.getColumnId(0));
        Assert.assertEquals("b", schema.getColumnName(0));
        Assert.assertEquals("t", schema.getColumnQualifier(0));
        Assert.assertFalse(schema.isVisible(0));
        Assert.assertTrue(schema.isNameProtected(0));
        Assert.assertEquals(2, schema.getColumnId(1));
        Assert.assertEquals(1, schema.getTimestampIndex());
        indexes.clear();
        indexes.add(0);
        schema.retain(indexes);
        Assert.assertEquals(1, schema.getColumnCount());
        Assert.assertEquals(3, schema.getColumnId(0));
        Assert.assertEquals(-1, schema.getTimestampIndex());
        indexes.clear();
        schema.retain(indexes);
        Assert.assertEquals(0, schema.getColumnCount());
        Assert.assertEquals(-1, schema.getTimestampIndex());
    }

    @Test
    public void testSetTimestampColumnId() {
        final OutputSchema schema = new OutputSchema();
        schema.add(1, "a", ColumnType.INT, true);
        schema.add(2, "ts", ColumnType.TIMESTAMP, true);
        schema.setTimestampColumnId(2);
        Assert.assertEquals(1, schema.getTimestampIndex());
        schema.setTimestampColumnId(9);
        Assert.assertEquals(-1, schema.getTimestampIndex());
        schema.setTimestampColumnId(-1);
        Assert.assertEquals(-1, schema.getTimestampIndex());
    }

    private static void add(OutputSchema schema, int id, String name, String qualifier) {
        schema.add(id, name, ColumnType.INT, null, true, qualifier);
    }

    private static void addColumns(OutputSchema schema, int from, int to) {
        for (int i = from; i < to; i++) {
            final String name = i % 5 == 0 ? (i % 2 == 0 ? "dup" : "DUP") : "c" + i;
            schema.add(i, name, ColumnType.INT, null, i % 7 != 3, i % 3 == 0 ? null : (i % 3 == 1 ? "a" : "b"));
            if (i % 11 == 4) {
                schema.protectName(i);
            }
        }
    }

    private static void assertLookupAfterMutations(int width) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < width; i++) {
            addColumns(schema, i, i + 1);
            assertMatchesScan(schema);
        }
        addColumns(schema, width, width + 3);
        assertMatchesScan(schema);

        schema.setColumnName(1, "dup", "B");
        assertMatchesScan(schema);
        schema.setColumnName(width + 2, "renamed", null);
        assertMatchesScan(schema);

        schema.protectName(0);
        assertMatchesScan(schema);

        schema.remove(0);
        assertMatchesScan(schema);
        schema.remove(schema.getColumnCount() - 1);
        assertMatchesScan(schema);

        final IntList retained = new IntList();
        for (int i = schema.getColumnCount() - 1; i >= 0; i -= 2) {
            retained.add(i);
        }
        schema.retain(retained);
        assertMatchesScan(schema);

        final OutputSchema source = new OutputSchema();
        addColumns(source, 0, width);
        schema.addColumnsFrom(source, "zz");
        assertMatchesScan(schema);
        schema.addColumnAs(source, 1, 1_000, "C1", true, "a");
        assertMatchesScan(schema);

        final OutputSchema copy = new OutputSchema();
        addColumns(copy, 0, width + 20);
        assertMatchesScan(copy);
        copy.copyFrom(schema);
        assertMatchesScan(copy);

        schema.clear();
        assertMatchesScan(schema);
        addColumns(schema, 0, width);
        schema.setColumnName(width / 2, "late", "a");
        assertMatchesScan(schema);
    }

    private static void assertLookupSemantics(int padding) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < padding; i++) {
            add(schema, i, "p" + i, null);
        }
        schema.add(100, "hidden", ColumnType.INT, null, false, "a");
        add(schema, 101, "Price", "a");
        add(schema, 102, "PRICE", "b");
        add(schema, 103, "hidden", "b");
        add(schema, 104, "guarded", "a");
        schema.protectName(schema.getColumnCount() - 1);
        add(schema, 105, "Qty", null);

        Assert.assertEquals(padding + 1, schema.getColumnIndexQuiet("price"));
        Assert.assertEquals(padding + 3, schema.getColumnIndexQuiet("HIDDEN"));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("guarded"));
        Assert.assertEquals(padding + 5, schema.getColumnIndexQuiet("\"qty\"", 1, 4));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("qty", 0, 2));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("missing"));

        Assert.assertEquals(OutputSchema.COLUMN_AMBIGUOUS, schema.getColumnIndexQuiet(null, "price", 0, 5));
        Assert.assertEquals(padding + 1, schema.getColumnIndexQuiet("A", "price", 0, 5));
        Assert.assertEquals(padding + 2, schema.getColumnIndexQuiet("b", "x.price", 2, 7));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("zz", "price", 0, 5));
        Assert.assertEquals(padding + 3, schema.getColumnIndexQuiet(null, "hidden", 0, 6));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("a", "hidden", 0, 6));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet(null, "guarded", 0, 7));
    }

    private static void assertMatchesScan(OutputSchema schema) {
        final int count = schema.getColumnCount();
        for (int i = -1; i < count; i++) {
            final String name = i < 0 ? "missing" : schema.getColumnName(i).toString();
            assertProbe(schema, name, 0, name.length());
            final String upper = name.toUpperCase();
            assertProbe(schema, upper, 0, upper.length());
            final String quoted = "\"" + name + "\"";
            assertProbe(schema, quoted, 1, quoted.length() - 1);
            assertProbe(schema, quoted, 0, quoted.length() - 1);
        }
    }

    private static void assertProbe(OutputSchema schema, CharSequence name, int lo, int hi) {
        Assert.assertEquals(scan(schema, name, lo, hi), schema.getColumnIndexQuiet(name, lo, hi));
        for (String qualifier : QUALIFIERS) {
            Assert.assertEquals(scan(schema, qualifier, name, lo, hi), schema.getColumnIndexQuiet(qualifier, name, lo, hi));
        }
    }

    private static boolean isMatch(OutputSchema schema, int index, CharSequence name, int lo, int hi) {
        final CharSequence column = schema.getColumnName(index);
        return schema.isVisible(index) && !schema.isNameProtected(index)
                && column.length() == hi - lo && Chars.equalsIgnoreCase(column, name, lo, hi);
    }

    private static int scan(OutputSchema schema, CharSequence name, int lo, int hi) {
        for (int i = 0, n = schema.getColumnCount(); i < n; i++) {
            if (isMatch(schema, i, name, lo, hi)) {
                return i;
            }
        }
        return -1;
    }

    private static int scan(OutputSchema schema, CharSequence qualifier, CharSequence name, int lo, int hi) {
        int index = -1;
        for (int i = 0, n = schema.getColumnCount(); i < n; i++) {
            if (isMatch(schema, i, name, lo, hi)
                    && (qualifier == null || Chars.equalsIgnoreCaseNc(qualifier, schema.getColumnQualifier(i)))) {
                if (index != -1) {
                    return OutputSchema.COLUMN_AMBIGUOUS;
                }
                index = i;
            }
        }
        return index;
    }
}
