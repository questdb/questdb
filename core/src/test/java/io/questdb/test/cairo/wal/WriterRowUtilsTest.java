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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.std.Decimal256;
import io.questdb.test.griffin.RowAsserter;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WriterRowUtilsTest {

    @Test
    public void testPutDecimalCharRejectsNonDecimalType() {
        assertPutDecimalCharFails(ColumnType.INT, "cannot store decimal into column type: INT");
        assertPutDecimalCharFails(ColumnType.DOUBLE, "cannot store decimal into column type: DOUBLE");
    }

    @Test
    public void testPutDecimalCharRejectsSurrogateDecimalType() {
        assertPutDecimalCharFails(ColumnType.DECIMAL, "cannot store decimal into column type: DECIMAL");
    }

    @Test
    public void testPutDecimalQuickRejectsNonDecimalTag() {
        assertPutDecimalQuickFails(ColumnType.INT, "cannot store decimal into column type: INT");
        assertPutDecimalQuickFails(ColumnType.DOUBLE, "cannot store decimal into column type: DOUBLE");
    }

    @Test
    public void testPutDecimalQuickRejectsSurrogateDecimalTag() {
        assertPutDecimalQuickFails(ColumnType.DECIMAL, "cannot store decimal into column type: DECIMAL");
    }

    @Test
    public void testPutNullDecimalNamesParameterisedType() {
        // the tag alone renders as "unknown" for a geohash, so the full type has to reach the message
        try {
            WriterRowUtils.putNullDecimal(new RowAsserter(), 0, ColumnType.getGeoHashTypeWithBits(20));
            Assert.fail("expected put to be rejected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cannot store decimal into column type: GEOHASH(4c)");
        }
    }

    @Test
    public void testPutNullDecimalRejectsNonDecimalTag() {
        assertPutNullDecimalFails(ColumnType.INT, "cannot store decimal into column type: INT");
        assertPutNullDecimalFails(ColumnType.DOUBLE, "cannot store decimal into column type: DOUBLE");
    }

    @Test
    public void testPutNullDecimalRejectsSurrogateDecimalTag() {
        assertPutNullDecimalFails(ColumnType.DECIMAL, "cannot store decimal into column type: DECIMAL");
    }

    private static void assertPutDecimalCharFails(int columnType, CharSequence message) {
        try {
            // a non-decimal type reports precision 0, which only a zero digit gets past
            WriterRowUtils.putDecimalChar(0, new Decimal256(), '0', columnType, new RowAsserter());
            Assert.fail("expected put to be rejected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertPutDecimalQuickFails(int columnType, CharSequence message) {
        final Decimal256 value = new Decimal256();
        value.ofLong(42, 0);
        try {
            // RowAsserter fails on any put, so a silent write into the wrong width is caught too
            WriterRowUtils.putDecimalQuick(0, value, ColumnType.tagOf(columnType), new RowAsserter());
            Assert.fail("expected put to be rejected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertPutNullDecimalFails(int columnType, CharSequence message) {
        try {
            WriterRowUtils.putNullDecimal(new RowAsserter(), 0, ColumnType.tagOf(columnType));
            Assert.fail("expected put to be rejected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }
}
