/*******************************************************************************
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

package io.questdb.test.std;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class Long256FromCharSequenceDecoderTest {
    private Long256FromCharSequenceDecoder decoder;
    private long l0;
    private long l1;
    private long l2;
    private long l3;

    @AfterClass
    public static void afterClass() {
        ColumnType.resetStringToDefault();
    }

    @BeforeClass
    public static void beforeClass() {
        ColumnType.makeUtf16DefaultString();
    }

    @Before
    public void before() {
        decoder = new Long256FromCharSequenceDecoder() {
            @Override
            public void setAll(long l0, long l1, long l2, long l3) {
                Long256FromCharSequenceDecoderTest.this.l0 = l0;
                Long256FromCharSequenceDecoderTest.this.l1 = l1;
                Long256FromCharSequenceDecoderTest.this.l2 = l2;
                Long256FromCharSequenceDecoderTest.this.l3 = l3;
            }
        };
    }

    @Test
    public void testBadEncoding() {
        try {
            assertDecoded("5g9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880L, 99607112989370L, 0, 0);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `5g9796963abad00001e5f6bbdb38` [STRING -> LONG256]");
        }
    }

    @Test
    public void testMixed() {
        assertDecoded("5a9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880L, 99607112989370L, 0, 0);
        assertDecoded("05a9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880L, 99607112989370L, 0, 0);
        assertDecoded("0x05a9796963abad00001e5f6bbdb38i", 2, 1, -3458762426621895880L, 99607112989370L, 0, 0);
        assertDecoded("5A9796963aBad00001E5f6bbdb38", 0, 0, -3458762426621895880L, 99607112989370L, 0, 0);
    }

    @Test
    public void testPartialLength() {
        assertDecoded("1", 0, 0, 0x1L, 0, 0, 0);
        assertDecoded("10", 0, 0, 0x10L, 0, 0, 0);
        assertDecoded("100", 0, 0, 0x100L, 0, 0, 0);
        assertDecoded("1000", 0, 0, 0x1000L, 0, 0, 0);
        assertDecoded("10000", 0, 0, 0x10000L, 0, 0, 0);
        assertDecoded("100000", 0, 0, 0x100000L, 0, 0, 0);
        assertDecoded("1000000", 0, 0, 0x1000000L, 0, 0, 0);
        assertDecoded("10000000", 0, 0, 0x10000000L, 0, 0, 0);
        assertDecoded("100000000", 0, 0, 0x100000000L, 0, 0, 0);
        assertDecoded("1000000000", 0, 0, 0x1000000000L, 0, 0, 0);
        assertDecoded("10000000000", 0, 0, 0x10000000000L, 0, 0, 0);
        assertDecoded("100000000000", 0, 0, 0x100000000000L, 0, 0, 0);
        assertDecoded("1000000000000", 0, 0, 0x1000000000000L, 0, 0, 0);
        assertDecoded("10000000000000", 0, 0, 0x10000000000000L, 0, 0, 0);
        assertDecoded("100000000000000", 0, 0, 0x100000000000000L, 0, 0, 0);
        assertDecoded("1000000000000000", 0, 0, 0x1000000000000000L, 0, 0, 0);
        assertDecoded("10000000000000000", 0, 0, 0, 0x1L, 0, 0);
        assertDecoded("100000000000000000", 0, 0, 0, 0x10L, 0, 0);
        assertDecoded("1000000000000000000", 0, 0, 0, 0x100L, 0, 0);
        assertDecoded("10000000000000000000", 0, 0, 0, 0x1000L, 0, 0);
        assertDecoded("100000000000000000000", 0, 0, 0, 0x10000L, 0, 0);
        assertDecoded("1000000000000000000000", 0, 0, 0, 0x100000L, 0, 0);
        assertDecoded("10000000000000000000000", 0, 0, 0, 0x1000000L, 0, 0);
        assertDecoded("100000000000000000000000", 0, 0, 0, 0x10000000L, 0, 0);
        assertDecoded("1000000000000000000000000", 0, 0, 0, 0x100000000L, 0, 0);
        assertDecoded("10000000000000000000000000", 0, 0, 0, 0x1000000000L, 0, 0);
        assertDecoded("100000000000000000000000000", 0, 0, 0, 0x10000000000L, 0, 0);
        assertDecoded("1000000000000000000000000000", 0, 0, 0, 0x100000000000L, 0, 0);
        assertDecoded("10000000000000000000000000000", 0, 0, 0, 0x1000000000000L, 0, 0);
        assertDecoded("100000000000000000000000000000", 0, 0, 0, 0x10000000000000L, 0, 0);
        assertDecoded("1000000000000000000000000000000", 0, 0, 0, 0x100000000000000L, 0, 0);
        assertDecoded("10000000000000000000000000000000", 0, 0, 0, 0x1000000000000000L, 0, 0);
        assertDecoded("100000000000000000000000000000000", 0, 0, 0, 0, 0x1L, 0);
        assertDecoded("1000000000000000000000000000000000", 0, 0, 0, 0, 0x10L, 0);
        assertDecoded("10000000000000000000000000000000000", 0, 0, 0, 0, 0x100L, 0);
        assertDecoded("100000000000000000000000000000000000", 0, 0, 0, 0, 0x1000L, 0);
        assertDecoded("1000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000L, 0);
        assertDecoded("10000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000L, 0);
        assertDecoded("100000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000L, 0);
        assertDecoded("1000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000L, 0);
        assertDecoded("10000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000L, 0);
        assertDecoded("100000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000L, 0);
        assertDecoded("1000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000000L, 0);
        assertDecoded("10000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000000L, 0);
        assertDecoded("100000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000000L, 0);
        assertDecoded("1000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000000000L, 0);
        assertDecoded("10000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000000000L, 0);
        assertDecoded("100000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000000000L, 0);
        assertDecoded("1000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1L);
        assertDecoded("10000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10L);
        assertDecoded("100000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100L);
        assertDecoded("1000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000L);
        assertDecoded("10000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000L);
        assertDecoded("100000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000L);
        assertDecoded("1000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000L);
        assertDecoded("10000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000L);
        assertDecoded("100000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000L);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000L);
        assertDecoded("10000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000000L);
        assertDecoded("100000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000000L);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000L);
        assertDecoded("10000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000000000L);
        assertDecoded("100000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000000000L);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000000L);
    }

    @Test
    public void testTooLong() {
        try {
            assertDecoded("10000000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000000L);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(
                    e.getFlyweightMessage(),
                    "inconvertible value: `10000000000000000000000000000000000000000000000000000000000000000` [STRING -> LONG256]"
            );
        }
    }

    private void assertDecoded(String hexString, int prefixSize, int suffixSize, long l0, long l1, long l2, long l3) {
        Long256FromCharSequenceDecoder.decode(hexString, prefixSize, hexString.length() - suffixSize, decoder);
        Assert.assertEquals(l0, this.l0);
        Assert.assertEquals(l1, this.l1);
        Assert.assertEquals(l2, this.l2);
        Assert.assertEquals(l3, this.l3);
    }
}
