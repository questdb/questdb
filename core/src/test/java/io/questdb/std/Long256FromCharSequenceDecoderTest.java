package io.questdb.std;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class Long256FromCharSequenceDecoderTest {
    private Long256FromCharSequenceDecoder decoder;
    private long l0;
    private long l1;
    private long l2;
    private long l3;

    @Test
    public void testMixed() throws NumericException {
        assertDecoded("5a9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880l, 99607112989370l, 0, 0);
        assertDecoded("05a9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880l, 99607112989370l, 0, 0);
        assertDecoded("0x05a9796963abad00001e5f6bbdb38i", 2, 1, -3458762426621895880l, 99607112989370l, 0, 0);
        assertDecoded("5A9796963aBad00001E5f6bbdb38", 0, 0, -3458762426621895880l, 99607112989370l, 0, 0);
    }

    @Test
    public void testPartialLength() throws NumericException {
        assertDecoded("1", 0, 0, 0x1l, 0, 0, 0);
        assertDecoded("10", 0, 0, 0x10l, 0, 0, 0);
        assertDecoded("100", 0, 0, 0x100l, 0, 0, 0);
        assertDecoded("1000", 0, 0, 0x1000l, 0, 0, 0);
        assertDecoded("10000", 0, 0, 0x10000l, 0, 0, 0);
        assertDecoded("100000", 0, 0, 0x100000l, 0, 0, 0);
        assertDecoded("1000000", 0, 0, 0x1000000l, 0, 0, 0);
        assertDecoded("10000000", 0, 0, 0x10000000l, 0, 0, 0);
        assertDecoded("100000000", 0, 0, 0x100000000l, 0, 0, 0);
        assertDecoded("1000000000", 0, 0, 0x1000000000l, 0, 0, 0);
        assertDecoded("10000000000", 0, 0, 0x10000000000l, 0, 0, 0);
        assertDecoded("100000000000", 0, 0, 0x100000000000l, 0, 0, 0);
        assertDecoded("1000000000000", 0, 0, 0x1000000000000l, 0, 0, 0);
        assertDecoded("10000000000000", 0, 0, 0x10000000000000l, 0, 0, 0);
        assertDecoded("100000000000000", 0, 0, 0x100000000000000l, 0, 0, 0);
        assertDecoded("1000000000000000", 0, 0, 0x1000000000000000l, 0, 0, 0);
        assertDecoded("10000000000000000", 0, 0, 0, 0x1l, 0, 0);
        assertDecoded("100000000000000000", 0, 0, 0, 0x10l, 0, 0);
        assertDecoded("1000000000000000000", 0, 0, 0, 0x100l, 0, 0);
        assertDecoded("10000000000000000000", 0, 0, 0, 0x1000l, 0, 0);
        assertDecoded("100000000000000000000", 0, 0, 0, 0x10000l, 0, 0);
        assertDecoded("1000000000000000000000", 0, 0, 0, 0x100000l, 0, 0);
        assertDecoded("10000000000000000000000", 0, 0, 0, 0x1000000l, 0, 0);
        assertDecoded("100000000000000000000000", 0, 0, 0, 0x10000000l, 0, 0);
        assertDecoded("1000000000000000000000000", 0, 0, 0, 0x100000000l, 0, 0);
        assertDecoded("10000000000000000000000000", 0, 0, 0, 0x1000000000l, 0, 0);
        assertDecoded("100000000000000000000000000", 0, 0, 0, 0x10000000000l, 0, 0);
        assertDecoded("1000000000000000000000000000", 0, 0, 0, 0x100000000000l, 0, 0);
        assertDecoded("10000000000000000000000000000", 0, 0, 0, 0x1000000000000l, 0, 0);
        assertDecoded("100000000000000000000000000000", 0, 0, 0, 0x10000000000000l, 0, 0);
        assertDecoded("1000000000000000000000000000000", 0, 0, 0, 0x100000000000000l, 0, 0);
        assertDecoded("10000000000000000000000000000000", 0, 0, 0, 0x1000000000000000l, 0, 0);
        assertDecoded("100000000000000000000000000000000", 0, 0, 0, 0, 0x1l, 0);
        assertDecoded("1000000000000000000000000000000000", 0, 0, 0, 0, 0x10l, 0);
        assertDecoded("10000000000000000000000000000000000", 0, 0, 0, 0, 0x100l, 0);
        assertDecoded("100000000000000000000000000000000000", 0, 0, 0, 0, 0x1000l, 0);
        assertDecoded("1000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000l, 0);
        assertDecoded("10000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000l, 0);
        assertDecoded("100000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000l, 0);
        assertDecoded("1000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000l, 0);
        assertDecoded("10000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000l, 0);
        assertDecoded("100000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000l, 0);
        assertDecoded("1000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000000l, 0);
        assertDecoded("10000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000000l, 0);
        assertDecoded("100000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000000l, 0);
        assertDecoded("1000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x10000000000000l, 0);
        assertDecoded("10000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x100000000000000l, 0);
        assertDecoded("100000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0x1000000000000000l, 0);
        assertDecoded("1000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1l);
        assertDecoded("10000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10l);
        assertDecoded("100000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100l);
        assertDecoded("1000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000l);
        assertDecoded("10000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000l);
        assertDecoded("100000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000l);
        assertDecoded("1000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000l);
        assertDecoded("10000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000l);
        assertDecoded("100000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000l);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000l);
        assertDecoded("10000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000000l);
        assertDecoded("100000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000000l);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000l);
        assertDecoded("10000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x10000000000000l);
        assertDecoded("100000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x100000000000000l);
        assertDecoded("1000000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000000l);
    }

    @Test(expected = NumericException.class)
    public void testBadEncoding() throws NumericException {
        assertDecoded("5g9796963abad00001e5f6bbdb38", 0, 0, -3458762426621895880l, 99607112989370l, 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testTooLong() throws NumericException {
        assertDecoded("10000000000000000000000000000000000000000000000000000000000000000", 0, 0, 0, 0, 0, 0x1000000000000000l);
    }

    private void assertDecoded(String hexString, int prefixSize, int suffixSize, long l0, long l1, long l2, long l3) throws NumericException {
        decoder.decode(hexString, prefixSize, hexString.length() - suffixSize);
        Assert.assertEquals(l0, this.l0);
        Assert.assertEquals(l1, this.l1);
        Assert.assertEquals(l2, this.l2);
        Assert.assertEquals(l3, this.l3);
    }

    @Before
    public void before() {
        decoder = new Long256FromCharSequenceDecoder() {

            @Override
            protected void onDecoded(long l0, long l1, long l2, long l3) {
                Long256FromCharSequenceDecoderTest.this.l0 = l0;
                Long256FromCharSequenceDecoderTest.this.l1 = l1;
                Long256FromCharSequenceDecoderTest.this.l2 = l2;
                Long256FromCharSequenceDecoderTest.this.l3 = l3;
            }
        };
    }
}
