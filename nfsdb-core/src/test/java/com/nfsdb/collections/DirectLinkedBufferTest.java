package com.nfsdb.collections;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Unsafe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DirectLinkedBufferTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testWriteSmallBuffersBuffer() throws Exception {
        testWriteToZeroBuffer(128, 33);
    }

    @Test
    public void testWriteBigBuffersBuffer() throws Exception {
        testWriteToZeroBuffer(128, 271);
    }

    @Test
    public void testGetWriteOffsetQuick() throws Exception {
        int pageLen = 128;
        try (DirectPagedBuffer pb = new DirectPagedBuffer(pageLen)) {
            Assert.assertEquals(0, pb.getWriteOffsetQuick(pageLen - 4));
            Assert.assertEquals(pageLen, pb.getWriteOffsetQuick(5));
            Assert.assertEquals(pageLen + 5, pb.getWriteOffsetQuick(8));
        }
    }

    @Test
    public void testGetWriteOffsetWithChecks() throws Exception {
        int pageLen = 128;
        try (DirectPagedBuffer pb = new DirectPagedBuffer(pageLen)) {
            exception.expect(JournalRuntimeException.class);
            pb.getWriteOffsetWithChecks(pageLen + 1);
        }
    }

    private void testWriteToZeroBuffer(int pageCapacity, int bufferLen) throws Exception {
        try (DirectPagedBuffer buffer = new DirectPagedBuffer(128)) {
            try (DirectPagedBuffer testPage = new DirectPagedBuffer(bufferLen)) {
                // append.
                long address = testPage.toAddress(0);
                for (int i = 0; i < bufferLen; i++) {
                    Unsafe.getUnsafe().putByte(address + i, (byte) (i % 255));
                }

                for (int j = 0; j < (long) pageCapacity / bufferLen + 1; j++) {
                    buffer.append(new DirectPagedBufferStream(testPage, 0, bufferLen));
                }
            }

            // read.
            long readBuffer = Unsafe.getUnsafe().allocateMemory(bufferLen);
            for (int j = 0; j < (long) pageCapacity / bufferLen + 1; j++) {
                buffer.read(readBuffer, j * bufferLen, bufferLen);

                for(int i = 0; i < bufferLen; i++){
                    Assert.assertEquals((byte)(i%255), Unsafe.getUnsafe().getByte(readBuffer + i), j);
                }
            }
        }
    }
}