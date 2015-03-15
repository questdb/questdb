package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class DirectLinkedBufferTest {

    @Test
    public void testWriteSmallBuffersBuffer() throws Exception {
        testWriteToZeroBuffer(128, 33);
    }


    @Test
    public void testWriteBigBuffersBuffer() throws Exception {
        testWriteToZeroBuffer(128, 271);
    }

    private void testWriteToZeroBuffer(int pageCapacity, int bufferLen) throws Exception {
        try (DirectLinkedBuffer lb = new DirectLinkedBuffer(128)) {
            // write.
            long buffer = Unsafe.getUnsafe().allocateMemory(bufferLen);
            for (int i = 0; i < bufferLen; i++) {
                Unsafe.getUnsafe().putByte(buffer + i , (byte) (i % 255));
            }

            for (int j = 0; j < (long) pageCapacity / bufferLen + 1; j++) {
                lb.write(buffer, j*bufferLen, bufferLen);
            }
            Unsafe.getUnsafe().freeMemory(buffer);

            // read.
            long readBuffer = Unsafe.getUnsafe().allocateMemory(bufferLen);
            for (int j = 0; j < (long) pageCapacity / bufferLen + 1; j++) {
                lb.read(readBuffer, j*bufferLen, bufferLen);

                for(int i = 0; i < bufferLen; i++){
                    Assert.assertEquals((byte)(i%255), Unsafe.getUnsafe().getByte(readBuffer + i), j);
                }
            }
        }
    }
}