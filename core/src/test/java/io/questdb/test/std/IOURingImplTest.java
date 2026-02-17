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

import io.questdb.cairo.CairoException;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

public class IOURingImplTest extends AbstractTest {

    private static final IOURingFacade rf = new IOURingFacadeImpl();

    @Test(expected = CairoException.class)
    public void testFailsToInit() {
        final IOURingFacade rf = new IOURingFacadeImpl() {
            @Override
            public long create(int capacity) {
                return -42;
            }
        };
        try (IOURing ignored = rf.newInstance(32)) {
            Assert.fail();
        }
    }

    @Test
    public void testIsAvailableOn() {
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("6.1"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("4.0.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.0.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.1.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.10.2"));

        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.12.0"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.14.0-1044-oem"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.13.1"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("6.2.2"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("7.1.1"));
    }

    @Test
    public void testRead() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int inFlight = 34;
            final int txtLen = 42;
            StringSink sink = new StringSink();
            for (int i = 0; i < inFlight; i++) {
                for (int j = 0; j < txtLen; j++) {
                    sink.put(i);
                }
            }
            String txt = sink.toString();

            File file = temp.newFile();
            TestUtils.writeStringToFile(file, txt);

            try (Path path = new Path()) {
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                long[] bufs = new long[inFlight];
                for (int i = 0; i < inFlight; i++) {
                    bufs[i] = Unsafe.malloc(txtLen, MemoryTag.NATIVE_DEFAULT);
                }

                try (IOURing ring = rf.newInstance(Numbers.ceilPow2(inFlight))) {
                    LongList expectedIds = new LongList();
                    for (int i = 0; i < inFlight; i++) {
                        long id = ring.enqueueRead(fd, i * txtLen, bufs[i], txtLen);
                        Assert.assertTrue(id > -1);
                        expectedIds.add(id);
                    }

                    int submitted = ring.submit();
                    Assert.assertEquals(inFlight, submitted);

                    LongList actualIds = new LongList();
                    for (int i = 0; i < inFlight; i++) {
                        while (!ring.nextCqe()) {
                            Os.pause();
                        }
                        actualIds.add(ring.getCqeId());
                        Assert.assertEquals(txtLen, ring.getCqeRes());
                    }

                    DirectUtf8String txtInBuf = new DirectUtf8String();
                    for (int i = 0; i < inFlight; i++) {
                        txtInBuf.of(bufs[i], bufs[i] + txtLen);
                        TestUtils.assertEquals(txt.substring(i * txtLen, (i + 1) * txtLen), txtInBuf);
                    }

                    TestUtils.assertEquals(expectedIds, actualIds);
                } finally {
                    Files.close(fd);
                    for (int i = 0; i < inFlight; i++) {
                        Unsafe.free(bufs[i], txtLen, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testSqOverflow() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int inFlight = 32;
            final int iterations = 100;
            try (IOURing ring = rf.newInstance(inFlight)) {
                for (int it = 0; it < iterations; it++) {
                    for (int i = 0; i < inFlight; i++) {
                        long id = ring.enqueueNop();
                        Assert.assertTrue(id > -1);
                    }
                    Assert.assertEquals(-1, ring.enqueueNop());

                    int submitted = ring.submit();
                    Assert.assertEquals(inFlight, submitted);

                    for (int i = 0; i < inFlight; i++) {
                        while (!ring.nextCqe()) {
                            Os.pause();
                        }
                        Assert.assertTrue(ring.getCqeId() > -1);
                        Assert.assertTrue(ring.getCqeRes() > -1);
                    }
                    Assert.assertFalse(ring.nextCqe());
                    Assert.assertEquals(-1, ring.getCqeId());
                    Assert.assertEquals(-1, ring.getCqeRes());
                }
            }
        });
    }

    @Test
    public void testSubmitAndWait() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            String txt = "1234";
            final int txtLen = txt.length();
            File file = temp.newFile();
            TestUtils.writeStringToFile(file, txt);

            try (Path path = new Path()) {
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                long buf = Unsafe.malloc(txtLen, MemoryTag.NATIVE_DEFAULT);

                try (IOURing ring = rf.newInstance(4)) {
                    long id = ring.enqueueRead(fd, 0, buf, txtLen);
                    Assert.assertTrue(id > -1);

                    int submitted = ring.submitAndWait();
                    Assert.assertEquals(1, submitted);

                    Assert.assertTrue(ring.nextCqe());
                    Assert.assertEquals(id, ring.getCqeId());
                    Assert.assertEquals(txtLen, ring.getCqeRes());

                    DirectUtf8String txtInBuf = new DirectUtf8String().of(buf, buf + txtLen);
                    TestUtils.assertEquals(txt.substring(0, txtLen), txtInBuf);
                } finally {
                    Files.close(fd);
                    Unsafe.free(buf, txtLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
