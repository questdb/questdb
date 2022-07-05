/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class IOUringTest {

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    private static final FilesFacade ff = new FilesFacadeImpl();
    private static final IOUringFacade rf = new IOUringFacadeImpl();

    @Test
    public void testRead() throws IOException {
        Assume.assumeTrue(rf.isAvailable());

        final int inFlight = 4;
        final int txtLen = 42;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inFlight; i++) {
            sb.append(String.valueOf(i).repeat(txtLen));
        }
        String txt = sb.toString();

        File file = temp.newFile();
        TestUtils.writeStringToFile(file, txt);

        try (
                Path path = new Path();
                IOUring ring = new IOUring(rf, 32)
        ) {
            long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
            Assert.assertTrue(fd > -1);

            Set<Long> expectedIds = new HashSet<>();
            long[] bufs = new long[inFlight];
            for (int i = 0; i < inFlight; i++) {
                bufs[i] = Unsafe.malloc(txtLen, MemoryTag.NATIVE_DEFAULT);
                long id = ring.enqueueRead(fd, i * txtLen, bufs[i], txtLen);
                Assert.assertTrue(id > -1);
                expectedIds.add(id);
            }

            int submitted = ring.submit();
            Assert.assertEquals(4, submitted);

            Set<Long> actualIds = new HashSet<>();
            for (int i = 0; i < inFlight; i++) {
                while (!ring.nextCqe()) {
                    Os.pause();
                }
                actualIds.add(ring.getCqeId());
                Assert.assertEquals(txtLen, ring.getCqeRes());
            }

            DirectByteCharSequence txtInBuf = new DirectByteCharSequence();
            for (int i = 0; i < inFlight; i++) {
                txtInBuf.of(bufs[i], bufs[i] + txtLen);
                TestUtils.assertEquals(txt.substring(i * txtLen, (i + 1) * txtLen), txtInBuf);
            }

            Assert.assertEquals(expectedIds, actualIds);

            for (int i = 0; i < inFlight; i++) {
                Unsafe.free(bufs[i], txtLen, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testIsIOUringAvailable() {
        Assert.assertFalse(IOUringFacadeImpl.isIOUringAvailable("6.1"));
        Assert.assertFalse(IOUringFacadeImpl.isIOUringAvailable("4.0.0"));
        Assert.assertFalse(IOUringFacadeImpl.isIOUringAvailable("5.0.0"));

        Assert.assertTrue(IOUringFacadeImpl.isIOUringAvailable("5.1.0"));
        Assert.assertTrue(IOUringFacadeImpl.isIOUringAvailable("5.14.0-1044-oem"));
        Assert.assertTrue(IOUringFacadeImpl.isIOUringAvailable("5.10.2"));
        Assert.assertTrue(IOUringFacadeImpl.isIOUringAvailable("6.2.2"));
        Assert.assertTrue(IOUringFacadeImpl.isIOUringAvailable("7.1.1"));
    }
}
