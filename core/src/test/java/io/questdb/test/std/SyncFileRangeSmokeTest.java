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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Smoke test verifying the sync_file_range JNI binding resolves at runtime (no UnsatisfiedLinkError) and
 * returns 0 on a valid, written file. On Linux it issues a real sync_file_range(2); on other platforms the
 * native shim is a no-op that also returns 0. Exercises both the static {@link Files} path and the
 * {@link FilesFacade} default method.
 */
public class SyncFileRangeSmokeTest extends AbstractTest {

    @Test
    public void testSyncFileRangeJniResolvesAndSucceeds() throws Exception {
        File tmpFile = temp.newFile("sync_file_range_smoke.dat");
        try (Path path = new Path()) {
            path.of(tmpFile.getAbsolutePath()).$();

            long fd = Files.openRW(path.$(), 0);
            Assert.assertTrue("open failed, fd=" + fd, fd > 0);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                // Write some bytes so there is dirty page-cache content for sync_file_range to act on.
                Unsafe.getUnsafe().setMemory(buf, 64, (byte) 7);
                Assert.assertEquals(64, Files.write(fd, buf, 64, 0));
                Files.fdatasync(fd);

                final int flags = Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER;

                // nbytes == 0 means "to end of file" on Linux; must resolve the JNI symbol and return 0.
                int rc = Files.syncFileRange(fd, 0, 0, flags);
                Assert.assertEquals("Files.syncFileRange should return 0 on a valid written file", 0, rc);

                // And via the FilesFacade default method.
                FilesFacade ff = new FilesFacadeImpl();
                int rc2 = ff.syncFileRange(fd, 0, 64, flags);
                Assert.assertEquals("FilesFacade.syncFileRange should return 0", 0, rc2);
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                Files.close(fd);
            }
        }
    }
}
