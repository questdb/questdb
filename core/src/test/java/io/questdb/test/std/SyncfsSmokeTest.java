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
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Smoke test verifying that the syncfs JNI binding resolves at runtime (no UnsatisfiedLinkError) and that
 * FilesFacadeImpl.syncfs behaves like fsync (returns/does-not-throw on a valid fd). syncfs(2) makes the whole
 * filesystem containing the fd durable in one device flush — the primitive the batched SYNC commit uses to
 * journal all column extent conversions at once (replacing the broken foreign-_cv-flush reliance).
 */
public class SyncfsSmokeTest extends AbstractTest {

    @Test
    public void testSyncfsJniResolvesAndSucceeds() throws Exception {
        // Create a real temp file, open it, write a few bytes, then call syncfs via Files and FilesFacadeImpl.
        File tmpFile = temp.newFile("syncfs_smoke.dat");
        try (Path path = new Path()) {
            path.of(tmpFile.getAbsolutePath()).$();

            // Open the file read-write (0 = O_NONE)
            long fd = Files.openRW(path.$(), 0);
            Assert.assertTrue("open failed, fd=" + fd, fd > 0);
            try {
                // Call syncfs via the static method (directly exercises the JNI symbol). On Linux this is a
                // real syncfs(2) over the temp filesystem; on non-Linux it falls back to fsync(fd). Either
                // way it must resolve and return 0 on a valid fd.
                int rc = Files.syncfs(fd);
                Assert.assertEquals("Files.syncfs should return 0 on a valid fd", 0, rc);

                // Call syncfs via FilesFacadeImpl (exercises the full Java facade path; must not throw
                // CairoException or UnsatisfiedLinkError).
                FilesFacade ff = new FilesFacadeImpl();
                ff.syncfs(fd);
            } finally {
                Files.close(fd);
            }
        }
    }
}
