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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FailureFileFacadeTest {
    private static final int MKDIR_MODE = 509;
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * A write through the destination shows in the source when hardLinkDirRecursive() linked the directory, and not
     * when it copied it.
     */
    @Test
    public void testHardLinkDirRecursiveCopiesOnlyDetachingDirWhenDetachCopied() throws Exception {
        assertMemoryLeak(() -> {
            final FailureFileFacade ff = new FailureFileFacade(TestFilesFacadeImpl.INSTANCE);
            final String root = temporaryFolder.getRoot().getAbsolutePath();
            final File srcFile = new File(temporaryFolder.newFolder("x"), "f.d");
            TestUtils.writeStringToFile(srcFile, "a");
            try (Path src = new Path(); Path dst = new Path()) {
                src.of(root).concat("x");

                // the switch is off by default: the temporary directory of DETACH PARTITION gets a hard link
                dst.of(root).concat("x.1").put(TableUtils.DETACHING_DIR_MARKER);
                Assert.assertEquals(0, ff.hardLinkDirRecursive(src, dst, MKDIR_MODE));
                final File linkedWhileOff = new File(dst.toString(), "f.d");
                Assert.assertEquals("a", TestUtils.readStringFromFile(linkedWhileOff));
                TestUtils.writeStringToFile(linkedWhileOff, "b");
                Assert.assertEquals("b", TestUtils.readStringFromFile(srcFile));

                // the temporary directory of DETACH PARTITION gets a copy while the switch is on
                ff.setDetachCopied(true);
                dst.of(root).concat("x.2").put(TableUtils.DETACHING_DIR_MARKER);
                Assert.assertEquals(0, ff.hardLinkDirRecursive(src, dst, MKDIR_MODE));
                final File copied = new File(dst.toString(), "f.d");
                Assert.assertEquals("b", TestUtils.readStringFromFile(copied));
                TestUtils.writeStringToFile(copied, "c");
                Assert.assertEquals("b", TestUtils.readStringFromFile(srcFile));

                // any other directory gets a hard link while the switch is on
                dst.of(root).concat("x.3");
                Assert.assertEquals(0, ff.hardLinkDirRecursive(src, dst, MKDIR_MODE));
                final File linkedWhileOn = new File(dst.toString(), "f.d");
                Assert.assertEquals("b", TestUtils.readStringFromFile(linkedWhileOn));
                TestUtils.writeStringToFile(linkedWhileOn, "d");
                Assert.assertEquals("d", TestUtils.readStringFromFile(srcFile));
            }
        });
    }
}
