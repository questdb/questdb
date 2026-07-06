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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoKeywords;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CairoKeywordsTest extends AbstractTest {

    @Test
    public void testIsLiveViewCheckpointsMatchesOnlyExactDirName() {
        // The live view checkpoint directory must be recognised so the partition-purge scans
        // (TableWriter.removePartitionDirsNotAttached and TableSnapshotRestore) skip it instead of
        // trying to parse it as a partition timestamp and logging a spurious ERROR.
        assertLiveViewCheckpoints(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME, true);
        assertLiveViewCheckpoints("_checkpoints", true);

        // Near misses must not match: a shorter/longer name, a differently-suffixed name, or a
        // name that merely shares the prefix.
        assertLiveViewCheckpoints("_checkpoint", false);
        assertLiveViewCheckpoints("_checkpointss", false);
        assertLiveViewCheckpoints("_checkpoints2", false);
        assertLiveViewCheckpoints("_checkpoints.tmp", false);
        assertLiveViewCheckpoints("checkpoints", false);
        assertLiveViewCheckpoints("_Checkpoints", false);
        assertLiveViewCheckpoints("", false);

        // Real partition directory names and other internal table folder entries must not match.
        assertLiveViewCheckpoints("2023-01-01", false);
        assertLiveViewCheckpoints("2023-01-01.5", false);
        assertLiveViewCheckpoints("wal1", false);
        assertLiveViewCheckpoints("txn_seq", false);
        assertLiveViewCheckpoints("seq", false);
        assertLiveViewCheckpoints("_meta", false);
        assertLiveViewCheckpoints("_txn", false);
    }

    private static void assertLiveViewCheckpoints(String name, boolean expected) {
        try (Path path = new Path()) {
            path.of(name).$();
            Assert.assertEquals(
                    "isLiveViewCheckpoints(\"" + name + "\")",
                    expected,
                    CairoKeywords.isLiveViewCheckpoints(path.ptr())
            );
        }
    }
}
