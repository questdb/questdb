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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The sequencer's per-record CRC lives in {@code TableTransactionLogV2}'s reserved trailing slot, and
 * V2 is selected only when {@code cairo.default.sequencer.part.txn.count > 0}. That default was 0, so
 * every newly created WAL table got V1 -- the one format whose records have no room for a CRC.
 * <p>
 * V1 is still supported and now carries its CRCs in the additive {@code _txnlog.c} sidecar, but new
 * tables should get the format that needs no sidecar at all.
 */
public class SequencerFormatDefaultTest extends AbstractCairoTest {

    @Test
    public void testDefaultPartTxnCountIsFiveThousand() {
        // 5000 is the value server.conf already documents for the replication primary and validates to
        // 10..299593, so tuned deployments keep the part size they already run.
        Assert.assertEquals(5000, configuration.getDefaultSeqPartTxnCount());
    }

    @Test
    public void testDroppedV2TableLeavesNothingBehind() throws Exception {
        // The regression that blocked this default: a V2 table always has at least seq part 0, and the
        // purge job treated a part file EXISTING as pending work, so no V2 table could ever be fully
        // dropped and its txn_seq leaked on every DROP.
        assertMemoryLeak(() -> {
            execute("create table seq_drop (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into seq_drop values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("seq_drop");
            execute("drop table seq_drop");
            drainWalQueue();
            drainPurgeJob();

            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token);
                Assert.assertFalse("the dropped table's directory must be reclaimed", ff.exists(path.$()));
            }
        });
    }

    @Test
    public void testNewWalTableUsesV2ByDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table seq_v2 (ts timestamp, v long) timestamp(ts) partition by day wal");
            Assert.assertEquals(
                    "a newly created WAL table must use the V2 txnlog, or its records carry no in-record CRC",
                    WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2,
                    readFormatVersion("seq_v2")
            );
        });
    }

    private int readFormatVersion(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_FILE_NAME);
            final long fd = ff.openRO(path.$());
            Assert.assertTrue("could not open " + path, fd > -1);
            try {
                // Mirrors TableTransactionLog.getFormatVersion: the format version is the header's
                // first int.
                return ff.readNonNegativeInt(fd, 0);
            } finally {
                ff.close(fd);
            }
        }
    }
}
