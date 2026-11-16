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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;

/**
 * The parts of a materialized view's transaction log, kept with {@code cairo.default.sequencer.part.txn.count} above
 * 0, and the WAL purge that deletes the parts below the one that holds the view's applied seqTxn.
 */
final class MatViewTxnLogParts {

    private MatViewTxnLogParts() {
    }

    // Whether the part of the table's transaction log is on disk.
    static boolean exists(CairoEngine engine, TableToken tableToken, long part) {
        final CairoConfiguration configuration = engine.getConfiguration();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR)
                    .slash().put(part);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    // Runs the WAL purge as it runs once the writers went idle and, under the ADAPTIVE commit mode, a save point covers
    // the view's applied transactions: the purge then deletes the view's txn-log parts below the one that holds its
    // applied seqTxn.
    static void purge(CairoEngine engine, TableToken viewToken) {
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        engine.releaseAllViewWalWriters();
        if (engine.getConfiguration().getCommitMode() == CommitMode.ADAPTIVE) {
            // The purge keeps the WAL back to the older of the two save points; the writers' close took one at the
            // applied transactions, and this one moves the older one there too.
            try (TableWriter writer = engine.getWriter(viewToken, "test")) {
                writer.advanceDurableEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
            }
        }
        TestUtils.drainPurgeJob(engine);
    }
}
