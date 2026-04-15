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

package io.questdb.cairo;

import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;

import java.io.Closeable;

/**
 * Turns a single {@link PostingSealPurgeTask} into actual file deletions
 * when the table's {@link TxnScoreboard} confirms no reader is still in the
 * visibility window of the superseded sealed version.
 * <p>
 * Stateless aside from the reused {@link Path} buffer;
 * {@link PostingSealPurgeJob} owns one instance and feeds it tasks
 * sequentially.
 */
public class PostingSealPurgeOperator implements Closeable {

    private static final Log LOG = LogFactory.getLog(PostingSealPurgeOperator.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final Path path;
    private final int pathRootLen;
    private TxnScoreboard txnScoreboard;

    public PostingSealPurgeOperator(CairoEngine engine) {
        try {
            this.engine = engine;
            CairoConfiguration configuration = engine.getConfiguration();
            this.ff = configuration.getFilesFacade();
            this.path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            this.path.of(configuration.getDbRoot());
            this.pathRootLen = path.size();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(path);
        txnScoreboard = Misc.free(txnScoreboard);
    }

    public boolean purge(PostingSealPurgeTask task) {
        if (task.isEmpty()) {
            return true;
        }
        TableToken originalToken = task.getTableToken();
        TableToken liveToken = engine.getTableTokenIfExists(originalToken.getTableName());
        if (liveToken == null || liveToken.getTableId() != originalToken.getTableId()) {
            LOG.info().$("posting seal purge: table no longer exists, skipping [table=")
                    .$(originalToken.getTableName())
                    .I$();
            return true;
        }
        if (txnScoreboard != null && !txnScoreboard.getTableToken().equals(liveToken)) {
            txnScoreboard = Misc.free(txnScoreboard);
        }
        if (txnScoreboard == null) {
            try {
                txnScoreboard = engine.getTxnScoreboard(liveToken);
            } catch (Throwable th) {
                LOG.error().$("posting seal purge: cannot acquire scoreboard, retrying [table=")
                        .$(liveToken.getTableName())
                        .$(", err=").$(th)
                        .I$();
                return false;
            }
        }

        boolean safe;
        try {
            safe = txnScoreboard.isRangeAvailable(task.getFromTableTxn(), task.getToTableTxn());
        } catch (CairoException ex) {
            LOG.error().$("posting seal purge: scoreboard query failed, retrying [table=")
                    .$(liveToken.getTableName())
                    .$(", column=").$(task.getIndexColumnName())
                    .$(", from=").$(task.getFromTableTxn())
                    .$(", to=").$(task.getToTableTxn())
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .I$();
            return false;
        }
        if (!safe) {
            return false;
        }

        path.trimTo(pathRootLen).concat(liveToken.getDirName());
        int pathTableLen = path.size();
        TableUtils.setPathForNativePartition(
                path,
                taskTimestampType(task, liveToken),
                task.getPartitionBy(),
                task.getPartitionTimestamp(),
                task.getPartitionNameTxn()
        );
        int pathPartitionLen = path.size();

        // Remove the .pv at the target sealTxn
        boolean allRemoved = true;
        path.trimTo(pathPartitionLen);
        LPSZ pv = PostingIndexUtils.valueFileName(path, task.getIndexColumnName(),
                task.getPostingColumnNameTxn(), task.getSealTxn());
        if (!ff.removeQuiet(pv)) {
            // Either it's a Windows file-in-use case or a transient I/O error;
            // either way, a follow-up retry will resolve it.
            allRemoved = false;
        }

        // Remove every .pc<N> at the target sealTxn. We cannot enumerate by
        // postingColumnNameTxn (the .pc<N> filename only has coveredColumnNameTxn),
        // so scanSealedFiles is the safe enumerator.
        final boolean[] coverRemoved = {true};
        final long targetSealTxn = task.getSealTxn();
        final String columnName = task.getIndexColumnName();
        path.trimTo(pathPartitionLen);
        PostingIndexUtils.scanSealedFiles(ff, path, pathPartitionLen, columnName, new PostingIndexUtils.SealedFileVisitor() {
            @Override
            public void onCoverDataFile(int includeIdx, long coveredColumnNameTxn, long sealTxn) {
                if (sealTxn != targetSealTxn) {
                    return;
                }
                LPSZ pc = PostingIndexUtils.coverDataFileName(path.trimTo(pathPartitionLen),
                        columnName, includeIdx, coveredColumnNameTxn, sealTxn);
                if (!ff.removeQuiet(pc)) {
                    coverRemoved[0] = false;
                }
                path.trimTo(pathPartitionLen);
            }

            @Override
            public void onValueFile(long postingColumnNameTxn, long sealTxn) {
                // .pv handled above by exact-name removeQuiet; skip here.
            }
        });
        path.trimTo(pathPartitionLen);

        boolean done = allRemoved && coverRemoved[0];
        if (done) {
            LOG.info().$("purged posting sealed version [table=").$(liveToken.getTableName())
                    .$(", column=").$(columnName)
                    .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                    .$(", sealTxn=").$(task.getSealTxn())
                    .I$();
        }
        path.trimTo(pathTableLen);
        return done;
    }

    /**
     * Reads the table's timestamp type from live metadata. The task does not
     * carry the timestamp type because it can change only via ALTER (which
     * also bumps {@code postingColumnNameTxn}, invalidating the task), so the
     * live value is always correct.
     */
    private int taskTimestampType(PostingSealPurgeTask task, TableToken token) {
        try (TableMetadata m = engine.getTableMetadata(token)) {
            return m.getTimestampType();
        } catch (Throwable th) {
            // Fall back to micros (the historic default) if metadata is
            // unavailable. The path may still resolve correctly because most
            // tables use the default and the task's partitionBy + timestamp
            // are still authoritative.
            return ColumnType.TIMESTAMP_MICRO;
        }
    }
}
