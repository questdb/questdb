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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;


import static io.questdb.cairo.TableUtils.dFile;

public class DropIndexOperator extends PurgingOperator {
    private static final Log LOG = LogFactory.getLog(DropIndexOperator.class);
    private final LongList rollbackColumnVersions = new LongList();
    private final Path other;

    public DropIndexOperator(
            CairoConfiguration configuration,
            MessageBus messageBus,
            TableWriter tableWriter,
            Path path,
            Path other,
            int rootLen
    ) {
        super(LOG, configuration, messageBus, tableWriter, path, rootLen);
        this.other = other;
    }

    public void executeDropIndex(CharSequence columnName, int columnIndex) {
        int partitionBy = tableWriter.getPartitionBy();
        int partitionCount = tableWriter.getPartitionCount();
        try {
            updateColumnIndexes.clear();
            cleanupColumnVersions.clear();
            rollbackColumnVersions.clear();
            for (int pIndex = 0; pIndex < partitionCount; pIndex++) {
                long pTimestamp = tableWriter.getPartitionTimestamp(pIndex);
                long pVersion = tableWriter.getPartitionNameTxn(pIndex);
                long columnVersion = tableWriter.getColumnNameTxn(pTimestamp, columnIndex);
                long columnTop = tableWriter.getColumnTop(pTimestamp, columnIndex, -1L);

                // bump up column version, metadata will be updated later
                tableWriter.upsertColumnVersion(pTimestamp, columnIndex, columnTop);
                final long columnDropIndexVersion = tableWriter.getColumnNameTxn(pTimestamp, columnIndex);

                // create hard link to column data
                // src
                partitionDFile(path, rootLen, partitionBy, pTimestamp, pVersion, columnName, columnVersion);
                // hard link
                partitionDFile(other, rootLen, partitionBy, pTimestamp, pVersion, columnName, columnDropIndexVersion);
                if (-1 == ff.hardLink(path, other)) {
                    throw CairoException.critical(ff.errno())
                            .put("cannot hardLink [src=").put(path)
                            .put(", hardLink=").put(other)
                            .put(']');
                }

                // add to cleanup tasks, the index will be removed in due time
                updateColumnIndexes.add(columnIndex);
                cleanupColumnVersions.add(columnIndex, columnVersion, pTimestamp, pVersion);
                rollbackColumnVersions.add(columnIndex, columnDropIndexVersion, pTimestamp, pVersion);
            }
        } catch (Throwable th) {
            LOG.error().$("Could not DROP INDEX: ").$(th.getMessage()).$();
            tableWriter.rollbackUpdate();
            updateColumnIndexes.clear();
            cleanupColumnVersions.clear();

            // cleanup successful links prior to the failed link operation
            int limit = rollbackColumnVersions.size();
            if (limit / 4 < partitionCount) {
                for (int i = 0; i < limit; i += 4) {
                    final long columnDropIndexVersion = rollbackColumnVersions.getQuick(i + 1);
                    final long pTimestamp = rollbackColumnVersions.getQuick(i + 2);
                    final long partitionNameTxn = rollbackColumnVersions.getQuick(i + 3);
                    partitionDFile(other, rootLen, partitionBy, pTimestamp, partitionNameTxn, columnName, columnDropIndexVersion);
                    if (!ff.remove(other)) {
                        LOG.info().$("Please remove this file \"").$(other).$('"').I$();
                    }
                }
            }
            throw th;
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private static void partitionDFile(
            Path path,
            int rootLen,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn
    ) {
        TableUtils.setPathForPartition(
                path,
                rootLen,
                partitionBy,
                partitionTimestamp,
                partitionNameTxn
        );
        dFile(path, columnName, columnNameTxn);
    }
}
