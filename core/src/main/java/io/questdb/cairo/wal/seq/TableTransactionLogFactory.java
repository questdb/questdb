/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalUtils.*;

public class TableTransactionLogFactory {
    private static final Log LOG = LogFactory.getLog(TableTransactionLogFactory.class);

    public static TableTransactionLog create(Path path, long timestamp, FilesFacade ff, int txnFileChunkTransactionCount) {
        int formatVersion  = txnFileChunkTransactionCount > 0 ? WAL_FORMAT_VERSION_V2 : WAL_FORMAT_VERSION_V1;
        switch (formatVersion) {
            case WAL_FORMAT_VERSION_V1:
                TableTransactionLogV1 ttl = new TableTransactionLogV1(ff);
                ttl.create(path, timestamp);
                return ttl;
            case WAL_FORMAT_VERSION_V2:
                TableTransactionLogV2 tt2 = new TableTransactionLogV2(ff);
                tt2.create(path, timestamp, txnFileChunkTransactionCount);
                return tt2;
            default:
                throw new UnsupportedOperationException("Unsupported transaction log version: " + formatVersion);
        }
    }

    public static TableTransactionLog open(Path path, FilesFacade ff) {
        int pathLen = path.size();
        int logFileFd = TableUtils.openRW(ff, path.concat(TXNLOG_FILE_NAME).$(), LOG, CairoConfiguration.O_NONE);
        int formatVersion;
        try {
            formatVersion = ff.readNonNegativeInt(logFileFd, 0);
            if (formatVersion < 0) {
                throw CairoException.critical(0).put("invalid transaction log file: ").put(path).put(", cannot read version at offset 0");
            }
        } finally {
            path.trimTo(pathLen);
            ff.close(logFileFd);
        }

        switch (formatVersion) {
            case WAL_FORMAT_VERSION_V1:
                TableTransactionLogV1 ttl = new TableTransactionLogV1(ff);
                ttl.open(path);
                return ttl;
            case WAL_FORMAT_VERSION_V2:
                TableTransactionLogV2 tt2 = new TableTransactionLogV2(ff);
                tt2.open(path);
                return tt2;
            default:
                throw new UnsupportedOperationException("Unsupported transaction log version: " + formatVersion);
        }
    }
}
