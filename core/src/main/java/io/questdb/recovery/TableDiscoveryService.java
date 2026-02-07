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

package io.questdb.recovery;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

public class TableDiscoveryService {
    private final FilesFacade ff;

    public TableDiscoveryService(FilesFacade ff) {
        this.ff = ff;
    }

    public ObjList<DiscoveredTable> discoverTables(CharSequence dbRoot) {
        final ObjList<DiscoveredTable> tables = new ObjList<>();
        try (Path path = new Path(); MemoryCMR tableNameMem = Vm.getCMRInstance()) {
            path.of(dbRoot);
            final int rootLen = path.size();

            long findPtr = ff.findFirst(path.$());
            if (findPtr < 1) {
                return tables;
            }

            try {
                final Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
                do {
                    if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                        tables.add(discoverOne(path, rootLen, dirNameSink, tableNameMem));
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
                tableNameMem.close();
            }
        }
        return tables;
    }

    private DiscoveredTable discoverOne(Path path, int rootLen, Utf8StringSink dirNameSink, MemoryCMR tableNameMem) {
        final String dirName = Utf8s.toString(dirNameSink);
        final boolean hasTxn = ff.exists(path.trimTo(rootLen).concat(dirNameSink).concat(TableUtils.TXN_FILE_NAME).$());
        final boolean hasSeqTxnLog =
                ff.exists(path.trimTo(rootLen).concat(dirNameSink).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_FILE_NAME).$())
                        || ff.exists(path.trimTo(rootLen).concat(dirNameSink).concat(WalUtils.SEQ_DIR_DEPRECATED).concat(WalUtils.TXNLOG_FILE_NAME).$());
        final TableDiscoveryState state = hasTxn
                ? TableDiscoveryState.HAS_TXN
                : (hasSeqTxnLog ? TableDiscoveryState.WAL_ONLY : TableDiscoveryState.NO_TXN);

        String tableName = TableUtils.readTableName(path.trimTo(rootLen).concat(dirNameSink), rootLen, tableNameMem, ff);
        if (tableName == null) {
            tableName = Chars.toString(TableUtils.getTableNameFromDirName(dirName));
        }

        final MetaWalFlag walFlag = readMetaWalFlag(path, rootLen, dirNameSink);
        final DiscoveredTable discoveredTable = new DiscoveredTable(
                tableName,
                dirName,
                state,
                walFlag.known,
                walFlag.value
        );
        if (walFlag.issueCode != null) {
            discoveredTable.addIssue(walFlag.severity, walFlag.issueCode, walFlag.message);
        }
        return discoveredTable;
    }

    private MetaWalFlag readMetaWalFlag(Path path, int rootLen, Utf8StringSink dirNameSink) {
        path.trimTo(rootLen).concat(dirNameSink).concat(TableUtils.META_FILE_NAME);
        if (!ff.exists(path.$())) {
            return MetaWalFlag.UNKNOWN;
        }

        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            return new MetaWalFlag(
                    false,
                    false,
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.IO_ERROR,
                    "cannot open _meta [path=" + path + ", errno=" + ff.errno() + ']'
            );
        }

        try {
            final long len = ff.length(fd);
            if (len <= TableUtils.META_OFFSET_WAL_ENABLED) {
                return new MetaWalFlag(
                        false,
                        false,
                        RecoveryIssueSeverity.WARN,
                        RecoveryIssueCode.SHORT_FILE,
                        "_meta too short to read walEnabled [path=" + path + ", size=" + len + ']'
                );
            }
            final byte walRaw = ff.readNonNegativeByte(fd, TableUtils.META_OFFSET_WAL_ENABLED);
            return new MetaWalFlag(true, walRaw != 0, null, null, null);
        } finally {
            ff.close(fd);
        }
    }

    private static class MetaWalFlag {
        private static final MetaWalFlag UNKNOWN = new MetaWalFlag(false, false, null, null, null);
        private final RecoveryIssueCode issueCode;
        private final String message;
        private final boolean known;
        private final RecoveryIssueSeverity severity;
        private final boolean value;

        private MetaWalFlag(
                boolean known,
                boolean value,
                RecoveryIssueSeverity severity,
                RecoveryIssueCode issueCode,
                String message
        ) {
            this.known = known;
            this.value = value;
            this.severity = severity;
            this.issueCode = issueCode;
            this.message = message;
        }
    }
}
