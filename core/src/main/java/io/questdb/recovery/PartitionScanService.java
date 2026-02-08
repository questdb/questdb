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

import io.questdb.cairo.ColumnType;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.util.Comparator;
import java.util.HashMap;

public class PartitionScanService {
    private final FilesFacade ff;

    public PartitionScanService(FilesFacade ff) {
        this.ff = ff;
    }

    public ObjList<PartitionScanEntry> scan(
            CharSequence tableDir,
            TxnState txnState,
            MetaState metaState
    ) {
        int partitionBy = metaState != null ? metaState.getPartitionBy() : TxnState.UNSET_INT;
        int timestampType = metaState != null ? metaState.getTimestampColumnType() : ColumnType.TIMESTAMP;

        // step 1: build expected dir name -> txn index map
        HashMap<String, Integer> expectedDirNames = new HashMap<>();
        ObjList<String> expectedDirNameList = new ObjList<>();
        ObjList<String> partitionNameList = new ObjList<>();
        int txnPartitionCount = 0;
        if (txnState != null) {
            ObjList<TxnPartitionState> partitions = txnState.getPartitions();
            txnPartitionCount = partitions.size();
            for (int i = 0; i < txnPartitionCount; i++) {
                TxnPartitionState part = partitions.getQuick(i);
                String partName = ConsoleRenderer.formatPartitionName(partitionBy, timestampType, part.getTimestampLo());
                String dirName = ConsoleRenderer.formatPartitionDirName(partName, part.getNameTxn());
                expectedDirNames.put(dirName, i);
                expectedDirNameList.add(dirName);
                partitionNameList.add(partName);
            }
        }

        boolean[] matched = new boolean[txnPartitionCount];

        // step 2: scan filesystem
        ObjList<PartitionScanEntry> orphanEntries = new ObjList<>();

        try (Path path = new Path()) {
            path.of(tableDir);
            int tableDirLen = path.size();

            long findPtr = ff.findFirst(path.$());
            if (findPtr > 0) {
                try {
                    Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
                    do {
                        if (ff.isDirOrSoftLinkDirNoDots(path, tableDirLen, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                            String name = Utf8s.toString(dirNameSink);
                            if (!isInternalDir(name)) {
                                Integer txnIndex = expectedDirNames.get(name);
                                if (txnIndex != null) {
                                    matched[txnIndex] = true;
                                } else {
                                    orphanEntries.add(new PartitionScanEntry(
                                            name, name,
                                            PartitionScanStatus.ORPHAN, null, -1
                                    ));
                                }
                            }
                        }
                    } while (ff.findNext(findPtr) > 0);
                } finally {
                    ff.findClose(findPtr);
                }
            }
        }

        // step 3: build result in correct order
        ObjList<PartitionScanEntry> result = new ObjList<>();

        // MATCHED entries in _txn order
        if (txnState != null) {
            ObjList<TxnPartitionState> partitions = txnState.getPartitions();
            for (int i = 0; i < txnPartitionCount; i++) {
                if (matched[i]) {
                    result.add(new PartitionScanEntry(
                            expectedDirNameList.getQuick(i),
                            partitionNameList.getQuick(i),
                            PartitionScanStatus.MATCHED,
                            partitions.getQuick(i),
                            i
                    ));
                }
            }
        }

        // ORPHAN entries sorted alphabetically
        orphanEntries.sort(Comparator.comparing(PartitionScanEntry::getDirName));
        for (int i = 0, n = orphanEntries.size(); i < n; i++) {
            result.add(orphanEntries.getQuick(i));
        }

        // MISSING entries in _txn order
        if (txnState != null) {
            ObjList<TxnPartitionState> partitions = txnState.getPartitions();
            for (int i = 0; i < txnPartitionCount; i++) {
                if (!matched[i]) {
                    result.add(new PartitionScanEntry(
                            expectedDirNameList.getQuick(i),
                            partitionNameList.getQuick(i),
                            PartitionScanStatus.MISSING,
                            partitions.getQuick(i),
                            i
                    ));
                }
            }
        }

        return result;
    }

    static boolean isInternalDir(String name) {
        return name.charAt(0) == '_'
                || name.charAt(0) == '.'
                || name.startsWith("wal")
                || name.startsWith("txn_seq")
                || name.equals("seq")
                || name.equals("snapshot");
    }
}
