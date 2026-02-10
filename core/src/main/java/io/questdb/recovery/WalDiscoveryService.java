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
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.util.Comparator;

/**
 * Scans a table directory for WAL sub-directories ({@code wal1}, {@code wal2}, etc.)
 * and cross-references them with the sequencer txnlog to classify each WAL as
 * {@link WalScanStatus#REFERENCED}, {@link WalScanStatus#ORPHAN}, or
 * {@link WalScanStatus#MISSING}. Within each WAL directory, enumerates segment
 * sub-directories and checks for the presence of key files ({@code _event},
 * {@code _event.i}, {@code _meta}).
 */
public class WalDiscoveryService {
    private static final int NO_ENTRY = -1;
    private final FilesFacade ff;

    public WalDiscoveryService(FilesFacade ff) {
        this.ff = ff;
    }

    public WalScanState scan(CharSequence tableDir, SeqTxnLogState seqState) {
        ObjList<WalDirEntry> entries = new ObjList<>();
        ObjList<ReadIssue> issues = new ObjList<>();

        // collect walIds referenced in the txnlog
        IntHashSet referencedWalIds = new IntHashSet();
        IntIntHashMap walRefCounts = new IntIntHashMap();
        if (seqState != null) {
            ObjList<SeqTxnRecord> records = seqState.getRecords();
            for (int i = 0, n = records.size(); i < n; i++) {
                SeqTxnRecord rec = records.getQuick(i);
                int walId = rec.getWalId();
                if (walId > 0) {
                    referencedWalIds.add(walId);
                    int prev = walRefCounts.get(walId);
                    walRefCounts.put(walId, prev == NO_ENTRY ? 1 : prev + 1);
                }
            }
        }

        // scan filesystem for wal* dirs
        IntHashSet onDiskWalIds = new IntHashSet();
        try (Path path = new Path()) {
            path.of(tableDir).$();
            long findPtr = ff.findFirst(path.$());
            if (findPtr > 0) {
                try {
                    StringSink nameSink = new StringSink();
                    do {
                        if (ff.findType(findPtr) == Files.DT_DIR) {
                            nameSink.clear();
                            Utf8s.utf8ToUtf16Z(ff.findName(findPtr), nameSink);
                            String name = nameSink.toString();
                            if (name.startsWith(WalUtils.WAL_NAME_BASE) && name.length() > WalUtils.WAL_NAME_BASE.length()) {
                                try {
                                    int walId = Numbers.parseInt(name, WalUtils.WAL_NAME_BASE.length(), name.length());
                                    if (walId > 0) {
                                        onDiskWalIds.add(walId);
                                    }
                                } catch (NumericException ignore) {
                                    // not a valid wal dir name
                                }
                            }
                        }
                    } while (ff.findNext(findPtr) > 0);
                } finally {
                    ff.findClose(findPtr);
                }
            }
        }

        // build entries: on-disk WALs
        for (int i = 0, n = onDiskWalIds.size(); i < n; i++) {
            int walId = onDiskWalIds.get(i);
            WalScanStatus status = referencedWalIds.contains(walId) ? WalScanStatus.REFERENCED : WalScanStatus.ORPHAN;
            int refCount = walRefCounts.get(walId);
            if (refCount == NO_ENTRY) {
                refCount = 0;
            }
            ObjList<WalSegmentEntry> segments = scanSegments(tableDir, walId);
            entries.add(new WalDirEntry(walId, segments, status, refCount));
        }

        // add MISSING entries for walIds in txnlog but not on disk
        for (int i = 0, n = referencedWalIds.size(); i < n; i++) {
            int walId = referencedWalIds.get(i);
            if (!onDiskWalIds.contains(walId)) {
                int refCount = walRefCounts.get(walId);
                if (refCount == NO_ENTRY) {
                    refCount = 0;
                }
                entries.add(new WalDirEntry(walId, new ObjList<>(), WalScanStatus.MISSING, refCount));
            }
        }

        // sort by walId
        entries.sort(Comparator.comparingInt(WalDirEntry::walId));

        return new WalScanState(entries, issues);
    }

    private ObjList<WalSegmentEntry> scanSegments(CharSequence tableDir, int walId) {
        ObjList<WalSegmentEntry> segments = new ObjList<>();
        try (Path walPath = new Path()) {
            walPath.of(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).$();

            long findPtr = ff.findFirst(walPath.$());
            if (findPtr > 0) {
                try {
                    StringSink nameSink = new StringSink();
                    do {
                        if (ff.findType(findPtr) == Files.DT_DIR) {
                            nameSink.clear();
                            Utf8s.utf8ToUtf16Z(ff.findName(findPtr), nameSink);
                            String name = nameSink.toString();
                            try {
                                int segmentId = Numbers.parseInt(name);
                                if (segmentId >= 0) {
                                    segments.add(scanSegmentFiles(tableDir, walId, segmentId));
                                }
                            } catch (NumericException ignore) {
                                // not a numeric segment dir
                            }
                        }
                    } while (ff.findNext(findPtr) > 0);
                } finally {
                    ff.findClose(findPtr);
                }
            }
        }

        segments.sort(Comparator.comparingInt(WalSegmentEntry::segmentId));
        return segments;
    }

    private WalSegmentEntry scanSegmentFiles(CharSequence tableDir, int walId, int segmentId) {
        try (Path segPath = new Path()) {
            segPath.of(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).slash();
            int pathLen = segPath.size();

            boolean hasEvent = ff.exists(segPath.trimTo(pathLen).concat(WalUtils.EVENT_FILE_NAME).$());

            boolean hasEventIndex = ff.exists(segPath.trimTo(pathLen).concat(WalUtils.EVENT_INDEX_FILE_NAME).$());

            boolean hasMeta = ff.exists(segPath.trimTo(pathLen).concat(TableUtils.META_FILE_NAME).$());

            return new WalSegmentEntry(segmentId, hasEvent, hasEventIndex, hasMeta);
        }
    }
}
