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

import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Reads and parses a sequencer {@code _txnlog} file into {@link SeqTxnLogState}.
 * Supports both V1 (single-file, 28-byte records) and V2 (partitioned,
 * 60-byte records in {@code _txn_parts/} files).
 *
 * <p>Record count is capped by {@code maxRecords} to bound memory usage
 * on corrupt files. All reads are bounds-checked via {@link AbstractBoundedReader}.
 */
public class BoundedSeqTxnLogReader extends AbstractBoundedReader {
    public static final int DEFAULT_MAX_RECORDS = 100_000;
    private static final long V1_RECORD_SIZE = TableTransactionLogV1.RECORD_SIZE;
    private static final long V2_RECORD_SIZE = TableTransactionLogV2.RECORD_SIZE;
    private final int maxRecords;

    public BoundedSeqTxnLogReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_RECORDS);
    }

    public BoundedSeqTxnLogReader(FilesFacade ff, int maxRecords) {
        super(ff);
        this.maxRecords = Math.max(1, maxRecords);
    }

    public SeqTxnLogState read(LPSZ txnlogPath) {
        final SeqTxnLogState state = new SeqTxnLogState();
        final String pathStr = txnlogPath.asAsciiCharSequence().toString();
        state.setTxnlogPath(pathStr);

        final long fd = ff.openRO(txnlogPath);
        if (fd < 0) {
            addFileOpenFailure(ff, txnlogPath, pathStr, "_txnlog", state.getIssues());
            return state;
        }

        try {
            final long fileSize = ff.length(fd);
            if (fileSize < TableTransactionLogFile.HEADER_SIZE) {
                state.getIssues().add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.SHORT_FILE,
                        "_txnlog file is shorter than header [path=" + pathStr + ", size=" + fileSize + ']'
                ));
                return state;
            }

            long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                readTxnLog(fd, fileSize, scratch, state, pathStr);
            } finally {
                Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }

        return state;
    }

    public SeqTxnLogState readForTable(CharSequence dbRoot, DiscoveredTable table) {
        try (Path path = new Path()) {
            LPSZ primary = path.of(dbRoot).concat(table.getDirName()).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_FILE_NAME).$();
            if (ff.exists(primary)) {
                return read(primary);
            }
            // fall back to deprecated sequencer directory
            return read(path.of(dbRoot).concat(table.getDirName()).concat(WalUtils.SEQ_DIR_DEPRECATED).concat(WalUtils.TXNLOG_FILE_NAME).$());
        }
    }

    private void readTxnLog(long fd, long fileSize, long scratch, SeqTxnLogState state, String pathStr) {
        final ObjList<ReadIssue> issues = state.getIssues();
        int issuesBefore = issues.size();

        // read version (int at offset 0)
        final int version = readIntValue(fd, fileSize, scratch, issues, TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, "version");
        if (issues.size() > issuesBefore) {
            return;
        }
        state.setVersion(version);

        if (version != WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1 && version != WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.CORRUPT_SEQ_TXNLOG,
                    "unsupported _txnlog version [version=" + version + ", path=" + pathStr + ']'
            ));
            return;
        }

        // read maxTxn (long at offset 4)
        final long maxTxn = readLongValue(fd, fileSize, scratch, issues, TableTransactionLogFile.MAX_TXN_OFFSET_64, "maxTxn");
        if (issues.size() > issuesBefore) {
            return;
        }
        state.setMaxTxn(maxTxn);

        if (maxTxn < 0) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.CORRUPT_SEQ_TXNLOG,
                    "negative maxTxn [value=" + maxTxn + ", path=" + pathStr + ']'
            ));
            return;
        }

        // read createTimestamp (long at offset 12)
        final long createTimestamp = readLongValue(fd, fileSize, scratch, issues, TableTransactionLogFile.TABLE_CREATE_TIMESTAMP_OFFSET_64, "createTimestamp");
        if (issues.size() > issuesBefore) {
            return;
        }
        state.setCreateTimestamp(createTimestamp);

        // read partSize (int at offset 20)
        final int partSize = readIntValue(fd, fileSize, scratch, issues, TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32, "partSize");
        if (issues.size() > issuesBefore) {
            return;
        }
        state.setPartSize(partSize);

        if (maxTxn == 0) {
            return;
        }

        if (version == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1) {
            readV1Records(fd, fileSize, scratch, state, pathStr, maxTxn);
        } else {
            if (partSize <= 0) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.CORRUPT_SEQ_TXNLOG,
                        "V2 txnlog has non-positive partSize [value=" + partSize + ", path=" + pathStr + ']'
                ));
                return;
            }
            readV2Records(scratch, state, pathStr, maxTxn, partSize);
        }
    }

    private void readV1Records(long fd, long fileSize, long scratch, SeqTxnLogState state, String pathStr, long maxTxn) {
        final ObjList<ReadIssue> issues = state.getIssues();

        try {
            long expectedSize = Math.addExact(TableTransactionLogFile.HEADER_SIZE, Math.multiplyExact(maxTxn, V1_RECORD_SIZE));
            if (fileSize < expectedSize) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.WARN,
                        RecoveryIssueCode.SHORT_FILE,
                        "_txnlog file shorter than expected for " + maxTxn + " V1 records [expected="
                                + expectedSize + ", actual=" + fileSize + ", path=" + pathStr + ']'
                ));
            }
        } catch (ArithmeticException e) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.SHORT_FILE,
                    "_txnlog expected size overflows for maxTxn=" + maxTxn + " [path=" + pathStr + ']'
            ));
        }

        // cap by what the file can actually hold
        long maxRecordsInFile = (fileSize - TableTransactionLogFile.HEADER_SIZE) / V1_RECORD_SIZE;
        long availableInFile = Math.min(maxTxn, maxRecordsInFile);
        long recordCount = Math.min(availableInFile, maxRecords);

        if (maxTxn > maxRecords) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "record list capped to newest " + recordCount + " of " + maxTxn + " records"
            ));
        }

        // read from the tail so we always have the newest records — these
        // are the ones downstream needs for pending/referenced status
        long startIndex = availableInFile - recordCount;

        for (long j = 0; j < recordCount; j++) {
            long i = startIndex + j;
            long offset = TableTransactionLogFile.HEADER_SIZE + i * V1_RECORD_SIZE;
            int issuesBefore = issues.size();

            long structureVersion = readLongValue(fd, fileSize, scratch, issues, offset + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, "record.structureVersion");
            if (issues.size() > issuesBefore) return;

            int walId = readIntValue(fd, fileSize, scratch, issues, offset + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET, "record.walId");
            if (issues.size() > issuesBefore) return;

            int segmentId = readIntValue(fd, fileSize, scratch, issues, offset + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET, "record.segmentId");
            if (issues.size() > issuesBefore) return;

            int segmentTxn = readIntValue(fd, fileSize, scratch, issues, offset + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET, "record.segmentTxn");
            if (issues.size() > issuesBefore) return;

            long commitTimestamp = readLongValue(fd, fileSize, scratch, issues, offset + TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET, "record.commitTimestamp");
            if (issues.size() > issuesBefore) return;

            state.getRecords().add(new SeqTxnRecord(
                    i + 1, // txns are 1-based
                    structureVersion,
                    walId,
                    segmentId,
                    segmentTxn,
                    commitTimestamp,
                    TxnState.UNSET_LONG, // V1 has no minTimestamp
                    TxnState.UNSET_LONG, // V1 has no maxTimestamp
                    TxnState.UNSET_LONG  // V1 has no rowCount
            ));
        }
    }

    private void readV2Records(long scratch, SeqTxnLogState state, String pathStr, long maxTxn, int partSize) {
        final ObjList<ReadIssue> issues = state.getIssues();

        long recordCount = Math.min(maxTxn, maxRecords);
        if (maxTxn > maxRecords) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "record list capped to newest " + recordCount + " of " + maxTxn + " records"
            ));
        }

        // read from the tail so we always have the newest records
        long startIndex = maxTxn - recordCount;

        // strip the _txnlog filename to get the txn_seq directory
        // pathStr is: dbRoot/tableDirName/{txn_seq|seq}/_txnlog (constructed by readForTable)
        int lastSep = Math.max(pathStr.lastIndexOf('/'), pathStr.lastIndexOf('\\'));
        String baseDir = lastSep >= 0 ? pathStr.substring(0, lastSep) : pathStr;

        long lastOpenedPartId = -1;
        long partFd = -1;
        long partFileSize = -1;

        try {
            for (long j = 0; j < recordCount; j++) {
                long i = startIndex + j;
                long partId = i / partSize;
                long inPartOffset = (i % partSize) * V2_RECORD_SIZE;

                // open new part file if needed
                if (partId != lastOpenedPartId) {
                    if (partFd >= 0) {
                        ff.close(partFd);
                        partFd = -1;
                    }
                    try (Path partPath = new Path()) {
                        LPSZ lpsz = partPath.of(baseDir).concat(WalUtils.TXNLOG_PARTS_DIR).slash().put(partId).$();
                        partFd = ff.openRO(lpsz);
                        if (partFd < 0) {
                            addFileOpenFailure(ff, lpsz, partPath.toString(), "_txn_parts/" + partId, issues);
                            return;
                        }
                        partFileSize = ff.length(partFd);
                    }
                    lastOpenedPartId = partId;
                }

                int issuesBefore = issues.size();

                long structureVersion = readLongValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, "record.structureVersion");
                if (issues.size() > issuesBefore) return;

                int walId = readIntValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET, "record.walId");
                if (issues.size() > issuesBefore) return;

                int segmentId = readIntValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET, "record.segmentId");
                if (issues.size() > issuesBefore) return;

                int segmentTxn = readIntValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET, "record.segmentTxn");
                if (issues.size() > issuesBefore) return;

                long commitTimestamp = readLongValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET, "record.commitTimestamp");
                if (issues.size() > issuesBefore) return;

                long minTimestamp = readLongValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogV2.MIN_TIMESTAMP_OFFSET, "record.minTimestamp");
                if (issues.size() > issuesBefore) return;

                long maxTimestamp = readLongValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogV2.MAX_TIMESTAMP_OFFSET, "record.maxTimestamp");
                if (issues.size() > issuesBefore) return;

                long rowCount = readLongValue(partFd, partFileSize, scratch, issues, inPartOffset + TableTransactionLogV2.ROW_COUNT_OFFSET, "record.rowCount");
                if (issues.size() > issuesBefore) return;

                state.getRecords().add(new SeqTxnRecord(
                        i + 1, // txns are 1-based
                        structureVersion,
                        walId,
                        segmentId,
                        segmentTxn,
                        commitTimestamp,
                        minTimestamp,
                        maxTimestamp,
                        rowCount
                ));
            }
        } finally {
            if (partFd >= 0) {
                ff.close(partFd);
            }
        }
    }
}
