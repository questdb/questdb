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

import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Reads and parses a WAL segment's {@code _event} and {@code _event.i} files
 * into {@link WalEventState}. Supports all event types: {@code DATA},
 * {@code SQL}, {@code TRUNCATE}, {@code MAT_VIEW_DATA},
 * {@code MAT_VIEW_INVALIDATE}, and {@code VIEW_DEFINITION}.
 *
 * <p>Event count is capped by {@code maxEvents} to bound memory usage.
 * All reads are bounds-checked via {@link AbstractBoundedReader}.
 *
 * <p>Binary format:
 * <ul>
 *   <li>{@code _event} header (8 bytes): maxTxn (int @0), formatVersion (int @4)</li>
 *   <li>{@code _event.i}: array of (maxTxn + 2) longs — offset[txn] in _event file</li>
 *   <li>Record: length (int) + txn (long) + type (byte) + type-specific payload</li>
 * </ul>
 */
public class BoundedWalEventReader extends AbstractBoundedReader {
    public static final int DEFAULT_MAX_EVENTS = 10_000;
    private static final int MAX_SQL_CHARS = 1024;
    private static final int RECORD_PREFIX_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES; // length + txn + type
    private final int maxEvents;

    public BoundedWalEventReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_EVENTS);
    }

    public BoundedWalEventReader(FilesFacade ff, int maxEvents) {
        super(ff);
        this.maxEvents = Math.max(1, maxEvents);
    }

    public WalEventState read(LPSZ eventPath, LPSZ eventIndexPath) {
        final WalEventState state = new WalEventState();
        final String eventPathStr = eventPath.asAsciiCharSequence().toString();
        final String indexPathStr = eventIndexPath.asAsciiCharSequence().toString();
        state.setEventPath(eventPathStr);
        state.setEventIndexPath(indexPathStr);

        // open _event file
        final long eventFd = ff.openRO(eventPath);
        if (eventFd < 0) {
            addFileOpenFailure(ff, eventPath, eventPathStr, "_event", state.getIssues());
            return state;
        }

        try {
            final long eventFileSize = ff.length(eventFd);
            state.setEventFileSize(eventFileSize);

            if (eventFileSize < WalUtils.WALE_HEADER_SIZE) {
                state.getIssues().add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.SHORT_FILE,
                        "_event file is shorter than header [path=" + eventPathStr + ", size=" + eventFileSize + ']'
                ));
                return state;
            }

            long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                readEventFile(eventFd, eventFileSize, scratch, state, eventPathStr, indexPathStr, eventIndexPath);
            } finally {
                Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(eventFd);
        }

        return state;
    }

    public WalEventState readForSegment(CharSequence dbRoot, CharSequence tableDirName, int walId, int segmentId) {
        try (Path eventPath = new Path(); Path indexPath = new Path()) {
            eventPath.of(dbRoot).concat(tableDirName)
                    .concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).slash()
                    .concat(WalUtils.EVENT_FILE_NAME).$();
            indexPath.of(dbRoot).concat(tableDirName)
                    .concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).slash()
                    .concat(WalUtils.EVENT_INDEX_FILE_NAME).$();
            return read(eventPath.$(), indexPath.$());
        }
    }

    private void readEventFile(
            long eventFd, long eventFileSize, long scratch,
            WalEventState state, String eventPathStr, String indexPathStr, LPSZ eventIndexPath
    ) {
        final ObjList<ReadIssue> issues = state.getIssues();
        int issuesBefore = issues.size();

        // read maxTxn (int at offset 0)
        int maxTxn = readIntValue(eventFd, eventFileSize, scratch, issues, WalUtils.WALE_MAX_TXN_OFFSET_32, "maxTxn");
        if (issues.size() > issuesBefore) return;
        state.setMaxTxn(maxTxn);

        // read formatVersion (int at offset 4)
        int formatVersion = readIntValue(eventFd, eventFileSize, scratch, issues, WalUtils.WAL_FORMAT_OFFSET_32, "formatVersion");
        if (issues.size() > issuesBefore) return;
        state.setFormatVersion(formatVersion);

        short fmtLow = Numbers.decodeLowShort(formatVersion);
        if (fmtLow != WalUtils.WALE_FORMAT_VERSION
                && fmtLow != WalUtils.WALE_MAT_VIEW_FORMAT_VERSION
                && fmtLow != WalUtils.WALE_VIEW_FORMAT_VERSION) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.CORRUPT_WAL_EVENT,
                    "unrecognized _event format version [value=" + formatVersion
                            + ", lowShort=" + fmtLow + ", path=" + eventPathStr + ']'
            ));
        }

        if (maxTxn < -1) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.CORRUPT_WAL_EVENT,
                    "negative maxTxn [value=" + maxTxn + ", path=" + eventPathStr + ']'
            ));
            return;
        }

        if (maxTxn == -1) {
            return; // no events
        }

        // open _event.i file
        long indexFd = ff.openRO(eventIndexPath);
        if (indexFd < 0) {
            addFileOpenFailure(ff, eventIndexPath, indexPathStr, "_event.i", issues);
            return;
        }

        try {
            long indexFileSize = ff.length(indexFd);
            state.setEventIndexFileSize(indexFileSize);

            long expectedIndexSize = (maxTxn + 2L) * Long.BYTES;
            if (indexFileSize < expectedIndexSize) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.WARN,
                        RecoveryIssueCode.SHORT_FILE,
                        "_event.i shorter than expected [expected=" + expectedIndexSize
                                + ", actual=" + indexFileSize + ", maxTxn=" + maxTxn + ", path=" + indexPathStr + ']'
                ));
            }

            readEvents(eventFd, eventFileSize, indexFd, indexFileSize, scratch, state, eventPathStr, maxTxn);
        } finally {
            ff.close(indexFd);
        }
    }

    private void readEvents(
            long eventFd, long eventFileSize,
            long indexFd, long indexFileSize, long scratch,
            WalEventState state, String eventPathStr, int maxTxn
    ) {
        final ObjList<ReadIssue> issues = state.getIssues();

        // maxTxn is the max 0-based txn ID, so event count = maxTxn + 1
        int totalEvents = maxTxn + 1;
        int eventCount = Math.min(totalEvents, maxEvents);
        // also limit by what _event.i can hold: we need one long per event
        long maxEventsInIndex = indexFileSize / Long.BYTES;
        if (eventCount > maxEventsInIndex) {
            eventCount = (int) Math.max(0, maxEventsInIndex);
        }

        if (totalEvents > maxEvents) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "event list capped [requested=" + totalEvents + ", cap=" + maxEvents + ']'
            ));
        }

        for (int i = 0; i < eventCount; i++) {
            int issuesBefore = issues.size();

            // read offset from _event.i
            long recordOffset = readLongValue(indexFd, indexFileSize, scratch, issues, (long) i * Long.BYTES, "eventIndex[" + i + "]");
            if (issues.size() > issuesBefore) return;

            if (recordOffset < WalUtils.WALE_HEADER_SIZE || recordOffset >= eventFileSize) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.INVALID_OFFSET,
                        "event offset out of range [txn=" + i + ", offset=" + recordOffset
                                + ", eventFileSize=" + eventFileSize + ", path=" + eventPathStr + ']'
                ));
                return;
            }

            // read record prefix: length (int) + txn (long) + type (byte)
            int recordLength = readIntValue(eventFd, eventFileSize, scratch, issues, recordOffset, "event.length");
            if (issues.size() > issuesBefore) return;

            if (recordLength < RECORD_PREFIX_SIZE) {
                if (recordLength <= 0) {
                    // EOF marker (length <= 0 means no more records)
                    return;
                }
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.CORRUPT_WAL_EVENT,
                        "event record too short [txn=" + i + ", length=" + recordLength + ", path=" + eventPathStr + ']'
                ));
                return;
            }

            // check record fits within file
            if (recordOffset + recordLength > eventFileSize) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "event record extends beyond file [txn=" + i + ", offset=" + recordOffset
                                + ", length=" + recordLength + ", eventFileSize=" + eventFileSize + ']'
                ));
                return;
            }

            long txn = readLongValue(eventFd, eventFileSize, scratch, issues, recordOffset + Integer.BYTES, "event.txn");
            if (issues.size() > issuesBefore) return;

            byte type = readByteValue(eventFd, eventFileSize, scratch, issues, recordOffset + Integer.BYTES + Long.BYTES, "event.type");
            if (issues.size() > issuesBefore) return;

            long payloadOffset = recordOffset + RECORD_PREFIX_SIZE;
            // use record end as the read bound to avoid spilling into adjacent records
            long recordEnd = recordOffset + recordLength;
            readEventPayload(eventFd, recordEnd, scratch, state, issues, payloadOffset, txn, recordOffset, recordLength, type, i, eventPathStr);
        }
    }

    private void readEventPayload(
            long eventFd, long recordEnd, long scratch,
            WalEventState state, ObjList<ReadIssue> issues,
            long payloadOffset, long txn, long recordOffset, int recordLength,
            byte type, int expectedTxn, String eventPathStr
    ) {
        // cross-check: txn in record should match iteration index
        if (txn != expectedTxn) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.CORRUPT_WAL_EVENT,
                    "event txn mismatch [expected=" + expectedTxn + ", actual=" + txn + ", path=" + eventPathStr + ']'
            ));
        }

        switch (type) {
            case WalTxnType.DATA, WalTxnType.MAT_VIEW_DATA ->
                    readDataPayloadInto(eventFd, recordEnd, scratch, state, issues, payloadOffset, txn, recordOffset, recordLength, type);
            case WalTxnType.SQL ->
                    readSqlPayloadInto(eventFd, recordEnd, scratch, state, issues, payloadOffset, txn, recordOffset, recordLength, eventPathStr);
            case WalTxnType.TRUNCATE ->
                    state.getEvents().add(WalEventEntry.ofTruncate(txn, recordOffset, recordLength));
            case WalTxnType.MAT_VIEW_INVALIDATE ->
                    state.getEvents().add(WalEventEntry.ofMatViewInvalidate(txn, recordOffset, recordLength));
            case WalTxnType.VIEW_DEFINITION ->
                    readViewDefinitionPayloadInto(eventFd, recordEnd, scratch, state, issues, payloadOffset, txn, recordOffset, recordLength, eventPathStr);
            default ->
                    state.getEvents().add(WalEventEntry.ofUnknown(txn, type, recordOffset, recordLength));
        }
    }

    private void readDataPayloadInto(
            long eventFd, long recordEnd, long scratch,
            WalEventState state, ObjList<ReadIssue> issues,
            long payloadOffset, long txn, long recordOffset, int recordLength, byte type
    ) {
        int issuesBefore = issues.size();

        long startRowID = readLongValue(eventFd, recordEnd, scratch, issues, payloadOffset, "event.startRowID");
        if (issues.size() > issuesBefore) return;

        long endRowID = readLongValue(eventFd, recordEnd, scratch, issues, payloadOffset + Long.BYTES, "event.endRowID");
        if (issues.size() > issuesBefore) return;

        long minTimestamp = readLongValue(eventFd, recordEnd, scratch, issues, payloadOffset + 2 * Long.BYTES, "event.minTimestamp");
        if (issues.size() > issuesBefore) return;

        long maxTimestamp = readLongValue(eventFd, recordEnd, scratch, issues, payloadOffset + 3 * Long.BYTES, "event.maxTimestamp");
        if (issues.size() > issuesBefore) return;

        byte outOfOrderByte = readByteValue(eventFd, recordEnd, scratch, issues, payloadOffset + 4 * Long.BYTES, "event.outOfOrder");
        if (issues.size() > issuesBefore) return;

        boolean outOfOrder = outOfOrderByte != 0;

        if (type == WalTxnType.MAT_VIEW_DATA) {
            state.getEvents().add(WalEventEntry.ofMatViewData(
                    txn, recordOffset, recordLength,
                    startRowID, endRowID, minTimestamp, maxTimestamp, outOfOrder
            ));
        } else {
            state.getEvents().add(WalEventEntry.ofData(
                    txn, recordOffset, recordLength,
                    startRowID, endRowID, minTimestamp, maxTimestamp, outOfOrder
            ));
        }
    }

    private void readSqlPayloadInto(
            long eventFd, long recordEnd, long scratch,
            WalEventState state, ObjList<ReadIssue> issues,
            long payloadOffset, long txn, long recordOffset, int recordLength, String eventPathStr
    ) {
        int issuesBefore = issues.size();

        int cmdType = readIntValue(eventFd, recordEnd, scratch, issues, payloadOffset, "event.cmdType");
        if (issues.size() > issuesBefore) return;

        String sqlText = readSqlText(eventFd, recordEnd, scratch, issues, payloadOffset + Integer.BYTES, eventPathStr);

        state.getEvents().add(WalEventEntry.ofSql(txn, recordOffset, recordLength, cmdType, sqlText));
    }

    private String readSqlText(long eventFd, long recordEnd, long scratch, ObjList<ReadIssue> issues, long offset, String eventPathStr) {
        int issuesBefore = issues.size();

        int strLen = readIntValue(eventFd, recordEnd, scratch, issues, offset, "event.sqlLen");
        if (issues.size() > issuesBefore) return null;

        if (strLen <= 0) {
            return null;
        }

        long charOffset = offset + Integer.BYTES;
        long fullCharBytes = (long) strLen * Character.BYTES;
        if (!isRangeReadable(charOffset, fullCharBytes, recordEnd)) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.CORRUPT_WAL_EVENT,
                    "declared SQL length exceeds record bounds [strLen=" + strLen
                            + ", available=" + (recordEnd - charOffset) + ", path=" + eventPathStr + ']'
            ));
        }

        int cappedLen = Math.min(strLen, MAX_SQL_CHARS);
        long charBytes = (long) cappedLen * Character.BYTES;

        if (!isRangeReadable(charOffset, charBytes, recordEnd)) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.PARTIAL_READ,
                    "SQL text extends beyond file [strLen=" + strLen + ", path=" + eventPathStr + ']'
            ));
            return null;
        }

        long buf = Unsafe.malloc(charBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            long bytesRead = ff.read(eventFd, buf, charBytes, charOffset);
            if (bytesRead != charBytes) {
                issues.add(new ReadIssue(
                        RecoveryIssueSeverity.WARN,
                        RecoveryIssueCode.PARTIAL_READ,
                        "could not read SQL text [expected=" + charBytes + ", read=" + bytesRead + ']'
                ));
                return null;
            }

            StringBuilder sb = new StringBuilder(cappedLen);
            for (int i = 0; i < cappedLen; i++) {
                sb.append(Unsafe.getUnsafe().getChar(buf + (long) i * Character.BYTES));
            }
            if (strLen > MAX_SQL_CHARS) {
                sb.append("...(truncated)");
            }
            return sb.toString();
        } finally {
            Unsafe.free(buf, charBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void readViewDefinitionPayloadInto(
            long eventFd, long recordEnd, long scratch,
            WalEventState state, ObjList<ReadIssue> issues,
            long payloadOffset, long txn, long recordOffset, int recordLength, String eventPathStr
    ) {
        String viewSql = readSqlText(eventFd, recordEnd, scratch, issues, payloadOffset, eventPathStr);
        state.getEvents().add(WalEventEntry.ofViewDefinition(txn, recordOffset, recordLength, viewSql));
    }

}
