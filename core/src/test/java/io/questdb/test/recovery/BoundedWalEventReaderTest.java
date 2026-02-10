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

package io.questdb.test.recovery;

import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.recovery.BoundedWalEventReader;
import io.questdb.recovery.ReadIssue;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.TxnState;
import io.questdb.recovery.WalEventEntry;
import io.questdb.recovery.WalEventState;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BoundedWalEventReaderTest extends AbstractCairoTest {
    private static final int DATA_PAYLOAD_SIZE = 4 * Long.BYTES + Byte.BYTES; // 33
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final int RECORD_PREFIX_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES; // 13
    private static final int WALE_HEADER_SIZE = WalUtils.WALE_HEADER_SIZE; // 8

    @Test
    public void testSingleDataEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("single_data").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 100, 1000L, 2000L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(0, state.getMaxTxn());
            Assert.assertEquals(0, state.getFormatVersion());
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(0, e.getTxn());
            Assert.assertEquals(WalTxnType.DATA, e.getType());
            Assert.assertEquals("DATA", e.getTypeName());
            Assert.assertEquals(0, e.getStartRowID());
            Assert.assertEquals(100, e.getEndRowID());
            Assert.assertEquals(1000L, e.getMinTimestamp());
            Assert.assertEquals(2000L, e.getMaxTimestamp());
            Assert.assertFalse(e.isOutOfOrder());
            Assert.assertEquals(WALE_HEADER_SIZE, e.getRawOffset());
            Assert.assertEquals(record.length, e.getRawLength());
        });
    }
    @Test
    public void testMultipleDataEvents() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("multi_data").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildDataRecord(1, 10, 20, 300L, 400L, true);
            byte[] r2 = buildDataRecord(2, 20, 30, 500L, 600L, false);

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long off2 = off1 + r1.length;
            long end = off2 + r2.length;

            writeEventFile(dir, 3, 0, r0, r1, r2);
            writeFullEventIndex(dir, 3, off0, off1, off2, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(2, state.getMaxTxn());
            Assert.assertEquals(3, state.getEvents().size());

            WalEventEntry e0 = state.getEvents().getQuick(0);
            Assert.assertEquals(0, e0.getTxn());
            Assert.assertEquals(0, e0.getStartRowID());
            Assert.assertEquals(10, e0.getEndRowID());
            Assert.assertFalse(e0.isOutOfOrder());

            WalEventEntry e1 = state.getEvents().getQuick(1);
            Assert.assertEquals(1, e1.getTxn());
            Assert.assertEquals(10, e1.getStartRowID());
            Assert.assertEquals(20, e1.getEndRowID());
            Assert.assertTrue(e1.isOutOfOrder());

            WalEventEntry e2 = state.getEvents().getQuick(2);
            Assert.assertEquals(2, e2.getTxn());
            Assert.assertEquals(500L, e2.getMinTimestamp());
            Assert.assertEquals(600L, e2.getMaxTimestamp());
        });
    }
    @Test
    public void testSqlEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_event").getAbsolutePath();
            byte[] record = buildSqlRecord(0, 42, "ALTER TABLE t ADD COLUMN x INT");
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WalTxnType.SQL, e.getType());
            Assert.assertEquals("SQL", e.getTypeName());
            Assert.assertEquals(42, e.getCmdType());
            Assert.assertEquals("ALTER TABLE t ADD COLUMN x INT", e.getSqlText());
            Assert.assertEquals(TxnState.UNSET_LONG, e.getStartRowID());
            Assert.assertEquals(TxnState.UNSET_LONG, e.getEndRowID());
        });
    }
    @Test
    public void testTruncateEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("truncate_event").getAbsolutePath();
            byte[] record = buildTruncateRecord(0);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WalTxnType.TRUNCATE, e.getType());
            Assert.assertEquals("TRUNCATE", e.getTypeName());
            Assert.assertEquals(0, e.getTxn());
            Assert.assertEquals(TxnState.UNSET_LONG, e.getStartRowID());
            Assert.assertEquals(TxnState.UNSET_INT, e.getCmdType());
        });
    }
    @Test
    public void testMixedEvents() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("mixed_events").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 50, 1000L, 2000L, false);
            byte[] r1 = buildSqlRecord(1, 7, "DROP INDEX");
            byte[] r2 = buildTruncateRecord(2);

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long off2 = off1 + r1.length;
            long end = off2 + r2.length;

            writeEventFile(dir, 3, 0, r0, r1, r2);
            writeFullEventIndex(dir, 3, off0, off1, off2, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(3, state.getEvents().size());
            Assert.assertEquals("DATA", state.getEvents().getQuick(0).getTypeName());
            Assert.assertEquals("SQL", state.getEvents().getQuick(1).getTypeName());
            Assert.assertEquals("TRUNCATE", state.getEvents().getQuick(2).getTypeName());
        });
    }
    @Test
    public void testMatViewDataEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("mat_view_data").getAbsolutePath();
            byte[] record = buildRecordWithType(0, WalTxnType.MAT_VIEW_DATA, buildDataPayload(0, 100, 1000L, 2000L, true));
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WalTxnType.MAT_VIEW_DATA, e.getType());
            Assert.assertEquals("MAT_VIEW_DATA", e.getTypeName());
            Assert.assertEquals(0, e.getStartRowID());
            Assert.assertEquals(100, e.getEndRowID());
            Assert.assertEquals(1000L, e.getMinTimestamp());
            Assert.assertEquals(2000L, e.getMaxTimestamp());
            Assert.assertTrue(e.isOutOfOrder());
        });
    }
    @Test
    public void testMatViewInvalidateEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("mat_view_inv").getAbsolutePath();
            byte[] record = buildRecordWithType(0, WalTxnType.MAT_VIEW_INVALIDATE, new byte[0]);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WalTxnType.MAT_VIEW_INVALIDATE, e.getType());
            Assert.assertEquals("MAT_VIEW_INVALIDATE", e.getTypeName());
            Assert.assertEquals(TxnState.UNSET_LONG, e.getStartRowID());
        });
    }
    @Test
    public void testViewDefinitionEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("view_def").getAbsolutePath();
            String viewSql = "SELECT * FROM base_table";
            byte[] record = buildViewDefinitionRecord(0, viewSql);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WalTxnType.VIEW_DEFINITION, e.getType());
            Assert.assertEquals("VIEW_DEFINITION", e.getTypeName());
            Assert.assertEquals(viewSql, e.getSqlText());
        });
    }
    @Test
    public void testUnknownEventType() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("unknown_type").getAbsolutePath();
            byte[] record = buildRecordWithType(0, (byte) 127, new byte[0]);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(127, e.getType());
            Assert.assertEquals("UNKNOWN(127)", e.getTypeName());
        });
    }
    @Test
    public void testReadForSegment() throws Exception {
        assertMemoryLeak(() -> {
            String dbRoot = temp.newFolder("db_root").getAbsolutePath();
            String tableDirName = "my_table~1";
            int walId = 1;
            int segmentId = 0;

            File segDir = new File(dbRoot, tableDirName + "/wal" + walId + "/" + segmentId);
            Assert.assertTrue(segDir.mkdirs());

            byte[] record = buildDataRecord(0, 0, 50, 1000L, 2000L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(segDir.getAbsolutePath(), 1, 0, record);
            writeFullEventIndex(segDir.getAbsolutePath(), 1, WALE_HEADER_SIZE, end);

            BoundedWalEventReader reader = new BoundedWalEventReader(FF);
            WalEventState state = reader.readForSegment(dbRoot, tableDirName, walId, segmentId);

            assertNoIssues(state);
            Assert.assertEquals(0, state.getMaxTxn());
            Assert.assertEquals(1, state.getEvents().size());
            Assert.assertEquals("DATA", state.getEvents().getQuick(0).getTypeName());
            Assert.assertTrue(state.getEventPath().contains("wal1"));
            Assert.assertTrue(state.getEventPath().contains(WalUtils.EVENT_FILE_NAME));
            Assert.assertTrue(state.getEventIndexPath().contains(WalUtils.EVENT_INDEX_FILE_NAME));
        });
    }
    @Test
    public void testSqlWithZeroLengthText() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_zero_len").getAbsolutePath();
            byte[] payload = new byte[Integer.BYTES + Integer.BYTES];
            ByteBuffer pb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            pb.putInt(99); // cmdType
            pb.putInt(0);  // sqlLen = 0
            byte[] record = buildRecordWithType(0, WalTxnType.SQL, payload);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(99, e.getCmdType());
            Assert.assertNull(e.getSqlText());
        });
    }
    @Test
    public void testSqlWithNegativeLengthText() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_neg_len").getAbsolutePath();
            byte[] payload = new byte[Integer.BYTES + Integer.BYTES];
            ByteBuffer pb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            pb.putInt(5); // cmdType
            pb.putInt(-1); // sqlLen = -1
            byte[] record = buildRecordWithType(0, WalTxnType.SQL, payload);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());
            Assert.assertNull(state.getEvents().getQuick(0).getSqlText());
        });
    }
    @Test
    public void testSqlWithLongTextTruncation() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_long").getAbsolutePath();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 2000; i++) {
                sb.append('A');
            }
            String longSql = sb.toString();
            byte[] record = buildSqlRecord(0, 1, longSql);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());

            String sqlText = state.getEvents().getQuick(0).getSqlText();
            Assert.assertNotNull(sqlText);
            Assert.assertTrue(sqlText.endsWith("...(truncated)"));
            Assert.assertEquals(1024, sqlText.indexOf("...(truncated)"));
        });
    }
    @Test
    public void testEmptySegment() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("empty_seg").getAbsolutePath();
            writeEventFile(dir, 0, 0);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(-1, state.getMaxTxn());
            Assert.assertEquals(0, state.getEvents().size());
        });
    }
    @Test
    public void testTruncatedEventHeader() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("trunc_hdr").getAbsolutePath();
            ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(0);
            writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            Assert.assertTrue(hasIssueContaining(state, "_event file is shorter than header"));
        });
    }
    @Test
    public void testMissingEventFile() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("missing_event").getAbsolutePath();

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
        });
    }
    @Test
    public void testMissingEventIndexFile() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("missing_idx").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            writeEventFile(dir, 1, 0, record);
            // do NOT write _event.i

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
        });
    }
    @Test
    public void testNegativeMaxTxnCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("neg_maxtxn").getAbsolutePath();
            ByteBuffer buf = ByteBuffer.allocate(WALE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(-2); // maxTxn = -2 is corrupt (< -1)
            buf.putInt(0);  // formatVersion
            writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_WAL_EVENT));
            Assert.assertTrue(hasIssueContaining(state, "negative maxTxn"));
        });
    }

    @Test
    public void testNegativeMaxTxnMinusOneIsEmpty() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("empty_maxtxn").getAbsolutePath();
            ByteBuffer buf = ByteBuffer.allocate(WALE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(-1); // maxTxn = -1 means no events (valid)
            buf.putInt(0);  // formatVersion
            writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());

            WalEventState state = readEvents(dir);
            Assert.assertFalse(hasIssue(state, RecoveryIssueCode.CORRUPT_WAL_EVENT));
            Assert.assertEquals(-1, state.getMaxTxn());
            Assert.assertEquals(0, state.getEvents().size());
        });
    }
    @Test
    public void testInvalidOffsetBelowHeader() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("invalid_off").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            writeEventFile(dir, 1, 0, record);
            // offset 2 is less than WALE_HEADER_SIZE=8
            writeFullEventIndex(dir, 1, 2L, (long) WALE_HEADER_SIZE + record.length);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_OFFSET));
            Assert.assertTrue(hasIssueContaining(state, "event offset out of range"));
        });
    }

    @Test
    public void testInvalidOffsetBeyondFileSize() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("off_beyond").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            int eventFileSize = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            // offset points beyond end of event file
            writeFullEventIndex(dir, 1, (long) eventFileSize + 100, (long) eventFileSize + 200);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_OFFSET));
        });
    }
    @Test
    public void testRecordLengthOverflow() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("rec_overflow").getAbsolutePath();
            int realLen = RECORD_PREFIX_SIZE + DATA_PAYLOAD_SIZE;
            ByteBuffer buf = ByteBuffer.allocate(realLen).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(realLen + 1000); // length claims more than file contains
            buf.putLong(0L); // txn
            buf.put(WalTxnType.DATA);
            buf.putLong(0L); // startRowID
            buf.putLong(10L); // endRowID
            buf.putLong(100L); // minTs
            buf.putLong(200L); // maxTs
            buf.put((byte) 0); // ooo

            writeEventFile(dir, 1, 0, buf.array());
            writeFullEventIndex(dir, 1, (long) WALE_HEADER_SIZE, (long) WALE_HEADER_SIZE + realLen);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            Assert.assertTrue(hasIssueContaining(state, "event record extends beyond file"));
        });
    }
    @Test
    public void testRecordTooShort() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("rec_short").getAbsolutePath();
            ByteBuffer recBuf = ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN);
            recBuf.putInt(5); // length = 5, too short
            recBuf.putLong(0L); // txn
            recBuf.put(WalTxnType.DATA); // type

            writeEventFile(dir, 1, 0, recBuf.array());
            writeFullEventIndex(dir, 1, (long) WALE_HEADER_SIZE, (long) WALE_HEADER_SIZE + 13);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_WAL_EVENT));
            Assert.assertTrue(hasIssueContaining(state, "event record too short"));
        });
    }
    @Test
    public void testEofMarkerStopsReading() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("eof_marker").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            ByteBuffer eofBuf = ByteBuffer.allocate(RECORD_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            eofBuf.putInt(0); // length = 0 -> EOF
            eofBuf.putLong(1L); // txn
            eofBuf.put(WalTxnType.DATA);
            byte[] r1 = eofBuf.array();

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long end = off1 + r1.length;

            writeEventFile(dir, 2, 0, r0, r1);
            writeFullEventIndex(dir, 2, off0, off1, end);

            WalEventState state = readEvents(dir);
            Assert.assertEquals(1, state.getEvents().size());
            Assert.assertEquals("DATA", state.getEvents().getQuick(0).getTypeName());
        });
    }

    @Test
    public void testNegativeLengthEofMarker() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("neg_eof").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            ByteBuffer eofBuf = ByteBuffer.allocate(RECORD_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            eofBuf.putInt(-1); // length = -1 -> EOF
            eofBuf.putLong(1L);
            eofBuf.put(WalTxnType.DATA);
            byte[] r1 = eofBuf.array();

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long end = off1 + r1.length;

            writeEventFile(dir, 2, 0, r0, r1);
            writeFullEventIndex(dir, 2, off0, off1, end);

            WalEventState state = readEvents(dir);
            Assert.assertEquals(1, state.getEvents().size());
        });
    }
    @Test
    public void testTxnMismatchWarning() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("txn_mismatch").getAbsolutePath();
            // record's txn=99 but it's at index 0, so expected txn=0
            byte[] record = buildDataRecord(99, 0, 10, 100L, 200L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_WAL_EVENT));
            Assert.assertTrue(hasIssueContaining(state, "event txn mismatch"));
            // event should still be read
            Assert.assertEquals(1, state.getEvents().size());
        });
    }
    @Test
    public void testTruncatedEventIndex() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("trunc_idx").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildDataRecord(1, 10, 20, 300L, 400L, false);

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;

            writeEventFile(dir, 3, 0, r0, r1);
            // only 2 index entries instead of expected eventCount+1=4
            writeRawEventIndex(dir, off0, off1);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            Assert.assertTrue(hasIssueContaining(state, "_event.i shorter than expected"));
            // 2 index entries -> 2 events can be read
            Assert.assertEquals(2, state.getEvents().size());
        });
    }
    @Test
    public void testMaxEventsCap() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("max_cap").getAbsolutePath();
            byte[][] records = new byte[5][];
            long[] offsets = new long[6]; // 5 records + 1 end offset
            long offset = WALE_HEADER_SIZE;
            for (int i = 0; i < 5; i++) {
                records[i] = buildDataRecord(i, i * 10, (i + 1) * 10, i * 100L, (i + 1) * 100L, false);
                offsets[i] = offset;
                offset += records[i].length;
            }
            offsets[5] = offset;

            writeEventFile(dir, 5, 0, records);
            // write full index: eventCount+1 = 6 entries
            writeFullEventIndex(dir, 5, offsets);

            BoundedWalEventReader reader = new BoundedWalEventReader(FF, 2);
            WalEventState state = readEventsWithReader(dir, reader);
            Assert.assertEquals(2, state.getEvents().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
            Assert.assertTrue(hasIssueContaining(state, "event list capped"));
        });
    }
    @Test
    public void testMaxEventsCapWithTruncatedIndex() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("max_trunc_idx").getAbsolutePath();
            // build 10 records
            byte[][] records = new byte[10][];
            long[] offsets = new long[11];
            long offset = WALE_HEADER_SIZE;
            for (int i = 0; i < 10; i++) {
                records[i] = buildDataRecord(i, i * 10, (i + 1) * 10, i * 100L, (i + 1) * 100L, false);
                offsets[i] = offset;
                offset += records[i].length;
            }
            offsets[10] = offset;

            // eventCount=10, but write only 3 index entries (enough for 3 events)
            writeEventFile(dir, 10, 0, records);
            writeRawEventIndex(dir, offsets[0], offsets[1], offsets[2]);

            // maxEvents=5 is higher than what the truncated index can serve (3)
            BoundedWalEventReader reader = new BoundedWalEventReader(FF, 5);
            WalEventState state = readEventsWithReader(dir, reader);

            // only 3 events should be read (limited by truncated index with 3 entries, not maxEvents)
            Assert.assertEquals(3, state.getEvents().size());
            // should have SHORT_FILE warning for truncated index
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            // should also have TRUNCATED_OUTPUT warning since maxTxn(10) > maxEvents(5)
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }
    @Test
    public void testDataWithOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("ooo_data").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 100, 5000L, 10000L, true);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertTrue(state.getEvents().getQuick(0).isOutOfOrder());
        });
    }
    @Test
    public void testMultipleSegmentsViaReadForSegment() throws Exception {
        assertMemoryLeak(() -> {
            String dbRoot = temp.newFolder("db_multi_seg").getAbsolutePath();
            String tableDirName = "table_x~1";

            for (int seg = 0; seg < 3; seg++) {
                File segDir = new File(dbRoot, tableDirName + "/wal1/" + seg);
                Assert.assertTrue(segDir.mkdirs());
                byte[] record = buildDataRecord(0, seg * 100, (seg + 1) * 100, seg * 1000L, (seg + 1) * 1000L, false);
                long end = WALE_HEADER_SIZE + record.length;
                writeEventFile(segDir.getAbsolutePath(), 1, 0, record);
                writeFullEventIndex(segDir.getAbsolutePath(), 1, WALE_HEADER_SIZE, end);
            }

            BoundedWalEventReader reader = new BoundedWalEventReader(FF);
            for (int seg = 0; seg < 3; seg++) {
                WalEventState state = reader.readForSegment(dbRoot, tableDirName, 1, seg);
                assertNoIssues(state);
                Assert.assertEquals(1, state.getEvents().size());
                Assert.assertEquals(seg * 100, state.getEvents().getQuick(0).getStartRowID());
                Assert.assertEquals((seg + 1) * 100, state.getEvents().getQuick(0).getEndRowID());
            }
        });
    }
    @Test
    public void testFormatVersionPreserved() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("fmt_ver").getAbsolutePath();
            writeEventFile(dir, 0, 42);

            WalEventState state = readEvents(dir);
            Assert.assertEquals(42, state.getFormatVersion());
        });
    }
    @Test
    public void testEventFileSizePreserved() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("file_size").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            Assert.assertEquals(WALE_HEADER_SIZE + record.length, state.getEventFileSize());
        });
    }
    @Test
    public void testEventIndexFileSizePreserved() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("idx_size").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            // eventCount=1 -> on-disk maxTxn=0 -> (0+2)*8 = 16 bytes
            Assert.assertEquals((1 + 1) * Long.BYTES, state.getEventIndexFileSize());
        });
    }
    @Test
    public void testEventPathsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("paths").getAbsolutePath();
            byte[] record = buildTruncateRecord(0);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            Assert.assertNotNull(state.getEventPath());
            Assert.assertTrue(state.getEventPath().contains(WalUtils.EVENT_FILE_NAME));
            Assert.assertNotNull(state.getEventIndexPath());
            Assert.assertTrue(state.getEventIndexPath().contains(WalUtils.EVENT_INDEX_FILE_NAME));
        });
    }
    @Test
    public void testDataWithZeroRows() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("zero_rows").getAbsolutePath();
            byte[] record = buildDataRecord(0, 50, 50, 1000L, 1000L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());
            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(50, e.getStartRowID());
            Assert.assertEquals(50, e.getEndRowID());
        });
    }
    @Test
    public void testLargeMaxTxnWithSmallFile() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("large_maxtxn").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildDataRecord(1, 10, 20, 300L, 400L, false);
            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long end = off1 + r1.length;

            // maxTxn says 1000 but only 2 records in the file
            writeEventFile(dir, 1000, 0, r0, r1);
            // only 3 index entries (enough for 2 records but not maxTxn+2=1002)
            writeRawEventIndex(dir, off0, off1, end);

            WalEventState state = readEvents(dir);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            Assert.assertEquals(2, state.getEvents().size());
        });
    }
    @Test
    public void testMaxEventsClampedToOne() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("clamp_one").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildDataRecord(1, 10, 20, 300L, 400L, false);
            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long end = off1 + r1.length;

            writeEventFile(dir, 2, 0, r0, r1);
            writeFullEventIndex(dir, 2, off0, off1, end);

            BoundedWalEventReader reader = new BoundedWalEventReader(FF, 0);
            WalEventState state = readEventsWithReader(dir, reader);
            Assert.assertEquals(1, state.getEvents().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }
    @Test
    public void testDefaultConstructor() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("default_ctor").getAbsolutePath();
            byte[] record = buildDataRecord(0, 0, 10, 100L, 200L, false);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            BoundedWalEventReader reader = new BoundedWalEventReader(FF);
            WalEventState state = readEventsWithReader(dir, reader);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());
        });
    }
    @Test
    public void testMaxTxnStoredOnError() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("maxtxn_err").getAbsolutePath();
            ByteBuffer buf = ByteBuffer.allocate(WALE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(-5); // negative maxTxn
            buf.putInt(0);
            writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());

            WalEventState state = readEvents(dir);
            Assert.assertEquals(-5, state.getMaxTxn());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_WAL_EVENT));
        });
    }
    @Test
    public void testFormatVersionStoredOnNegativeMaxTxn() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("fmt_neg").getAbsolutePath();
            ByteBuffer buf = ByteBuffer.allocate(WALE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(-1); // maxTxn
            buf.putInt(77); // formatVersion
            writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());

            WalEventState state = readEvents(dir);
            Assert.assertEquals(77, state.getFormatVersion());
            Assert.assertEquals(-1, state.getMaxTxn());
        });
    }
    @Test
    public void testAllEventTypesInOneBatch() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("all_types").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildSqlRecord(1, 3, "UPDATE t SET x=1");
            byte[] r2 = buildTruncateRecord(2);
            byte[] r3 = buildRecordWithType(3, WalTxnType.MAT_VIEW_DATA, buildDataPayload(0, 50, 500L, 600L, false));
            byte[] r4 = buildRecordWithType(4, WalTxnType.MAT_VIEW_INVALIDATE, new byte[0]);
            byte[] r5 = buildViewDefinitionRecord(5, "SELECT 1");
            byte[] r6 = buildRecordWithType(6, (byte) 99, new byte[0]);

            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long off2 = off1 + r1.length;
            long off3 = off2 + r2.length;
            long off4 = off3 + r3.length;
            long off5 = off4 + r4.length;
            long off6 = off5 + r5.length;
            long end = off6 + r6.length;

            writeEventFile(dir, 7, 0, r0, r1, r2, r3, r4, r5, r6);
            writeFullEventIndex(dir, 7, off0, off1, off2, off3, off4, off5, off6, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(7, state.getEvents().size());

            Assert.assertEquals("DATA", state.getEvents().getQuick(0).getTypeName());
            Assert.assertEquals("SQL", state.getEvents().getQuick(1).getTypeName());
            Assert.assertEquals("TRUNCATE", state.getEvents().getQuick(2).getTypeName());
            Assert.assertEquals("MAT_VIEW_DATA", state.getEvents().getQuick(3).getTypeName());
            Assert.assertEquals("MAT_VIEW_INVALIDATE", state.getEvents().getQuick(4).getTypeName());
            Assert.assertEquals("VIEW_DEFINITION", state.getEvents().getQuick(5).getTypeName());
            Assert.assertEquals("UNKNOWN(99)", state.getEvents().getQuick(6).getTypeName());
        });
    }
    @Test
    public void testRawOffsetAndLengthForSqlEvent() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_offset").getAbsolutePath();
            byte[] record = buildSqlRecord(0, 1, "X");
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertEquals(WALE_HEADER_SIZE, e.getRawOffset());
            Assert.assertEquals(record.length, e.getRawLength());
        });
    }
    @Test
    public void testViewDefinitionWithNullViewSql() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("view_null").getAbsolutePath();
            byte[] payload = new byte[Integer.BYTES];
            ByteBuffer pb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            pb.putInt(-1); // sqlLen = -1
            byte[] record = buildRecordWithType(0, WalTxnType.VIEW_DEFINITION, payload);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            Assert.assertEquals(1, state.getEvents().size());
            Assert.assertNull(state.getEvents().getQuick(0).getSqlText());
        });
    }
    @Test
    public void testUnsetStateWhenEventFileMissing() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("unset_state").getAbsolutePath();
            WalEventState state = readEvents(dir);
            Assert.assertEquals(TxnState.UNSET_INT, state.getMaxTxn());
            Assert.assertEquals(TxnState.UNSET_INT, state.getFormatVersion());
            Assert.assertEquals(-1, state.getEventFileSize());
            Assert.assertEquals(-1, state.getEventIndexFileSize());
            Assert.assertEquals(0, state.getEvents().size());
        });
    }
    @Test
    public void testSqlWithExact1024Chars() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_1024").getAbsolutePath();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1024; i++) {
                sb.append('B');
            }
            String sql1024 = sb.toString();
            byte[] record = buildSqlRecord(0, 1, sql1024);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            String sqlText = state.getEvents().getQuick(0).getSqlText();
            Assert.assertNotNull(sqlText);
            Assert.assertEquals(1024, sqlText.length());
            Assert.assertFalse(sqlText.contains("truncated"));
        });
    }
    @Test
    public void testSqlTextExtendsBeyondFile() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("sql_beyond").getAbsolutePath();
            // Build a SQL record where strLen claims 100 chars but the _event file
            // is truncated so the char data does not fit.
            int claimedStrLen = 100;
            // payload: cmdType(4) + sqlLen(4) — but NO actual char data follows
            byte[] payload = new byte[Integer.BYTES + Integer.BYTES];
            ByteBuffer pb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            pb.putInt(1); // cmdType
            pb.putInt(claimedStrLen); // sqlLen = 100, but file has no room for chars
            byte[] record = buildRecordWithType(0, WalTxnType.SQL, payload);
            long end = WALE_HEADER_SIZE + record.length;
            writeEventFile(dir, 1, 0, record);
            writeFullEventIndex(dir, 1, WALE_HEADER_SIZE, end);

            WalEventState state = readEvents(dir);
            Assert.assertEquals(1, state.getEvents().size());

            // sqlText should be null because the chars extend beyond the file
            WalEventEntry e = state.getEvents().getQuick(0);
            Assert.assertNull(e.getSqlText());

            // should have a PARTIAL_READ warning about SQL text extending beyond file
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            Assert.assertTrue(hasIssueContaining(state, "SQL text extends beyond file"));
        });
    }
    @Test
    public void testTxnFieldMatchesExpected() throws Exception {
        assertMemoryLeak(() -> {
            String dir = temp.newFolder("txn_match").getAbsolutePath();
            byte[] r0 = buildDataRecord(0, 0, 10, 100L, 200L, false);
            byte[] r1 = buildDataRecord(1, 10, 20, 300L, 400L, false);
            byte[] r2 = buildDataRecord(2, 20, 30, 500L, 600L, false);
            long off0 = WALE_HEADER_SIZE;
            long off1 = off0 + r0.length;
            long off2 = off1 + r1.length;
            long end = off2 + r2.length;

            writeEventFile(dir, 3, 0, r0, r1, r2);
            writeFullEventIndex(dir, 3, off0, off1, off2, end);

            WalEventState state = readEvents(dir);
            assertNoIssues(state);
            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(i, state.getEvents().getQuick(i).getTxn());
            }
        });
    }

    @Test
    public void testRecordLengthBoundsPayloadReads() throws Exception {
        assertMemoryLeak(() -> {
            // Craft a DATA record where recordLength = RECORD_PREFIX_SIZE (13 bytes).
            // This claims to be a DATA event but leaves no room for any payload fields.
            // A second valid DATA record follows immediately after.
            // Before the fix, reads would spill into the second record's bytes.
            // After the fix, the reader should detect the out-of-bounds read and add an issue.
            String dir = temp.newFolder("record_bounds").getAbsolutePath();

            // record 0: DATA type with recordLength = RECORD_PREFIX_SIZE only (no payload)
            ByteBuffer malformed = ByteBuffer.allocate(RECORD_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            malformed.putInt(RECORD_PREFIX_SIZE); // recordLength too small for DATA
            malformed.putLong(0); // txn=0
            malformed.put(WalTxnType.DATA); // type=DATA
            byte[] malformedRecord = malformed.array();

            // record 1: valid DATA record
            byte[] validRecord = buildDataRecord(1, 0, 50, 3000L, 4000L, false);

            long offset0 = WALE_HEADER_SIZE;
            long offset1 = offset0 + malformedRecord.length;
            long end = offset1 + validRecord.length;

            writeEventFile(dir, 2, 0, malformedRecord, validRecord);
            writeFullEventIndex(dir, 2, offset0, offset1, end);

            WalEventState state = readEvents(dir);
            // should have at least one issue about the out-of-bounds payload read
            Assert.assertTrue("should have issues from malformed record",
                    state.getIssues().size() > 0);
            // the malformed DATA record should NOT produce a valid event with
            // fields from the adjacent record
            for (int i = 0, n = state.getEvents().size(); i < n; i++) {
                WalEventEntry e = state.getEvents().getQuick(i);
                if (e.getTxn() == 0 && e.getType() == WalTxnType.DATA) {
                    Assert.fail("malformed record should not produce a valid DATA event");
                }
            }
        });
    }

    @Test
    public void testRecordLengthBoundsSqlPayload() throws Exception {
        assertMemoryLeak(() -> {
            // Craft a SQL record where recordLength = RECORD_PREFIX_SIZE (no room for cmdType).
            String dir = temp.newFolder("record_bounds_sql").getAbsolutePath();

            ByteBuffer malformed = ByteBuffer.allocate(RECORD_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            malformed.putInt(RECORD_PREFIX_SIZE);
            malformed.putLong(0); // txn=0
            malformed.put(WalTxnType.SQL);
            byte[] malformedRecord = malformed.array();

            writeEventFile(dir, 1, 0, malformedRecord);
            writeFullEventIndex(dir, 1, (long) WALE_HEADER_SIZE, (long) WALE_HEADER_SIZE + malformedRecord.length);

            WalEventState state = readEvents(dir);
            Assert.assertTrue("should have issues from truncated SQL record",
                    state.getIssues().size() > 0);
        });
    }

    private static void assertNoIssues(WalEventState state) {
        Assert.assertEquals("issues: " + dumpIssues(state), 0, state.getIssues().size());
    }

    private static byte[] buildDataPayload(long startRowID, long endRowID, long minTs, long maxTs, boolean ooo) {
        ByteBuffer buf = ByteBuffer.allocate(DATA_PAYLOAD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(startRowID);
        buf.putLong(endRowID);
        buf.putLong(minTs);
        buf.putLong(maxTs);
        buf.put((byte) (ooo ? 1 : 0));
        return buf.array();
    }

    private static byte[] buildDataRecord(long txn, long startRowID, long endRowID, long minTs, long maxTs, boolean ooo) {
        int totalLen = RECORD_PREFIX_SIZE + DATA_PAYLOAD_SIZE;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(totalLen);
        buf.putLong(txn);
        buf.put(WalTxnType.DATA);
        buf.putLong(startRowID);
        buf.putLong(endRowID);
        buf.putLong(minTs);
        buf.putLong(maxTs);
        buf.put((byte) (ooo ? 1 : 0));
        return buf.array();
    }

    private static byte[] buildRecordWithType(long txn, byte type, byte[] payload) {
        int totalLen = RECORD_PREFIX_SIZE + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(totalLen);
        buf.putLong(txn);
        buf.put(type);
        buf.put(payload);
        return buf.array();
    }

    private static byte[] buildSqlRecord(long txn, int cmdType, String sql) {
        int strBytes = sql.length() * Character.BYTES;
        int payloadSize = Integer.BYTES + Integer.BYTES + strBytes;
        int totalLen = RECORD_PREFIX_SIZE + payloadSize;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(totalLen);
        buf.putLong(txn);
        buf.put(WalTxnType.SQL);
        buf.putInt(cmdType);
        buf.putInt(sql.length());
        for (int i = 0; i < sql.length(); i++) {
            buf.putChar(sql.charAt(i));
        }
        return buf.array();
    }

    private static byte[] buildTruncateRecord(long txn) {
        int totalLen = RECORD_PREFIX_SIZE;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(totalLen);
        buf.putLong(txn);
        buf.put(WalTxnType.TRUNCATE);
        return buf.array();
    }

    private static byte[] buildViewDefinitionRecord(long txn, String viewSql) {
        int strBytes = viewSql.length() * Character.BYTES;
        int payloadSize = Integer.BYTES + strBytes;
        int totalLen = RECORD_PREFIX_SIZE + payloadSize;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(totalLen);
        buf.putLong(txn);
        buf.put(WalTxnType.VIEW_DEFINITION);
        buf.putInt(viewSql.length());
        for (int i = 0; i < viewSql.length(); i++) {
            buf.putChar(viewSql.charAt(i));
        }
        return buf.array();
    }

    private static String dumpIssues(WalEventState state) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (i > 0) sb.append("; ");
            ReadIssue issue = state.getIssues().getQuick(i);
            sb.append(issue.code()).append(':').append(issue.message());
        }
        return sb.toString();
    }

    private static boolean hasIssue(WalEventState state, RecoveryIssueCode issueCode) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).code() == issueCode) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIssueContaining(WalEventState state, String substring) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).message().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    private static WalEventState readEvents(String dir) {
        return readEventsWithReader(dir, new BoundedWalEventReader(FF));
    }

    private static WalEventState readEventsWithReader(String dir, BoundedWalEventReader reader) {
        try (Path eventPath = new Path(); Path indexPath = new Path()) {
            eventPath.of(dir).concat(WalUtils.EVENT_FILE_NAME).$();
            indexPath.of(dir).concat(WalUtils.EVENT_INDEX_FILE_NAME).$();
            return reader.read(eventPath.$(), indexPath.$());
        }
    }

    /**
     * Writes an _event file. The {@code eventCount} parameter is the number of
     * events (records). The on-disk maxTxn header value is written as
     * {@code eventCount - 1} to match production format (0-based max txn ID).
     */
    private static void writeEventFile(String dir, int eventCount, int formatVersion, byte[]... records) throws IOException {
        int totalRecordBytes = 0;
        for (byte[] r : records) {
            totalRecordBytes += r.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(WALE_HEADER_SIZE + totalRecordBytes).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(eventCount - 1); // on-disk maxTxn = max 0-based txn ID
        buf.putInt(formatVersion);
        for (byte[] r : records) {
            buf.put(r);
        }
        writeFile(new File(dir, WalUtils.EVENT_FILE_NAME), buf.array());
    }

    private static void writeFile(File file, byte[] data) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }

    /**
     * Writes a full _event.i file with exactly (eventCount + 1) entries.
     * The provided offsets should contain the record offsets followed by the end-of-records sentinel.
     * Any remaining entries are padded with the last provided offset value.
     *
     * @param dir            directory to write into
     * @param eventCount     the number of events (determines required entry count)
     * @param recordOffsets  record offsets (entry[0]..entry[N-1]) plus end sentinel
     */
    private static void writeFullEventIndex(String dir, int eventCount, long... recordOffsets) throws IOException {
        int requiredEntries = eventCount + 1;
        ByteBuffer buf = ByteBuffer.allocate(requiredEntries * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        long lastOffset = recordOffsets.length > 0 ? recordOffsets[recordOffsets.length - 1] : 0;
        for (int i = 0; i < requiredEntries; i++) {
            if (i < recordOffsets.length) {
                buf.putLong(recordOffsets[i]);
            } else {
                buf.putLong(lastOffset);
            }
        }
        writeFile(new File(dir, WalUtils.EVENT_INDEX_FILE_NAME), buf.array());
    }

    /**
     * Writes a raw _event.i file with exactly the provided offsets (no padding).
     * Used for tests that intentionally create truncated index files.
     */
    private static void writeRawEventIndex(String dir, long... offsets) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(offsets.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long offset : offsets) {
            buf.putLong(offset);
        }
        writeFile(new File(dir, WalUtils.EVENT_INDEX_FILE_NAME), buf.array());
    }
}
