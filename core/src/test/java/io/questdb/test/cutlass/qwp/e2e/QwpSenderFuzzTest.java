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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.fuzz.FuzzChangeColumnTypeOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;

public class QwpSenderFuzzTest extends AbstractQwpWebSocketTest {

    // QuestDB's base32 alphabet (matches io.questdb.cairo.GeoHashes#base32):
    // skips 'a', 'i', 'l', 'o' versus straight 0-9a-z.
    private static final char[] GEO_HASH_BASE32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };
    // colNameBases / colTypes / colValueBases are partitioned: the first
    // LEGACY_COLUMN_COUNT entries are STRING/DOUBLE columns and participate
    // in the full skip / new-column-injection fuzz; the rest are typed
    // columns (BYTE, SHORT, INT, FLOAT, CHAR, UUID, LONG256, TIMESTAMP_NANO,
    // GEOHASH at four precisions) that are always set on every row and never
    // auto-injected as extras. Reason: typed columns whose unset cells render
    // differently from their type-default (e.g. BYTE 0 vs INT NULL) would
    // clash with the oracle's single-default-per-column model once
    // startAlterTableThread converts them across the integer family.
    private static final int LEGACY_COLUMN_COUNT = 6;
    private static final Log LOG = LogFactory.getLog(QwpSenderFuzzTest.class);
    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;
    private static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    private static final short[] integerColumnTypes = new short[]{ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG};
    private final int batchSize = 10;
    private final String[][] colNameBases = new String[][]{
            {"terület", "TERÜLet", "tERülET", "TERÜLET"},
            {"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
            {"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
            {"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
            {"notes", "NOTES", "NotEs", "noTeS"},
            {"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"},
            {"pressure_b", "PRESSURE_B", "Pressure_B"},
            {"pressure_s", "PRESSURE_S", "Pressure_S"},
            {"pressure_i", "PRESSURE_I", "Pressure_I"},
            {"pressure_f", "PRESSURE_F", "Pressure_F"},
            {"flag_c", "FLAG_C", "Flag_C"},
            {"sensor_id_u", "SENSOR_ID_U", "Sensor_Id_U"},
            {"token_l256", "TOKEN_L256", "Token_L256"},
            {"event_at_ns", "EVENT_AT_NS", "Event_At_Ns"},
            {"loc_g5", "LOC_G5", "Loc_G5"},
            {"loc_g15", "LOC_G15", "Loc_G15"},
            {"loc_g20", "LOC_G20", "Loc_G20"},
            {"loc_g35", "LOC_G35", "Loc_G35"},
            {"payload_bin", "PAYLOAD_BIN", "Payload_Bin"}
    };
    // New non-string/non-double types added to fuzz native wire-type emission
    // through the QWP sender. Their addColumnValue cases yield exactly the
    // text representation that CursorPrinter writes for each type so the
    // TableData oracle's two-pass assertion matches.
    // int[] (not short[]) because TIMESTAMP_NANO is an int sentinel
    // (1 << 18 | TIMESTAMP) outside the short range. GEOBYTE/GEOSHORT/GEOINT/
    // GEOLONG act as storage-class discriminants; addColumnValue maps each to
    // a fixed precision (5b / 15b / 20b / 35b — all multiples of 5 so the
    // cursor renders bare base32, matching the yielded string verbatim).
    private final int[] colTypes = new int[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE,
            BYTE, SHORT, INT, FLOAT, CHAR, UUID, LONG256, TIMESTAMP_NANO,
            GEOBYTE, GEOSHORT, GEOINT, GEOLONG, BINARY};
    // Integer-family value bases (indices 6..9, for BYTE/SHORT/INT/FLOAT) are
    // capped so the *10 + d arithmetic stays within byte range. When the
    // alter-table thread narrows a column across the integer family (e.g.
    // INT -> BYTE), each previously-written value casts losslessly and the
    // oracle's stored yield still matches what the cursor renders.
    // GEOHASH value bases are unused; the geohash cases generate random base32
    // strings independent of valueBase.
    private final String[] colValueBases = new String[]{"europe", "8", "2", "1", "note", "6",
            "5", "9", "11", "7", "M", "u", "l", "1700000000000000000",
            "", "", "", "", ""};
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};
    private final String[][] symbolNameBases = new String[][]{
            {"location", "Location", "LOCATION", "loCATion", "LocATioN"},
            {"city", "ciTY", "CITY"}
    };
    private final String[] symbolValueBases = new String[]{"us-midwest", "London"};
    private final AtomicLong timestampMicros = new AtomicLong(1_465_839_830_102_300L);
    // Cap the client's outgoing WS frame size, by row count. The client
    // never fragments outgoing WS messages, so the per-batch wire payload
    // plus WS header must stay strictly under the server's recvBufferSize
    // or the server tears the connection down with MESSAGE_TOO_BIG. We
    // bound by rows rather than bytes because the auto_flush_bytes
    // threshold compares against the per-row column-buffer encoding and
    // significantly underestimates the final wire frame size in
    // multi-table batches (full schema headers and a global symbol-dict
    // delta are added at flush time). 0 means use sender defaults.
    private int clientAutoFlushRows = 0;
    private double columnConvertProb;
    private int columnReorderingFactor = -1;
    private int columnSkipFactor = -1;
    private boolean diffCasesInColNames = false;
    private int duplicatesFactor = -1;
    private volatile String errorMsg = null;
    private boolean exerciseSymbols = true;
    private int forceRecvFragmentationChunkSize = Integer.MAX_VALUE;
    private int newColumnFactor = -1;
    private int nonAsciiValueFactor = -1;
    private int numOfIterations;
    private int numOfLines;
    private int numOfTables;
    private int numOfThreads;
    private Rnd random;
    // 32 KiB default. With 14 column types and frequent auto-create of "ex_..."
    // extras, full schema headers plus a global symbol-dict delta inflate
    // each flushed frame well past the legacy 8 KiB default. Tests that need
    // to assert behaviour around the recv-buffer limit override this
    // explicitly (see testLoadSmallBuffer).
    private int recvBufferSize = 32_768;
    private boolean sendSymbolsWithSpace = false;
    private LowerCaseCharSequenceObjHashMap<TableData> tables;
    private SOCountDownLatch threadPushFinished;
    private long waitBetweenIterationsMillis;

    @Before
    public void setUp() {
        super.setUp();
        random = TestUtils.generateRandom(LOG);
        forceRecvFragmentationChunkSize = 10 + random.nextInt(Math.min(512, recvBufferSize) - 10);
        LOG.info().$("fragmentation params [recvBufferSize=").$(recvBufferSize)
                .$(", forceRecvFragmentationChunkSize=").$(forceRecvFragmentationChunkSize)
                .I$();
    }

    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15 + random.nextInt(100), 5 + random.nextInt(5),
                2 + random.nextInt(Os.isWindows() ? 5 : 20), 1 + random.nextInt(4),
                random.nextInt(75));
        initFuzzParameters(-1, 1, 1 + random.nextInt(3), 6, false, true, false, 0.1);
        runTest();
    }

    @Test
    public void testAddColumnsNoSymbols() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, 4, 3, true, false, false, 0.15);
        runTest();
    }

    @Test
    public void testAddConvertColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, 4, -1, false, true, true, 0.2);
        runTest();
    }

    @Test
    public void testAllMixed() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(3, 4, 5, 10, 5, true, true, true, 0.05);
        runTest();
    }

    @Test
    public void testAllMixedNoSymbols() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(3, 4, 5, 10, 5, true, false, true, 0.05);
        runTest();
    }

    @Test
    public void testAllMixedSingleTable() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 1, 50);
        initFuzzParameters(3, 4, 5, 10, 5, true, true, true, 0.05);
        runTest();
    }

    @Test
    public void testAllMixedSplitPart() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 1, 50);
        initFuzzParameters(-1, -1, -1, 10, -1, false, true, false, 0.05);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, 2, -1, true, true, false);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumnsNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, true, false, false);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, 3, -1, true, true, true);
        // Same wide-batch-vs-default-recv-buffer issue as testLoadSendSymbolsWithSpace.
        clientAutoFlushRows = 5;
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, false, false, 0.05);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, true, 0.05);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        runTest();
    }

    @Test
    public void testLoadLargePayload() throws Exception {
        initLoadParameters(500, Os.isWindows() ? 3 : 5, 5, 5, 10);
        runTest();
    }

    @Test
    public void testLoadNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, 5, true, false, false, 0.05);
        runTest();
    }

    @Test
    public void testLoadSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 4, 8, 20);
        initFuzzParameters(-1, -1, 2, -1, false, true, true);
        // Frequent new-column injection (newColumnFactor=2) plus 8
        // interleaved tables and symbols with embedded spaces inflates
        // batch size enough that a 10-row frame exceeds the default
        // 8192-byte server recv buffer. Cap rows-per-frame to keep the
        // wire payload safely under it.
        clientAutoFlushRows = 5;
        runTest();
    }

    @Test
    public void testLoadSmallBuffer() throws Exception {
        recvBufferSize = 2048;
        // Cap rows-per-frame so the wire payload stays under recvBufferSize.
        // This schema is ~9 columns of mixed string/double; with 5 tables
        // interleaved per batch and full schema headers, ~3 rows yields
        // a wire frame around 1KB, comfortably under 2048.
        clientAutoFlushRows = 3;
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 20);
        runTest();
    }

    @Test
    public void testNonAsciiValues() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, -1, 3, 4, false, true, false);
        runTest();
    }

    @Test
    public void testNonAsciiValuesNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, -1, -1, 4, false, false, false);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, 8, false, true, true, 0.05);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, true, false, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingManyThreads() throws Exception {
        initLoadParameters(15 + random.nextInt(100), 5 + random.nextInt(5),
                2 + random.nextInt(Os.isWindows() ? 5 : 20), 1 + random.nextInt(4),
                random.nextInt(75));
        initFuzzParameters(3, -1, 1 + random.nextInt(3), -1, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, 2, 4, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingSkipColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 2, 4, true, true, false);
        runTest();
    }

    @Test
    public void testReorderingSkipColumnsWithNonAsciiNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, 4, true, false, false);
        runTest();
    }

    @Test
    public void testReorderingSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingSkipDuplicateColumnsWithNonAsciiNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, false, false, 0.05);
        runTest();
    }

    private static int changeColumnTypeTo(Rnd rnd, int columnType) {
        int nextColType = columnType;
        return switch (columnType) {
            case ColumnType.STRING -> rnd.nextBoolean() ? ColumnType.SYMBOL : ColumnType.VARCHAR;
            case ColumnType.SYMBOL -> rnd.nextBoolean() ? ColumnType.STRING : ColumnType.VARCHAR;
            case ColumnType.VARCHAR -> rnd.nextBoolean() ? ColumnType.STRING : ColumnType.SYMBOL;
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                while (nextColType == columnType) {
                    nextColType = integerColumnTypes[rnd.nextInt(integerColumnTypes.length)];
                }
                yield nextColType;
            }
            case ColumnType.FLOAT -> ColumnType.DOUBLE;
            case ColumnType.DOUBLE -> ColumnType.FLOAT;
            case TIMESTAMP -> ColumnType.LONG;
            default -> columnType;
        };
    }

    private CharSequence addColumn(LineData line, int colIndex, QwpWebSocketSender sender, Rnd rnd) {
        CharSequence colName = generateColumnName(colIndex, false, rnd);
        CharSequence colValue = addColumnValue(colTypes[colIndex], colValueBases[colIndex], colName, sender, rnd);
        line.addColumn(colName, colValue);
        return colName;
    }

    private String addColumnValue(int type, String valueBase, CharSequence colName, QwpWebSocketSender sender, Rnd rnd) {
        return switch (type) {
            case DOUBLE -> {
                int d = rnd.nextInt(9);
                sender.doubleColumn(colName, Numbers.parseInt(valueBase) * 10 + d);
                yield valueBase + d + ".0";
            }
            case SYMBOL -> {
                String postfix = Character.toString(shouldFuzz(nonAsciiValueFactor, rnd) ? nonAsciiChars[rnd.nextInt(nonAsciiChars.length)] : rnd.nextChar());
                String base = valueBase;
                if (sendSymbolsWithSpace && rnd.nextInt(SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR) == 0) {
                    int spaceIndex = rnd.nextInt(base.length() - 1);
                    base = base.substring(0, spaceIndex) + "  " + base.substring(spaceIndex);
                }
                String symVal = base + postfix;
                sender.symbol(colName, symVal);
                yield symVal;
            }
            case STRING -> {
                String postfix = Character.toString(shouldFuzz(nonAsciiValueFactor, rnd) ? nonAsciiChars[rnd.nextInt(nonAsciiChars.length)] : rnd.nextChar());
                sender.stringColumn(colName, valueBase + postfix);
                yield "\"" + valueBase + postfix + "\"";
            }
            case BYTE -> {
                // valueBase may belong to a column whose type was changed via DDL
                // (e.g. an INT column with base "100" coerced to BYTE), so the
                // raw int can exceed the byte range. Yield the truncated byte
                // so the oracle matches what the cursor renders.
                int d = rnd.nextInt(9);
                byte v = (byte) (Numbers.parseInt(valueBase) * 10 + d);
                sender.byteColumn(colName, v);
                yield Integer.toString(v);
            }
            case SHORT -> {
                int d = rnd.nextInt(9);
                short v = (short) (Numbers.parseInt(valueBase) * 10 + d);
                sender.shortColumn(colName, v);
                yield Integer.toString(v);
            }
            case INT -> {
                int d = rnd.nextInt(9);
                int v = Numbers.parseInt(valueBase) * 10 + d;
                sender.intColumn(colName, v);
                yield Integer.toString(v);
            }
            case FLOAT -> {
                // Integer-valued floats render identically via CursorPrinter and
                // Float.toString, so the yielded "70.0".."79.0" matches.
                int d = rnd.nextInt(9);
                float v = (float) (Numbers.parseInt(valueBase) * 10 + d);
                sender.floatColumn(colName, v);
                yield ((int) v) + ".0";
            }
            case CHAR -> {
                // CursorPrinter writes a CHAR via sink.put(char), no decoration.
                // Offset random letter from the valueBase letter to vary per row.
                char c = (char) (valueBase.charAt(0) + rnd.nextInt(10));
                sender.charColumn(colName, c);
                yield Character.toString(c);
            }
            case UUID -> {
                // hi/lo crafted so neither limb hits LONG_NULL (which would render
                // as empty per Uuid.isNull). Use Numbers.appendUuid to match the
                // exact text format the cursor produces.
                long hi = (long) (rnd.nextInt(0x7FFF_FFFF) + 1) << 32 | (rnd.nextInt() & 0xFFFFFFFFL);
                long lo = (long) (rnd.nextInt(0x7FFF_FFFF) + 1) << 32 | (rnd.nextInt() & 0xFFFFFFFFL);
                sender.uuidColumn(colName, lo, hi);
                StringSink sink = new StringSink();
                Numbers.appendUuid(lo, hi, sink);
                yield sink.toString();
            }
            case LONG256 -> {
                // Force the top limb non-zero so the four-limb hex render path
                // fires deterministically; values stay positive to keep things
                // simple.
                long l0 = (rnd.nextLong() & 0x7FFF_FFFF_FFFF_FFFFL) | 1L;
                long l1 = (rnd.nextLong() & 0x7FFF_FFFF_FFFF_FFFFL) | 1L;
                long l2 = (rnd.nextLong() & 0x7FFF_FFFF_FFFF_FFFFL) | 1L;
                long l3 = (rnd.nextLong() & 0x7FFF_FFFF_FFFF_FFFFL) | 1L;
                sender.long256Column(colName, l0, l1, l2, l3);
                StringSink sink = new StringSink();
                Numbers.appendLong256(l0, l1, l2, l3, sink);
                yield sink.toString();
            }
            case TIMESTAMP_NANO -> {
                // Step in 1-microsecond units (1_000 nanos) off the base so nanos
                // values stay distinct per row but always end in 000 — that keeps
                // the rendered string stable to read.
                long base = Numbers.parseLong(valueBase);
                long nanos = base + (long) rnd.nextInt(1_000_000) * 1_000L;
                sender.timestampColumn(colName, nanos, ChronoUnit.NANOS);
                StringSink sink = new StringSink();
                NanosFormatUtils.appendDateTimeNSec(sink, nanos);
                yield sink.toString();
            }
            case GEOBYTE -> randomGeoHash(sender, colName, 1, rnd);   // GEOHASH(5b)
            case GEOSHORT -> randomGeoHash(sender, colName, 3, rnd);  // GEOHASH(15b)
            case GEOINT -> randomGeoHash(sender, colName, 4, rnd);    // GEOHASH(20b)
            case GEOLONG -> randomGeoHash(sender, colName, 7, rnd);   // GEOHASH(35b)
            case BINARY -> randomBinaryOneByte(sender, colName, rnd);
            default -> {
                sender.stringColumn(colName, valueBase);
                yield valueBase;
            }
        };
    }

    private void addDuplicateColumn(LineData line, int colIndex, CharSequence colName, QwpWebSocketSender sender, Rnd rnd) {
        if (shouldFuzz(duplicatesFactor, rnd)) {
            CharSequence colValue = addColumnValue(colTypes[colIndex], colValueBases[colIndex], colName, sender, rnd);
            line.addColumn(colName, colValue);
        }
    }

    private void addDuplicateSymbol(LineData line, int symIndex, CharSequence symName, QwpWebSocketSender sender, Rnd rnd) {
        if (shouldFuzz(duplicatesFactor, rnd)) {
            CharSequence symValue = addSymbolValue(symIndex, symName, sender, rnd);
            line.addColumn(symName, symValue);
        }
    }

    private void addNewColumn(LineData line, QwpWebSocketSender sender, Rnd rnd) {
        if (shouldFuzz(newColumnFactor, rnd)) {
            // Pick only from the legacy STRING/DOUBLE pool; typed columns are
            // always set on every row, so they have no auto-create-only mode.
            int extraColIndex = rnd.nextInt(LEGACY_COLUMN_COUNT);
            CharSequence colName = generateColumnName(extraColIndex, true, rnd);
            CharSequence colValue = addColumnValue(colTypes[extraColIndex], colValueBases[extraColIndex], colName, sender, rnd);
            line.addColumn(colName, colValue);
        }
    }

    private void addNewSymbol(LineData line, QwpWebSocketSender sender, Rnd rnd) {
        if (shouldFuzz(newColumnFactor, rnd)) {
            int extraSymIndex = rnd.nextInt(symbolNameBases.length);
            CharSequence symName = generateSymbolName(extraSymIndex, true, rnd);
            CharSequence symValue = addSymbolValue(extraSymIndex, symName, sender, rnd);
            line.addColumn(symName, symValue);
        }
    }

    private CharSequence addSymbol(LineData line, int symIndex, QwpWebSocketSender sender, Rnd rnd) {
        CharSequence symName = generateSymbolName(symIndex, false, rnd);
        CharSequence symValue = addSymbolValue(symIndex, symName, sender, rnd);
        line.addColumn(symName, symValue);
        return symName;
    }

    private String addSymbolValue(int index, CharSequence colName, QwpWebSocketSender sender, Rnd rnd) {
        return addColumnValue(SYMBOL, symbolValueBases[index], colName, sender, rnd);
    }

    private void assertTable(TableData table, String tableName) {
        if (table.size() < 1) {
            return;
        }
        try (
                TableReader reader = engine.getReader(tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            LOG.info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                    .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            TableReaderMetadata metadata = reader.getMetadata();
            CharSequence expected = table.generateRows(metadata);

            long txnMinTs = reader.getMinTimestamp();
            int timestampIndex = reader.getMetadata().getTimestampIndex();
            if (cursor.hasNext()) {
                long dataMinTs = cursor.getRecord().getLong(timestampIndex);
                Assert.assertEquals(dataMinTs, txnMinTs);
                cursor.toTop();
            }

            try {
                assertCursorTwoPass(expected, cursor, metadata);
            } catch (AssertionError e) {
                throw new AssertionError("Table: " + table.getName(), e);
            }
        } catch (CairoException e) {
            if (e.getFlyweightMessage().toString().contains("table does not exist")) {
                Assert.fail("Table " + tableName + " does not exist but expected " + table.size() + " rows");
            }
            throw e;
        }
    }

    private String generateColumnName(int index, boolean randomize, Rnd rnd) {
        return generateName(colNameBases[index], randomize, rnd);
    }

    private LineData generateLine(CharSequence tableName, QwpWebSocketSender sender, Rnd rnd) {
        LineData line = new LineData(timestampMicros.incrementAndGet());
        sender.table(tableName);

        if (exerciseSymbols) {
            int[] symIndexes = getSymbolIndexes(rnd);
            for (int symIndex : symIndexes) {
                CharSequence symName = addSymbol(line, symIndex, sender, rnd);
                addDuplicateSymbol(line, symIndex, symName, sender, rnd);
                addNewSymbol(line, sender, rnd);
            }
        }
        int[] columnIndexes = getColumnIndexes(rnd);
        for (int colIndex : columnIndexes) {
            CharSequence colName = addColumn(line, colIndex, sender, rnd);
            addDuplicateColumn(line, colIndex, colName, sender, rnd);
            addNewColumn(line, sender, rnd);
        }
        sender.at(line.getTimestamp(), ChronoUnit.MICROS);
        return line;
    }

    private String generateName(String[] names, boolean randomize, Rnd rnd) {
        int caseIndex = diffCasesInColNames ? rnd.nextInt(names.length) : 0;
        String postfix = randomize ? Integer.toString(rnd.nextInt(NEW_COLUMN_RANDOMIZE_FACTOR)) : "";
        return names[caseIndex] + postfix;
    }

    private int[] generateOrdering(int numOfCols, Rnd rnd) {
        int[] columnOrdering = new int[numOfCols];
        if (shouldFuzz(columnReorderingFactor, rnd)) {
            List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < columnOrdering.length; i++) {
                indexes.add(i);
            }
            Collections.shuffle(indexes);
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = indexes.get(i);
            }
        } else {
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = i;
            }
        }
        return columnOrdering;
    }

    private String generateSymbolName(int index, boolean randomize, Rnd rnd) {
        return generateName(symbolNameBases[index], randomize, rnd);
    }

    private int[] getColumnIndexes(Rnd rnd) {
        return skipColumns(generateOrdering(colNameBases.length, rnd), rnd);
    }

    private int[] getSymbolIndexes(Rnd rnd) {
        return skipColumns(generateOrdering(symbolNameBases.length, rnd), rnd);
    }

    private String getTableName(int tableIndex) {
        return "weather" + tableIndex;
    }

    private void initFuzzParameters(
            int columnReorderingFactor,
            int columnSkipFactor,
            int newColumnFactor,
            int nonAsciiValueFactor,
            boolean diffCasesInColNames,
            boolean exerciseSymbols,
            boolean sendSymbolsWithSpace
    ) {
        initFuzzParameters(-1, columnReorderingFactor, columnSkipFactor, newColumnFactor,
                nonAsciiValueFactor, diffCasesInColNames, exerciseSymbols, sendSymbolsWithSpace, 0.05);
    }

    private void initFuzzParameters(
            int columnReorderingFactor,
            int columnSkipFactor,
            int newColumnFactor,
            int nonAsciiValueFactor,
            boolean diffCasesInColNames,
            boolean exerciseSymbols,
            boolean sendSymbolsWithSpace,
            double columnConvertProb
    ) {
        initFuzzParameters(-1, columnReorderingFactor, columnSkipFactor, newColumnFactor,
                nonAsciiValueFactor, diffCasesInColNames, exerciseSymbols, sendSymbolsWithSpace, columnConvertProb);
    }

    private void initFuzzParameters(
            int duplicatesFactor,
            int columnReorderingFactor,
            int columnSkipFactor,
            int newColumnFactor,
            int nonAsciiValueFactor,
            boolean diffCasesInColNames,
            boolean exerciseSymbols,
            boolean sendSymbolsWithSpace,
            double columnConvertProb
    ) {
        this.columnConvertProb = columnConvertProb;
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.duplicatesFactor = duplicatesFactor;
        this.newColumnFactor = newColumnFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.diffCasesInColNames = diffCasesInColNames;
        this.exerciseSymbols = exerciseSymbols;
        this.sendSymbolsWithSpace = sendSymbolsWithSpace;
    }

    private void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        this.numOfLines = numOfLines;
        this.numOfIterations = numOfIterations;
        this.numOfThreads = numOfThreads;
        this.numOfTables = numOfTables;
        this.waitBetweenIterationsMillis = waitBetweenIterationsMillis;

        threadPushFinished = new SOCountDownLatch();
        tables = new LowerCaseCharSequenceObjHashMap<>();
    }

    private CharSequence pickTableName(Rnd rnd) {
        String tableName = rnd.nextInt(UPPERCASE_TABLE_RANDOMIZE_FACTOR) == 0 ? "WEATHER" : "weather";
        return tableName + rnd.nextInt(numOfTables);
    }

    /**
     * Emit a BINARY column with a random 1-byte payload and yield the exact
     * text that {@code CursorPrinter} -> {@code Chars.toSink(BinarySequence,...)}
     * produces for that payload: an 8-digit hex offset, a space, then the
     * lowercase hex of the byte. Single-byte payloads keep the render to one
     * line; multi-byte payloads would need to reproduce the 16-byte wrap rule.
     */
    private String randomBinaryOneByte(QwpWebSocketSender sender, CharSequence colName, Rnd rnd) {
        byte b = (byte) rnd.nextInt(256);
        sender.binaryColumn(colName, new byte[]{b});
        char[] hex = io.questdb.std.Numbers.hexDigits;
        int v = b & 0xFF;
        return "00000000 " + hex[v >>> 4] + hex[v & 0xF];
    }

    /**
     * Emit a GEOHASH column from a random {@code chars}-char base32 string and
     * yield the same string back. With precisions that are multiples of 5
     * CursorPrinter renders the stored value as bare base32 (no '#' prefix),
     * so the yield matches what the oracle reads back verbatim.
     */
    private String randomGeoHash(QwpWebSocketSender sender, CharSequence colName, int chars, Rnd rnd) {
        char[] buf = new char[chars];
        for (int i = 0; i < chars; i++) {
            buf[i] = GEO_HASH_BASE32[rnd.nextInt(GEO_HASH_BASE32.length)];
        }
        String hash = new String(buf);
        sender.geoHashColumn(colName, hash);
        return hash;
    }

    private void runTest() throws Exception {

        Assert.assertEquals(0, tables.size());
        for (int i = 0; i < numOfTables; i++) {
            CharSequence tableName = getTableName(i);
            tables.put(tableName, new TableData(tableName));
        }

        runInContext(port -> {
            int waitCount = numOfThreads;
            threadPushFinished.setCount(waitCount);
            AtomicInteger failureCounter = new AtomicInteger();
            for (int i = 0; i < numOfThreads; i++) {
                Rnd threadRnd = new Rnd(random.nextLong(), random.nextLong());
                startThread(port, threadPushFinished, failureCounter, threadRnd);
            }
            Thread alterTableThread = null;
            if (columnConvertProb > 0) {
                alterTableThread = startAlterTableThread(threadPushFinished, failureCounter);
            }
            threadPushFinished.await();
            if (alterTableThread != null) {
                alterTableThread.join();
            }
            Assert.assertEquals(0, failureCounter.get());

            drainWalQueue();

            for (int i = 0; i < numOfTables; i++) {
                String tableName = getTableName(i);
                TableData table = tables.get(tableName);
                if (table.size() > 0) {
                    engine.awaitTable(tableName, 30, TimeUnit.SECONDS);
                    assertTable(table, tableName);
                }
            }
        }, recvBufferSize, forceRecvFragmentationChunkSize);

        if (errorMsg != null) {
            Assert.fail(errorMsg);
        }
    }

    private boolean shouldFuzz(int fuzzFactor, Rnd rnd) {
        return fuzzFactor > 0 && rnd.nextInt(fuzzFactor) == 0;
    }

    private int[] skipColumns(int[] originalColumnIndexes, Rnd rnd) {
        if (shouldFuzz(columnSkipFactor, rnd)) {
            List<Integer> indexes = new ArrayList<>();
            for (int originalColumnIndex : originalColumnIndexes) {
                indexes.add(originalColumnIndex);
            }
            int numOfSkippedCols = rnd.nextInt(MAX_NUM_OF_SKIPPED_COLS) + 1;
            for (int i = 0; i < numOfSkippedCols; i++) {
                // Only legacy STRING/DOUBLE columns are eligible to be skipped.
                // Typed columns (BYTE/SHORT/INT/FLOAT/CHAR/UUID/LONG256/TIMESTAMP_NANO)
                // must remain in every row so the oracle never falls back to a
                // type default whose cursor rendering can drift after a DDL
                // type conversion.
                int legacyEligible = 0;
                for (Integer idx : indexes) {
                    if (idx < LEGACY_COLUMN_COUNT) {
                        legacyEligible++;
                    }
                }
                if (legacyEligible == 0) {
                    break;
                }
                int target = rnd.nextInt(legacyEligible);
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j) < LEGACY_COLUMN_COUNT) {
                        if (target == 0) {
                            indexes.remove(j);
                            break;
                        }
                        target--;
                    }
                }
            }
            int[] columnIndexes = new int[indexes.size()];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnIndexes[i] = indexes.get(i);
            }
            return columnIndexes;
        }
        return originalColumnIndexes;
    }

    private Thread startAlterTableThread(SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        Rnd rnd = new Rnd(random.nextLong(), random.nextLong());
        Thread thread = new Thread(() -> {
            int totalTestConversions = rnd.nextInt((int) (numOfLines * numOfTables * columnConvertProb));
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                while (totalTestConversions > 0 && threadPushFinished.getCount() > 0 && failureCounter.get() == 0) {
                    CharSequence tableName = getTableName(rnd.nextInt(numOfTables));
                    TableToken tableToken = engine.getTableTokenIfExists(tableName);
                    if (tableToken != null) {
                        try (TableMetadata meta = engine.getTableMetadata(tableToken)) {
                            int startColIndex = rnd.nextInt(meta.getColumnCount());
                            for (int i = 0; i < meta.getColumnCount(); i++) {
                                int colIndex = (startColIndex + i) % meta.getColumnCount();
                                int type = meta.getColumnType(colIndex);
                                if (type > 0 && FuzzChangeColumnTypeOperation.canGenerateColumnTypeChange(meta, colIndex)) {
                                    int newType = changeColumnTypeTo(rnd, type);
                                    try {
                                        engine.execute(
                                                "ALTER TABLE " + tableName + " ALTER COLUMN "
                                                        + meta.getColumnName(colIndex) + " TYPE " + ColumnType.nameOf(newType),
                                                executionContext
                                        );
                                        totalTestConversions--;
                                    } catch (SqlException ex) {
                                        if (!Chars.contains(ex.getFlyweightMessage(), "type is already")) {
                                            throw ex;
                                        }
                                    }
                                    break;
                                }
                            }
                        } catch (SqlException e) {
                            LOG.error().$("Failed to alter table [e=").$((Throwable) e).I$();
                            failureCounter.incrementAndGet();
                        }
                    }
                    Os.sleep(10 + rnd.nextInt(100));
                }
            } finally {
                Path.clearThreadLocals();
            }
        });
        thread.start();
        return thread;
    }

    private void startThread(int port, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter, Rnd rnd) {
        new Thread(() -> {
            try (QwpWebSocketSender sender = clientAutoFlushRows > 0
                    ? connectWs(port, clientAutoFlushRows, QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS)
                    : connectWs(port)) {
                long points = 0;
                for (int n = 0; n < numOfIterations; n++) {
                    for (int j = 0; j < numOfLines; j++) {
                        CharSequence tableName = pickTableName(rnd);
                        LineData line = generateLine(tableName, sender, rnd);
                        TableData table = tables.get(tableName);
                        table.addLine(line);

                        if (++points % batchSize == 0) {
                            sender.flush();
                        }
                    }

                    sender.flush();
                    Os.sleep(waitBetweenIterationsMillis);
                }
            } catch (Exception e) {
                LOG.error().$("Data sending failed [e=").$(e).I$();
                failureCounter.incrementAndGet();
                errorMsg = "Data sending failed [e=" + e + "]";
            } finally {
                threadPushFinished.countDown();
                Path.clearThreadLocals();
            }
        }).start();
    }
}
