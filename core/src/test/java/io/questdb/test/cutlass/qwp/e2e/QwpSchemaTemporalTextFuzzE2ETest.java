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

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * A schema-aware sender parses DATE and timestamp text itself and sends the
 * value. This test sends random text through such a sender and compares what
 * the table stores with what the server's parser returns for the same text. The
 * two must agree on every text, with two exceptions where the sender rejects
 * the text whatever the server does: a time zone name, and a year that does not
 * fit an INT, which the server sometimes wraps into another year and stores.
 */
public class QwpSchemaTemporalTextFuzzE2ETest extends AbstractQwpWebSocketTest {
    private static final String[] ALL_DAYS = {"01", "28", "29", "30", "31", "00", "32", "1", "001"};
    private static final String[] ALL_FRACTIONS = {
            "", ".", ".1", ".12", ".123", ".1234", ".12345", ".123456", ".1234567", ".12345678",
            ".123456789", ".1234567890", ".000", ".999999", ",123"
    };
    private static final String[] ALL_HOURS = {"00", "12", "23", "24", "25", "1", "99"};
    private static final String[] ALL_MINUTES = {"00", "30", "59", "60", "1"};
    private static final String[] ALL_MONTHS = {"01", "02", "12", "00", "13", "1", "7", "001"};
    private static final String[] ALL_SEPARATORS = {"-", "/", ""};
    private static final String[] ALL_TIME_SEPARATORS = {"T", " ", "t", "", "  "};
    private static final String[] ALL_YEARS = {
            "2024", "1970", "1969", "0001", "0000", "9999", "24", "1", "024", "12345", "292278994",
            "-0001", "-001", "-1", "-2024", "-12345", "-292275055", "+2024", "1_000", "2024L", ""
    };
    // Forms the sender takes, and malformed ones that hold no name.
    private static final String[] ALL_ZONES = {
            "", "Z", "z", "UTC", "utc", "GMT", "gmt", "Utc", "+01", "-01", "+0100", "+01:00", "-01:30",
            "+00:00", "-00", "+23:59", "UTC+01", "GMT-01:30", "utc+0100", "gmt-00", " Z", " +01:00",
            " UTC", "+1", "+24", "+01:60", "+0160", "+01:0", "+01:", "+", "-", "UTC+", "GMT:",
            "+01:00:00", "Z ", "ZZ", "+-01", "UTC+01:00Z", "+2400", "-23:59", "+001", "01", "0100",
            "01:00", "00", "1", "UTC01"
    };
    // How much of the layout a text holds, up to the fraction. Most hold a time,
    // which alone takes a zone.
    private static final int[] DEPTHS = {0, 1, 2, 2, 3, 4, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6};
    private static final String[] GOOD_DAYS = {"01", "15", "28", "29", "30", "31"};
    private static final String[] GOOD_HOURS = {"00", "07", "12", "23", "24"};
    private static final String[] GOOD_MINUTES = {"00", "30", "59"};
    private static final String[] GOOD_MONTHS = {"01", "02", "06", "12"};
    // Half of them fit TIMESTAMP_NS, which spans the years 1677 to 2262.
    private static final String[] GOOD_YEARS = {
            "2024", "1970", "1969", "2023", "1999", "2038", "2100", "1700", "2262", "24", "70",
            "0001", "0000", "9999", "1600", "1677", "-0001", "-2024", "1", "-1", "12345", "-12345"
    };
    private static final String[] GOOD_ZONES = {
            "", "Z", "z", "UTC", "utc", "GMT", "gmt", "+01", "-01", "+0100", "+01:00", "-01:30",
            "+23:59", "-23:59", "UTC+01", "GMT-01:30", "utc+0100"
    };
    private static final int INPUT_COUNT = 10_000;
    private static final Log LOG = LogFactory.getLog(QwpSchemaTemporalTextFuzzE2ETest.class);
    // No letter that could complete a zone name.
    private static final char[] MUTATIONS = "0123456789-:. T+ZzW,/_\u00e9\uff11".toCharArray();
    // Names the server resolves and names it does not; the sender rejects them all.
    private static final String[] ZONE_NAMES = {
            "EST", "Europe/Berlin", "europe/berlin", "CET", "BST", "MT", "Zulu", "UCT", "GMT0", "Etc/UTC",
            "America/New_York", "PST", "UT", "GB", "NZ", " EST", "ESTZ", "Etc/GMT+1", "UTCx", "\u0131ST",
            "E\u017ft"
    };

    @Test
    public void testSenderStoresWhatTheServerParserReturns() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final ObjList<Input> inputs = new ObjList<>();
        for (int i = 0; i < INPUT_COUNT; i++) {
            inputs.add(generate(rnd));
        }
        runInContext(port -> {
            for (Target target : Target.values()) {
                execute("CREATE TABLE " + target.table + " (case_id LONG, value " + target.sqlType
                        + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            final ObjList<StringBuilder> expected = new ObjList<>();
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                for (Target target : Target.values()) {
                    final StringBuilder rows = new StringBuilder("case_id\tvalue\n");
                    expected.add(rows);
                    int stored = 0;
                    int rejected = 0;
                    int zoneNames = 0;
                    for (int i = 0; i < INPUT_COUNT; i++) {
                        final Input input = inputs.getQuick(i);
                        final String message = target + " text '" + input.text + "' [case_id=" + i + ']';
                        boolean isServerRejecting = false;
                        long serverValue = 0;
                        final boolean isSenderAlone = input.isZoneName || isYearBeyondInt(input.text);
                        if (!isSenderAlone) {
                            try {
                                // The server parses the UTF-8 bytes of the text, one byte per character.
                                serverValue = target.parseAsServer(new Utf8String(input.text).asAsciiCharSequence());
                            } catch (NumericException e) {
                                isServerRejecting = true;
                            } catch (RuntimeException e) {
                                throw new AssertionError(message, e);
                            }
                        }

                        boolean isSenderRejecting = false;
                        sender.table(target.table).longColumn("case_id", i);
                        try {
                            sender.stringColumn("value", input.text);
                        } catch (LineSenderSchemaException e) {
                            Assert.assertEquals(message, LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                            isSenderRejecting = true;
                        }

                        if (isSenderAlone) {
                            Assert.assertTrue(message, isSenderRejecting);
                        } else {
                            Assert.assertEquals(message, isServerRejecting, isSenderRejecting);
                        }
                        if (input.isZoneName) {
                            zoneNames++;
                        }
                        if (isSenderRejecting) {
                            // The failed setter cancelled the row.
                            rejected++;
                        } else {
                            sender.at(1_000_000L + i, ChronoUnit.MICROS);
                            rows.append(i).append('\t');
                            if (serverValue == Long.MIN_VALUE) {
                                rows.append("null");
                            } else {
                                rows.append(serverValue);
                            }
                            rows.append('\n');
                            stored++;
                        }
                    }
                    LOG.info().$("fuzzed [target=").$(target.name()).$(", stored=").$(stored)
                            .$(", rejected=").$(rejected).$(", zoneNames=").$(zoneNames).I$();
                    // The generator must keep producing all three kinds of text. TIMESTAMP_NS
                    // stores the fewest, about one text in eleven: most years do not fit it.
                    Assert.assertTrue(target + " stored " + stored, stored >= INPUT_COUNT / 20);
                    Assert.assertTrue(target + " rejected " + rejected, rejected - zoneNames >= INPUT_COUNT / 10);
                    Assert.assertTrue(target + " zone names " + zoneNames, zoneNames >= INPUT_COUNT / 20);
                }
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 30_000));
            }
            drainWalQueue();
            for (Target target : Target.values()) {
                assertQuery("SELECT case_id, value::LONG value FROM " + target.table + " ORDER BY case_id")
                        .noLeakCheck().expectSize().returns(expected.getQuick(target.ordinal()).toString());
            }
        });
    }

    private static Input generate(Rnd rnd) {
        final StringBuilder text = new StringBuilder();
        text.append(pick(rnd, GOOD_YEARS, ALL_YEARS));
        final int depth = DEPTHS[rnd.nextInt(DEPTHS.length)];
        if (depth > 0) {
            text.append(pick(rnd, "-", ALL_SEPARATORS)).append(pick(rnd, GOOD_MONTHS, ALL_MONTHS));
        }
        if (depth > 1) {
            text.append(pick(rnd, "-", ALL_SEPARATORS)).append(pick(rnd, GOOD_DAYS, ALL_DAYS));
        }
        if (depth > 2) {
            text.append(pick(rnd, rnd.nextBoolean() ? "T" : " ", ALL_TIME_SEPARATORS))
                    .append(pick(rnd, GOOD_HOURS, ALL_HOURS));
        }
        if (depth > 3) {
            text.append(':').append(pick(rnd, GOOD_MINUTES, ALL_MINUTES));
        }
        if (depth > 4) {
            text.append(':').append(pick(rnd, GOOD_MINUTES, ALL_MINUTES));
        }
        if (depth > 5) {
            text.append(ALL_FRACTIONS[rnd.nextInt(ALL_FRACTIONS.length)]);
        }
        // Mutate before the zone goes on: dropping a letter of GMT leaves MT, a zone name.
        if (rnd.nextInt(6) == 0) {
            mutate(rnd, text);
        }
        final boolean isZoneName = rnd.nextInt(8) == 0;
        if (isZoneName) {
            text.append(ZONE_NAMES[rnd.nextInt(ZONE_NAMES.length)]);
        } else if (depth > 4 || rnd.nextInt(4) == 0) {
            text.append(pick(rnd, GOOD_ZONES, ALL_ZONES));
        }
        return new Input(text.toString(), isZoneName);
    }

    // A dated text whose year, the digits before the first separator dash, does not
    // fit an INT. Numbers.parseIntSafely() misses some of these overflows.
    private static boolean isYearBeyondInt(String text) {
        final boolean isNegative = text.startsWith("-");
        final int lo = isNegative ? 1 : 0;
        int hi = lo;
        while (hi < text.length() && text.charAt(hi) >= '0' && text.charAt(hi) <= '9') {
            hi++;
        }
        if (hi - lo < 10 || hi == text.length() || text.charAt(hi) != '-') {
            return false;
        }
        return hi - lo > 10 || Long.parseLong(text, lo, hi, 10) > (isNegative ? 2_147_483_648L : Integer.MAX_VALUE);
    }

    private static void mutate(Rnd rnd, StringBuilder text) {
        for (int i = 0, n = 1 + rnd.nextInt(2); i < n; i++) {
            final int pos = rnd.nextInt(text.length() + 1);
            final char c = MUTATIONS[rnd.nextInt(MUTATIONS.length)];
            switch (rnd.nextInt(3)) {
                case 0 -> text.insert(pos, c);
                case 1 -> {
                    if (pos < text.length()) {
                        text.deleteCharAt(pos);
                    }
                }
                default -> {
                    if (pos < text.length()) {
                        text.setCharAt(pos, c);
                    }
                }
            }
        }
    }

    // Mostly the value the layouts take, sometimes any of the values.
    private static String pick(Rnd rnd, String good, String[] all) {
        return rnd.nextInt(12) == 0 ? all[rnd.nextInt(all.length)] : good;
    }

    private static String pick(Rnd rnd, String[] good, String[] all) {
        return pick(rnd, good[rnd.nextInt(good.length)], all);
    }

    private static class Input {
        private final boolean isZoneName;
        private final String text;

        private Input(String text, boolean isZoneName) {
            this.text = text;
            this.isZoneName = isZoneName;
        }
    }

    private enum Target {
        DATE("date"),
        TIMESTAMP("timestamp"),
        TIMESTAMP_NS("timestamp_ns");

        private final String sqlType;
        private final String table;

        Target(String sqlType) {
            this.sqlType = sqlType;
            this.table = "temporal_text_" + sqlType;
        }

        // What the server stores for text that reaches it, as WalColumnarRowAppender parses it.
        private long parseAsServer(CharSequence text) throws NumericException {
            if (this == DATE) {
                return DateFormatUtils.parseDate(text);
            }
            final long micros = MicrosFormatUtils.parseTimestamp(text);
            if (this == TIMESTAMP) {
                return micros;
            }
            if (micros > Long.MAX_VALUE / 1000 || micros < Long.MIN_VALUE / 1000) {
                throw NumericException.instance();
            }
            return micros * 1000;
        }
    }
}
