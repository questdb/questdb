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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.std.Rnd;

/**
 * One anchor shape for an anchored fuzz run: the {@code ANCHOR} clause the view's named
 * window carries, the bucket expression the recompute oracle adds to its
 * {@code PARTITION BY}, and where the run's data starts.
 * <p>
 * The oracle is the same window over {@code PARTITION BY key, bucket}. That is exact for an
 * anchor whose value only moves forward in timestamp order, since a reset on a change of
 * value is then a reset per bucket. A zoned anchor moves backward across a DST fall-back
 * when its wall time falls inside the repeated hour, and there the view resets per run of
 * equal values rather than per bucket. A draw that asks for the run oracle may anchor
 * there, and its oracle numbers each key's runs of equal anchor values and partitions by
 * the run ({@link #oracleFrom}, {@link #oraclePartition}). Any other draw anchors at a wall
 * time its zone's fall-back does not repeat, so the bucket oracle stays exact.
 * <p>
 * {@code ANCHOR WEEKLY} is not a shape here: the parser rejects it.
 */
final class LiveViewFuzzAnchor {
    // A zone-less DAILY anchor floors with the three-argument timestamp_floor, which puts
    // every row below its 1970-01-01 origin into the origin's bucket. Data on that first day,
    // on both sides of the anchor's wall time, is what reaches that bucket.
    static final int DAILY_EPOCH = 3;
    // ANCHOR DAILY 'HH:MM' with no zone, over data from 2025-10 or 2026.
    static final int DAILY_PLAIN = 1;
    // ANCHOR DAILY 'HH:MM' 'zone', over data that crosses a DST change in most zones.
    static final int DAILY_ZONED = 2;
    // ANCHOR EXPRESSION timestamp_floor('1d', ts) or ('1h', ts).
    static final int EXPRESSION = 0;
    static final int MODE_COUNT = 4;
    // Each start sits a few days ahead of a DST change: the EU fall-back on 2025-10-26, the
    // US one on 2025-11-02, the EU spring-forward on 2026-03-29 and the Lord Howe fall-back
    // on 2026-04-05. 2026-01-01 crosses none.
    private static final String[] STARTS = {
            "2025-10-24T00:00:00.000000Z",
            "2026-03-26T00:00:00.000000Z",
            "2026-04-03T00:00:00.000000Z",
            "2026-01-01T00:00:00.000000Z"
    };
    // Europe/Berlin repeats 02:00 to 03:00 at its fall-back, so it never anchors at 02:30.
    // New York repeats 01:00 to 02:00 and Lord Howe 01:30 to 02:00; the other zones repeat
    // nothing.
    private static final String[] TIMES = {"00:00", "02:30", "09:30"};
    // UTC+05:00 is an offset zone, which the plan once put on the UTC grid. Pacific/Apia has
    // a whole-day historical shift; Australia/Lord_Howe changes by half an hour.
    private static final String[] ZONES = {
            "UTC", "Europe/Berlin", "America/New_York", "UTC+05:00", "Pacific/Apia", "Australia/Lord_Howe"
    };
    final String bucket;
    final String clause;
    // The clock the run pins before CREATE, below every row it generates.
    final long clockMicros;
    final long dataStartMicros;
    // A step bound, in micros, under which the shape's data covers the instants it is about, and
    // no day jumps; 0 leaves the step to the caller.
    final int denseStepMaxMicros;
    // Whether the oracle partitions by runs of equal anchor values rather than by bucket.
    final boolean isRunOracle;
    final int mode;

    private LiveViewFuzzAnchor(int mode, String clause, String bucket, long dataStartMicros, long clockMicros, boolean isRunOracle) {
        this(mode, clause, bucket, dataStartMicros, clockMicros, isRunOracle, 0);
    }

    private LiveViewFuzzAnchor(
            int mode,
            String clause,
            String bucket,
            long dataStartMicros,
            long clockMicros,
            boolean isRunOracle,
            int denseStepMaxMicros
    ) {
        this.mode = mode;
        this.clause = clause;
        this.bucket = bucket;
        this.dataStartMicros = dataStartMicros;
        this.clockMicros = clockMicros;
        this.isRunOracle = isRunOracle;
        this.denseStepMaxMicros = denseStepMaxMicros;
    }

    static LiveViewFuzzAnchor draw(Rnd rnd, int mode) {
        return draw(rnd, mode, false);
    }

    /**
     * @param isRunOracle whether the caller compares with {@link #oracleFrom} and
     *                    {@link #oraclePartition}, which lets a zoned anchor sit inside its
     *                    zone's repeated hour
     */
    static LiveViewFuzzAnchor draw(Rnd rnd, int mode, boolean isRunOracle) {
        final String time = TIMES[rnd.nextInt(TIMES.length)];
        final String origin = "'1970-01-01T" + time + ":00.000000Z'::timestamp";
        switch (mode) {
            case EXPRESSION -> {
                final String unit = rnd.nextInt(4) == 0 ? "1h" : "1d";
                final long start = MicrosTimestampDriver.floor(STARTS[3]);
                return new LiveViewFuzzAnchor(
                        mode,
                        "ANCHOR EXPRESSION timestamp_floor('" + unit + "', ts)",
                        "timestamp_floor('" + unit + "', ts)",
                        start,
                        start - 86_400_000_000L,
                        false
                );
            }
            case DAILY_PLAIN -> {
                final long start = MicrosTimestampDriver.floor(STARTS[rnd.nextInt(STARTS.length)]);
                return new LiveViewFuzzAnchor(
                        mode,
                        "ANCHOR DAILY '" + time + "'",
                        "timestamp_floor('1d', ts, " + origin + ")",
                        start,
                        start - 86_400_000_000L,
                        false
                );
            }
            case DAILY_ZONED -> {
                if (isRunOracle && rnd.nextInt(3) == 0) {
                    // Inside Berlin's repeated hour: the 2025-10-26 fall-back winds 03:00 CEST back
                    // to 02:00 CET at 01:00Z, so the rows of 01:00Z to 01:30Z read 02:00 to 02:29
                    // again and carry the previous day's anchor value, above the segment that
                    // started at 00:30Z. Dense data from 00:15Z covers that night.
                    final long start = MicrosTimestampDriver.floor("2025-10-26T00:15:00.000000Z");
                    return new LiveViewFuzzAnchor(
                            mode,
                            "ANCHOR DAILY '02:30' 'Europe/Berlin'",
                            "timestamp_floor_utc('1d', ts, '1970-01-01T02:30:00.000000Z'::timestamp, '+00:00', 'Europe/Berlin')",
                            start,
                            start - 86_400_000_000L,
                            true,
                            30_000_000
                    );
                }
                final String zone = ZONES[rnd.nextInt(ZONES.length)];
                final String zonedTime = !isRunOracle && "Europe/Berlin".equals(zone) && "02:30".equals(time) ? "09:30" : time;
                final String zonedOrigin = "'1970-01-01T" + zonedTime + ":00.000000Z'::timestamp";
                final long start = MicrosTimestampDriver.floor(STARTS[rnd.nextInt(STARTS.length)]);
                return new LiveViewFuzzAnchor(
                        mode,
                        "ANCHOR DAILY '" + zonedTime + "' '" + zone + "'",
                        "timestamp_floor_utc('1d', ts, " + zonedOrigin + ", '+00:00', '" + zone + "')",
                        start,
                        start - 86_400_000_000L,
                        isRunOracle
                );
            }
            case DAILY_EPOCH -> {
                // A non-midnight time, so the first day has rows below the origin. The clock
                // pins at the epoch, which stays below the data: a run's passes move it by
                // seconds, and the data starts half an hour in.
                final String epochTime = rnd.nextBoolean() ? "02:30" : "09:30";
                return new LiveViewFuzzAnchor(
                        mode,
                        "ANCHOR DAILY '" + epochTime + "'",
                        "timestamp_floor('1d', ts, '1970-01-01T" + epochTime + ":00.000000Z'::timestamp)",
                        MicrosTimestampDriver.floor("1970-01-01T00:30:00.000000Z"),
                        0,
                        false
                );
            }
            default -> throw new IllegalArgumentException("anchor mode=" + mode);
        }
    }

    /**
     * The FROM source of the oracle over {@code table}, whose columns are
     * {@code (ts, sym, i, x)}, filtered by {@code where}, which is empty or starts with a
     * space. A run oracle adds a {@code run} column: each key's runs of equal anchor values
     * in timestamp order, numbered from 1.
     */
    String oracleFrom(String table, String where) {
        if (!isRunOracle) {
            return table + where;
        }
        return "(SELECT ts, sym, i, x, sum(chg) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM ("
                + "SELECT ts, sym, i, x, CASE WHEN b = lag(b) OVER (PARTITION BY sym ORDER BY ts) THEN 0 ELSE 1 END AS chg FROM ("
                + "SELECT ts, sym, i, x, " + bucket + " AS b FROM " + table + where + ")))";
    }

    /**
     * What the oracle partitions by beside the key: the bucket, or the run.
     */
    String oraclePartition() {
        return isRunOracle ? "run" : bucket;
    }

    @Override
    public String toString() {
        return clause;
    }
}
