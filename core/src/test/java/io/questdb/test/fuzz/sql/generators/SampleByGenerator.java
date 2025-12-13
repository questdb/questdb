/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.fuzz.sql.generators;

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.GeneratorContext;

/**
 * Generates QuestDB SAMPLE BY clauses for time-series aggregation.
 * <p>
 * Supports:
 * <ul>
 *   <li>SAMPLE BY interval (e.g., 1h, 30m, 1d)</li>
 *   <li>FROM timestamp TO timestamp</li>
 *   <li>FILL clause (NONE, PREV, NULL, LINEAR, or value list)</li>
 *   <li>ALIGN TO CALENDAR [TIME ZONE tz] [WITH OFFSET offset]</li>
 *   <li>ALIGN TO FIRST OBSERVATION</li>
 * </ul>
 */
public final class SampleByGenerator {

    // Sample intervals
    private static final String[] SAMPLE_INTERVALS = {
            "1s", "5s", "10s", "30s",
            "1m", "5m", "10m", "15m", "30m",
            "1h", "2h", "4h", "6h", "12h",
            "1d", "7d", "30d"
    };

    // FILL options
    private static final String[] FILL_OPTIONS = {
            "NONE", "PREV", "NULL", "LINEAR"
    };

    // Time zones
    private static final String[] TIME_ZONES = {
            "'UTC'", "'America/New_York'", "'Europe/London'", "'Asia/Tokyo'",
            "'America/Los_Angeles'", "'Europe/Paris'", "'Australia/Sydney'"
    };

    // Offset intervals
    private static final String[] OFFSET_INTERVALS = {
            "00:00", "01:00", "02:00", "06:00", "12:00", "-01:00", "-06:00"
    };

    private SampleByGenerator() {
        // Static utility class
    }

    /**
     * Generates a complete SAMPLE BY clause.
     */
    public static void generate(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("SAMPLE");
        ctx.keyword("BY");

        // Sample interval
        ctx.literal(SAMPLE_INTERVALS[rnd.nextInt(SAMPLE_INTERVALS.length)]);

        // Optional FROM...TO
        if (rnd.nextDouble() < 0.2) {
            generateFromTo(ctx);
        }

        // Optional FILL
        if (rnd.nextDouble() < 0.3) {
            generateFill(ctx);
        }

        // Optional ALIGN TO
        if (rnd.nextDouble() < 0.3) {
            generateAlignTo(ctx);
        }
    }

    /**
     * Generates just the SAMPLE BY interval part.
     */
    public static void generateSimple(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("SAMPLE");
        ctx.keyword("BY");
        ctx.literal(SAMPLE_INTERVALS[rnd.nextInt(SAMPLE_INTERVALS.length)]);
    }

    /**
     * Generates SAMPLE BY with a specific interval.
     */
    public static void generateWithInterval(GeneratorContext ctx, String interval) {
        ctx.keyword("SAMPLE");
        ctx.keyword("BY");
        ctx.literal(interval);
    }

    /**
     * Generates the FROM...TO clause.
     */
    public static void generateFromTo(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("FROM");
        generateTimestampLiteral(ctx);

        ctx.keyword("TO");
        generateTimestampLiteral(ctx);
    }

    /**
     * Generates the FILL clause.
     */
    public static void generateFill(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("FILL");
        ctx.openParen();

        // Choose fill type
        int fillType = rnd.nextInt(4);
        switch (fillType) {
            case 0:
                // Single fill option
                ctx.keyword(FILL_OPTIONS[rnd.nextInt(FILL_OPTIONS.length)]);
                break;
            case 1:
                // Multiple fill values for different columns
                int valueCount = 1 + rnd.nextInt(3);
                for (int i = 0; i < valueCount; i++) {
                    if (i > 0) {
                        ctx.comma();
                    }
                    ctx.keyword(FILL_OPTIONS[rnd.nextInt(FILL_OPTIONS.length)]);
                }
                break;
            case 2:
                // Numeric fill value
                ctx.literal(String.valueOf(rnd.nextInt(1000)));
                break;
            default:
                // Mix of options and values
                ctx.keyword(FILL_OPTIONS[rnd.nextInt(FILL_OPTIONS.length)]);
                ctx.comma();
                ctx.literal(String.valueOf(rnd.nextDouble() * 100));
                break;
        }

        ctx.closeParen();
    }

    /**
     * Generates the ALIGN TO clause.
     */
    public static void generateAlignTo(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("ALIGN");
        ctx.keyword("TO");

        if (rnd.nextBoolean()) {
            // ALIGN TO CALENDAR
            generateAlignToCalendar(ctx);
        } else {
            // ALIGN TO FIRST OBSERVATION
            generateAlignToFirstObservation(ctx);
        }
    }

    /**
     * Generates ALIGN TO CALENDAR clause.
     */
    public static void generateAlignToCalendar(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        ctx.keyword("CALENDAR");

        // Optional TIME ZONE
        if (rnd.nextDouble() < 0.4) {
            ctx.keyword("TIME");
            ctx.keyword("ZONE");
            ctx.literal(TIME_ZONES[rnd.nextInt(TIME_ZONES.length)]);
        }

        // Optional WITH OFFSET
        if (rnd.nextDouble() < 0.3) {
            ctx.keyword("WITH");
            ctx.keyword("OFFSET");
            ctx.literal("'" + OFFSET_INTERVALS[rnd.nextInt(OFFSET_INTERVALS.length)] + "'");
        }
    }

    /**
     * Generates ALIGN TO FIRST OBSERVATION clause.
     */
    public static void generateAlignToFirstObservation(GeneratorContext ctx) {
        ctx.keyword("FIRST");
        ctx.keyword("OBSERVATION");
    }

    /**
     * Generates a timestamp literal for FROM/TO clauses.
     */
    private static void generateTimestampLiteral(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();

        // Generate a reasonable timestamp
        int year = 2020 + rnd.nextInt(5);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);
        int hour = rnd.nextInt(24);
        int minute = rnd.nextInt(60);

        String timestamp = String.format("'%04d-%02d-%02dT%02d:%02d:00.000000Z'",
                year, month, day, hour, minute);
        ctx.literal(timestamp);
    }
}
