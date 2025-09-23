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

package io.questdb.test.std.datetime.microtime;

import io.questdb.std.datetime.microtime.Micros;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjuster;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.time.DayOfWeek.MONDAY;
import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoUnit.*;
import static java.time.temporal.TemporalAdjusters.firstDayOfMonth;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.fail;

public class TimestampsBruteForceTest {

    private static final Function<ZonedDateTime, ZonedDateTime> HOURS_STEP = current -> current.plus(ThreadLocalRandom.current().nextInt((int) HOURS.toMillis(1), (int) HOURS.toMillis(12)), MILLIS);
    private static final Function<ZonedDateTime, ZonedDateTime> SECONDS_STEP = current -> current.plus(ThreadLocalRandom.current().nextInt(1, 20_000), ChronoUnit.MILLIS);
    private static final TemporalAdjuster TRUNCATE_TO_CENTURY = (temporal -> {
        int year = temporal.get(YEAR);
        int yearRemainder = year % 100;
        if (yearRemainder == 0) {
            // 1900, 2000,...
            year = year - 99;
        } else {
            year -= yearRemainder - 1;
        }
        return temporal.with(YEAR, year)
                .with(DAY_OF_YEAR, 1)
                .with(MICRO_OF_DAY, 0);
    });
    private static final TemporalAdjuster TRUNCATE_TO_DECADE = (temporal -> {
        // intentionally naive and different from the production impl
        String yearString = String.valueOf(temporal.get(YEAR));
        int shiftedYear = Integer.parseInt(replaceLastCharWithZero(yearString));
        return temporal.with(YEAR, shiftedYear)
                .with(DAY_OF_YEAR, 1)
                .with(MICRO_OF_DAY, 0);
    });
    private static final TemporalAdjuster TRUNCATE_TO_QUARTER = (temporal -> {
        int month = temporal.get(MONTH_OF_YEAR);
        int shiftedMonth;
        // this is intentionally naive, to have the code as straightforward as possible
        // and different from the production implementation under test
        switch (month) {
            case 1:
            case 2:
            case 3:
                shiftedMonth = 1;
                break;
            case 4:
            case 5:
            case 6:
                shiftedMonth = 4;
                break;
            case 7:
            case 8:
            case 9:
                shiftedMonth = 7;
                break;
            case 10:
            case 11:
            case 12:
                shiftedMonth = 10;
                break;
            default:
                throw new AssertionError("More than 12 months in a year, huh? month: " + month);
        }
        return temporal.with(MONTH_OF_YEAR, shiftedMonth)
                .with(DAY_OF_MONTH, 1)
                .with(MICRO_OF_DAY, 0);
    });

    @Test
    public void testFlooring_CENTURY() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(TRUNCATE_TO_CENTURY), Micros.floorCentury(tested)));
    }

    @Test
    public void testFlooring_DD() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.truncatedTo(DAYS), Micros.floorDD(tested)));
    }

    @Test
    public void testFlooring_DECADE() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(TRUNCATE_TO_DECADE), Micros.floorDecade(tested)));
    }

    @Test
    public void testFlooring_DOW() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(MONDAY).truncatedTo(DAYS), Micros.floorDOW(tested)));
    }

    @Test
    public void testFlooring_MM() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(firstDayOfMonth()).truncatedTo(DAYS), Micros.floorMM(tested)));
    }

    @Test
    public void testFlooring_MS_SS_MI_HH() {
        // testing 4 cases at once because it allows us to amortize cost of toEpochMicros()
        // separating this would make the test(s) run longer by a few 100s of ms.
        testFlooring(2, SECONDS_STEP,
                (expected, tested) -> {
                    assertEpochMicrosEquals(expected.truncatedTo(MILLIS), Micros.floorMS(tested));
                    assertEpochMicrosEquals(expected.truncatedTo(SECONDS), Micros.floorSS(tested));
                    assertEpochMicrosEquals(expected.truncatedTo(MINUTES), Micros.floorMI(tested));
                    assertEpochMicrosEquals(expected.truncatedTo(ChronoUnit.HOURS), Micros.floorHH(tested));
                });
    }

    @Test
    public void testFlooring_Quarter() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(TRUNCATE_TO_QUARTER), Micros.floorQuarter(tested)));
    }

    @Test
    public void testFlooring_YYYY() {
        testFlooring(40, HOURS_STEP,
                (expected, tested) -> assertEpochMicrosEquals(expected.with(firstDayOfYear()).truncatedTo(DAYS), Micros.floorYYYY(tested)));
    }

    private static void assertEpochMicrosEquals(ZonedDateTime expected, long epochMicros) {
        long expectedMicros = toEpochMicros(expected);
        if (expectedMicros != epochMicros) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
                    .withLocale(Locale.US)
                    .withZone(ZoneId.of("UTC"));
            fail("Epoch micros " + epochMicros + " (="
                    + dateTimeFormatter.format(Instant.ofEpochMilli(epochMicros / 1000))
                    + ") is not the same instant as the expected " + expectedMicros + " (="
                    + dateTimeFormatter.format(expected.toInstant()) + ")");
        }
    }

    private static String replaceLastCharWithZero(String yearString) {
        return yearString.substring(0, yearString.length() - 1) + "0";
    }

    private static void testFlooring(long yearsToTest, Function<ZonedDateTime, ZonedDateTime> stepFunction, BiConsumer<ZonedDateTime, Long> assertFunction) {
        ZoneId utc = ZoneId.of("UTC");
        ZonedDateTime current = ZonedDateTime.now(utc).withYear(1999);
        ZonedDateTime deadline = current.plusYears(yearsToTest);

        while (current.isBefore(deadline)) {
            long epochMicros = toEpochMicros(current);
            assertFunction.accept(current, epochMicros);
            current = stepFunction.apply(current);
        }
    }

    private static long toEpochMicros(ZonedDateTime zonedDateTime) {
        return TimeUnit.SECONDS.toMicros(zonedDateTime.toEpochSecond())
                + TimeUnit.NANOSECONDS.toMicros(zonedDateTime.getNano());
    }
}
