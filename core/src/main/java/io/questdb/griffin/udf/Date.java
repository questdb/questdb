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
 ******************************************************************************/

package io.questdb.griffin.udf;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Wrapper class for QuestDB date values in UDFs.
 * <p>
 * QuestDB stores dates as milliseconds since Unix epoch (1970-01-01 00:00:00 UTC).
 * This class provides a type-safe wrapper for use in the simplified UDF API.
 * <p>
 * Note: Unlike {@link Timestamp} which has microsecond precision, Date has only
 * millisecond precision and represents a date without time zone information.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a UDF that extracts the year from a date
 * FunctionFactory yearExtractor = UDFRegistry.scalar(
 *     "my_year",
 *     Date.class,
 *     Integer.class,
 *     d -> d == null ? null : d.toLocalDate().getYear()
 * );
 * }</pre>
 */
public final class Date {

    private final long millis;

    /**
     * Creates a Date from milliseconds since epoch.
     *
     * @param millis milliseconds since Unix epoch
     */
    public Date(long millis) {
        this.millis = millis;
    }

    /**
     * Creates a Date from a Java Instant.
     *
     * @param instant the instant to convert
     * @return the Date, or null if instant is null
     */
    public static Date fromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        return new Date(instant.toEpochMilli());
    }

    /**
     * Creates a Date from a LocalDate (at start of day UTC).
     *
     * @param localDate the local date to convert
     * @return the Date, or null if localDate is null
     */
    public static Date fromLocalDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return new Date(localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    /**
     * Creates a Date from microseconds since epoch.
     * Note: Sub-millisecond precision is lost.
     *
     * @param micros microseconds since Unix epoch
     * @return the Date
     */
    public static Date fromMicros(long micros) {
        return new Date(micros / 1000L);
    }

    /**
     * Returns the date value in milliseconds since epoch.
     *
     * @return milliseconds since Unix epoch
     */
    public long getMillis() {
        return millis;
    }

    /**
     * Returns the date value in microseconds since epoch.
     * Note: This is just millis * 1000, no sub-millisecond precision.
     *
     * @return microseconds since Unix epoch
     */
    public long getMicros() {
        return millis * 1000L;
    }

    /**
     * Converts this date to a Java Instant.
     *
     * @return the equivalent Instant
     */
    public Instant toInstant() {
        return Instant.ofEpochMilli(millis);
    }

    /**
     * Converts this date to a LocalDate in UTC.
     *
     * @return the equivalent LocalDate
     */
    public LocalDate toLocalDate() {
        return Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Date date = (Date) o;
        return millis == date.millis;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(millis);
    }

    @Override
    public String toString() {
        return toLocalDate().toString();
    }
}
