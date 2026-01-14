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

/**
 * Wrapper class for QuestDB timestamp values in UDFs.
 * <p>
 * QuestDB stores timestamps as microseconds since Unix epoch (1970-01-01 00:00:00 UTC).
 * This class provides a type-safe wrapper for use in the simplified UDF API.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a UDF that extracts the hour from a timestamp
 * FunctionFactory hourExtractor = UDFRegistry.scalar(
 *     "my_hour",
 *     Timestamp.class,
 *     Integer.class,
 *     ts -> ts == null ? null : ts.toInstant().atZone(ZoneOffset.UTC).getHour()
 * );
 * }</pre>
 */
public final class Timestamp {

    private final long micros;

    /**
     * Creates a Timestamp from microseconds since epoch.
     *
     * @param micros microseconds since Unix epoch
     */
    public Timestamp(long micros) {
        this.micros = micros;
    }

    /**
     * Creates a Timestamp from a Java Instant.
     *
     * @param instant the instant to convert
     * @return the Timestamp, or null if instant is null
     */
    public static Timestamp fromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        long epochSecond = instant.getEpochSecond();
        int nano = instant.getNano();
        return new Timestamp(epochSecond * 1_000_000L + nano / 1000L);
    }

    /**
     * Creates a Timestamp from milliseconds since epoch.
     *
     * @param millis milliseconds since Unix epoch
     * @return the Timestamp
     */
    public static Timestamp fromMillis(long millis) {
        return new Timestamp(millis * 1000L);
    }

    /**
     * Returns the timestamp value in microseconds since epoch.
     *
     * @return microseconds since Unix epoch
     */
    public long getMicros() {
        return micros;
    }

    /**
     * Returns the timestamp value in milliseconds since epoch.
     * Note: This truncates sub-millisecond precision.
     *
     * @return milliseconds since Unix epoch
     */
    public long getMillis() {
        return micros / 1000L;
    }

    /**
     * Converts this timestamp to a Java Instant.
     *
     * @return the equivalent Instant
     */
    public Instant toInstant() {
        long seconds = micros / 1_000_000L;
        int nanos = (int) ((micros % 1_000_000L) * 1000L);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Timestamp timestamp = (Timestamp) o;
        return micros == timestamp.micros;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(micros);
    }

    @Override
    public String toString() {
        return toInstant().toString();
    }
}
