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

package io.questdb.cairo.sql;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public interface SqlExecutionCircuitBreaker extends ExecutionCircuitBreaker {

    int STATE_OK = 0;
    SqlExecutionCircuitBreaker NOOP_CIRCUIT_BREAKER = new SqlExecutionCircuitBreaker() {
        @Override
        public void cancel() {
        }

        @Override
        public boolean checkIfTripped() {
            return false;
        }

        @Override
        public boolean checkIfTripped(long millis, long fd) {
            return false;
        }

        @Override
        public AtomicBoolean getCancelledFlag() {
            return null;
        }

        @Override
        public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
            return null;
        }

        @Override
        public long getFd() {
            return -1L;
        }

        @Override
        public int getState() {
            return STATE_OK;
        }

        @Override
        public int getState(long millis, long fd) {
            return STATE_OK;
        }

        @Override
        public long getTimeout() {
            return -1L;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public boolean isTimerSet() {
            return true;
        }

        @Override
        public void resetTimer() {
        }

        @Override
        public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        }

        @Override
        public void setFd(long fd) {
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
        }

        @Override
        public void unsetTimer() {
        }
    };
    int STATE_TIMEOUT = STATE_OK + 1; // 1
    int STATE_BROKEN_CONNECTION = STATE_TIMEOUT + 1; // 2
    int STATE_CANCELLED = STATE_BROKEN_CONNECTION + 1; // 3
    // Triggers timeout on first timeout check regardless of how much time elapsed since timer was reset
    // (used mainly for testing)
    long TIMEOUT_FAIL_ON_FIRST_CHECK = Long.MIN_VALUE;

    /**
     * Trigger this circuit breaker to fail on next check.
     */
    void cancel();

    boolean checkIfTripped(long millis, long fd);

    AtomicBoolean getCancelledFlag();

    @Nullable
    SqlExecutionCircuitBreakerConfiguration getConfiguration();

    long getFd();

    /**
     * Similar to checkIfTripped() method but returns int value describing reason for tripping.
     *
     * @return circuit breaker state, one of: <br>
     * - {@link #STATE_OK} <br>
     * - {@link #STATE_CANCELLED} <br>
     * - {@link #STATE_BROKEN_CONNECTION} <br>
     * - {@link #STATE_TIMEOUT} <br>
     */
    int getState();

    /**
     * Similar to checkIfTripped(long millis, long fd) method but returns int value describing reason for tripping.
     *
     * @return circuit breaker state, one of: <br>
     * - {@link #STATE_OK} <br>
     * - {@link #STATE_CANCELLED} <br>
     * - {@link #STATE_BROKEN_CONNECTION} <br>
     * - {@link #STATE_TIMEOUT} <br>
     */
    int getState(long millis, long fd);

    long getTimeout();

    boolean isThreadSafe();

    /**
     * Checks if timer is due.
     *
     * @return true if time was reset/powered up (for current sql command) and false otherwise
     */
    boolean isTimerSet();

    void resetTimer();

    void setCancelledFlag(AtomicBoolean cancelled);

    void setFd(long fd);

    /**
     * Uses internal state of the circuit breaker to assert conditions. This method also
     * throttles heavy checks. It is meant to be used in single-threaded applications.
     */
    void statefulThrowExceptionIfTripped();

    /**
     * Same as statefulThrowExceptionIfTripped() but doesn't throttle checks.
     * It is meant to be used in more coarse-grained processing, e.g. before native operation on whole page frame.
     */
    void statefulThrowExceptionIfTrippedNoThrottle();

    /**
     * Unsets timer reset/power-up time, so it won't time out on any check (unless resetTimer() is called).
     */
    void unsetTimer();
}
