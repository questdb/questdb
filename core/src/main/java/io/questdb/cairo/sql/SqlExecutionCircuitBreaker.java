/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public interface SqlExecutionCircuitBreaker extends ExecutionCircuitBreaker {

    //Triggers timeout on first timeout check regardless of how much time elapsed since timer was reset 
    //(used mainly for testing) 
    long TIMEOUT_FAIL_ON_FIRST_CHECK = Long.MIN_VALUE;

    SqlExecutionCircuitBreaker NOOP_CIRCUIT_BREAKER = new SqlExecutionCircuitBreaker() {
        @Override
        public void statefulThrowExceptionIfTripped() {
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {

        }

        @Override
        public boolean checkIfTripped() {
            return false;
        }

        @Override
        public void resetTimer() {
        }

        @Override
        public boolean checkIfTripped(long millis, long fd) {
            return false;
        }

        @Override
        public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
            return null;
        }

        @Override
        public void setFd(long fd) {
        }

        @Override
        public long getFd() {
            return -1;
        }

        @Override
        public void unsetTimer() {
        }

        @Override
        public boolean isTimerSet() {
            return true;
        }
    };

    SqlExecutionCircuitBreakerConfiguration getConfiguration();

    /**
     * Uses internal state of the circuit breaker to assert conditions. This method also
     * throttles heavy checks. It is meant to be used in single-threaded applications.
     */
    void statefulThrowExceptionIfTripped();

    /**
     * Same as statefulThrowExceptionIfTripped() but doesn't throttle checks .
     * It is meant to be used in more coarse-grained processing, e.g. before native operation on whole page frame.
     */
    void statefulThrowExceptionIfTrippedNoThrottle();

    boolean checkIfTripped(long millis, long fd);

    void resetTimer();

    void setFd(long fd);

    long getFd();

    /* Unsets timer reset/power-up time so it won't time out on any check (unless resetTimer() is called)  */
    void unsetTimer();

    /* Returns true if time was reset/powered up (for current sql command) and false otherwise . */
    boolean isTimerSet();
}
