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

public interface SqlExecutionCircuitBreaker {
    SqlExecutionCircuitBreaker NOOP_CIRCUIT_BREAKER = new SqlExecutionCircuitBreaker() {
        @Override
        public void statefulThrowExceptionIfTripped() {
        }

        @Override
        public void setState() {
        }

        @Override
        public boolean checkIfTripped(long executionStartTimeUs, long fd) {
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
    };

    SqlExecutionCircuitBreakerConfiguration getConfiguration();

    /**
     * Uses internal state of the circuit breaker to assert conditions. This method also
     * throttles heavy checks. It is meant to be used in single-threaded applications.
     */
    void statefulThrowExceptionIfTripped();

    boolean checkIfTripped(long executionStartTimeUs, long fd);

    void setState();

    void setFd(long fd);

    long getFd();
}
