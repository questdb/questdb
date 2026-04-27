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

package io.questdb.cutlass.pgwire;

/**
 * Flow-control signal thrown by {@code PGConnectionContext.handleClientOperation} when a
 * SQL function (e.g. {@code wait_wal_table}) has suspended its continuation. The PGServer
 * processor treats this as "the worker that resumes the continuation will re-register the
 * connection with the dispatcher" and therefore neither disconnects the client nor re-arms
 * the fd itself.
 *
 * <p>Singleton pattern mirrors other network flow-control exceptions
 * ({@link io.questdb.network.PeerIsSlowToWriteException} etc.) to stay allocation-free.
 */
public class OperationParkedException extends Exception {
    public static final OperationParkedException INSTANCE = new OperationParkedException();

    private OperationParkedException() {
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
