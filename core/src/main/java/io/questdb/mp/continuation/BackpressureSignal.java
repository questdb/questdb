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

package io.questdb.mp.continuation;

/**
 * Park signal of the query-fiber tier: {@link QueryTask#runStep()} raises it when
 * the task's sink cannot accept more output right now (socket send buffer full,
 * egress credit exhausted). The task-runner fiber catches it, returns the task's
 * schedule gate to IDLE and invokes {@link QueryTask#onParked()}; a later external
 * event (fd WRITE-ready, CREDIT frame) re-launches the task via
 * {@link QueryFiberPool#launch(QueryTask)}.
 *
 * <p>Singleton without a stack trace: this is control flow, not an error, mirroring
 * {@code PeerIsSlowToReadException}.
 */
public final class BackpressureSignal extends Exception {
    public static final BackpressureSignal INSTANCE = new BackpressureSignal();

    private BackpressureSignal() {
        super("backpressure", null, false, false);
    }
}
