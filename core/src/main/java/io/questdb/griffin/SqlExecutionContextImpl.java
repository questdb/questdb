/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.mp.*;
import io.questdb.std.IntStack;
import io.questdb.std.Rnd;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlExecutionContextImpl implements SqlExecutionContext {
    private final IntStack timestampRequiredStack = new IntStack();
    private final int workerCount;
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine cairoEngine;
    @Nullable
    private final MessageBus messageBus;
    private final MicrosecondClock clock;
    private RingQueue<TelemetryTask> telemetryQueue;
    private Sequence telemetryPubSeq;
    private TelemetryMethod telemetryMethod = this::storeTelemetryNoop;
    private BindVariableService bindVariableService;
    private CairoSecurityContext cairoSecurityContext;
    private Rnd random;
    private long requestFd = -1;
    private SqlExecutionInterruptor interruptor = SqlExecutionInterruptor.NOP_INTERRUPTOR;

    public SqlExecutionContextImpl(@Nullable MessageBus messageBus, int workerCount, CairoEngine cairoEngine) {
        this(cairoEngine.getConfiguration(), messageBus, workerCount, cairoEngine);
    }

    public SqlExecutionContextImpl(CairoConfiguration cairoConfiguration, @Nullable MessageBus messageBus, int workerCount, CairoEngine cairoEngine) {
        this.cairoConfiguration = cairoConfiguration;
        this.messageBus = messageBus;
        this.workerCount = workerCount;
        assert workerCount > 0;
        this.cairoEngine = cairoEngine;
        this.clock = cairoConfiguration.getMicrosecondClock();

        if(messageBus != null) {
            this.telemetryQueue = messageBus.getTelemetryQueue();
            this.telemetryPubSeq = messageBus.getTelemetryPubSequence();
            this.telemetryMethod = this::doStoreTelemetry;
        }
    }

    @Override
    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    @Override
    public CairoSecurityContext getCairoSecurityContext() {
        return cairoSecurityContext;
    }

    @Override
    @Nullable
    public MessageBus getMessageBus() {
        return messageBus;
    }

    @Override
    public boolean isTimestampRequired() {
        return timestampRequiredStack.notEmpty() && timestampRequiredStack.peek() == 1;
    }

    @Override
    public void popTimestampRequiredFlag() {
        timestampRequiredStack.pop();
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
        timestampRequiredStack.push(flag ? 1 : 0);
    }

    @Override
    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public Rnd getRandom() {
        return random != null ? random : SharedRandom.getRandom(cairoConfiguration);
    }

    @Override
    public void setRandom(Rnd rnd) {
        this.random = rnd;
    }

    @Override
    public CairoEngine getCairoEngine() {
        return cairoEngine;
    }

    @Override
    public long getRequestFd() {
        return requestFd;
    }

    @Override
    public SqlExecutionInterruptor getSqlExecutionInterruptor() {
        return interruptor;
    }

    @Override
    public void storeTelemetry(short event) {
        telemetryMethod.store(event);
    }

    private void doStoreTelemetry(short event) {
        final long cursor = telemetryPubSeq.next();
        TelemetryTask row = telemetryQueue.get(cursor);

        row.ts = clock.getTicks();
        row.event = event;
        telemetryPubSeq.done(cursor);
    }

    private void storeTelemetryNoop(short event) {
    }

    @FunctionalInterface
    private interface TelemetryMethod {
        void store(short event);
    }

    public SqlExecutionContextImpl with(
            @NotNull CairoSecurityContext cairoSecurityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd
    ) {
        this.cairoSecurityContext = cairoSecurityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        return this;
    }

    public SqlExecutionContextImpl with(
            long requestFd
    ) {
        this.requestFd = requestFd;
        return this;
    }

    public SqlExecutionContextImpl with(
            @NotNull CairoSecurityContext cairoSecurityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd,
            long requestFd,
            @Nullable SqlExecutionInterruptor interruptor
    ) {
        this.cairoSecurityContext = cairoSecurityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        this.requestFd = requestFd;
        this.interruptor = null == interruptor ? SqlExecutionInterruptor.NOP_INTERRUPTOR : interruptor;
        return this;
    }
}
