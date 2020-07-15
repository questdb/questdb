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

package io.questdb;

import io.questdb.mp.*;
import io.questdb.tasks.ColumnIndexerTask;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.VectorAggregateTask;
import org.jetbrains.annotations.NotNull;

public class MessageBusImpl implements MessageBus {
    private final RingQueue<ColumnIndexerTask> indexerQueue;
    private final MPSequence indexerPubSeq;
    private final MCSequence indexerSubSeq;

    private final RingQueue<VectorAggregateTask> vectorAggregateQueue;
    private final MPSequence vectorAggregatePubSeq;
    private final MCSequence vectorAggregateSubSeq;

    private final RingQueue<TelemetryTask> telemetryQueue;
    private final MPSequence telemetryPubSeq;
    private final SCSequence telemetrySubSeq;

    private final ServerConfiguration configuration;

    public MessageBusImpl(@NotNull ServerConfiguration configuration) {
        this.configuration = configuration;

        this.indexerQueue = new RingQueue<>(ColumnIndexerTask::new, 1024);
        this.indexerPubSeq = new MPSequence(indexerQueue.getCapacity());
        this.indexerSubSeq = new MCSequence(indexerQueue.getCapacity());

        this.vectorAggregateQueue = new RingQueue<>(VectorAggregateTask::new, 1024);
        this.vectorAggregatePubSeq = new MPSequence(vectorAggregateQueue.getCapacity());
        this.vectorAggregateSubSeq = new MCSequence(vectorAggregateQueue.getCapacity());

        this.telemetryQueue = new RingQueue<>(TelemetryTask::new, configuration.getTelemetryConfiguration().getQueueCapacity());
        this.telemetryPubSeq = new MPSequence(telemetryQueue.getCapacity());
        this.telemetrySubSeq = new SCSequence();

        indexerPubSeq.then(indexerSubSeq).then(indexerPubSeq);
        vectorAggregatePubSeq.then(vectorAggregateSubSeq).then(vectorAggregatePubSeq);
        telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
    }

    @Override
    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public Sequence getIndexerPubSequence() {
        return indexerPubSeq;
    }

    @Override
    public RingQueue<ColumnIndexerTask> getIndexerQueue() {
        return indexerQueue;
    }

    @Override
    public Sequence getIndexerSubSequence() {
        return indexerSubSeq;
    }

    @Override
    public RingQueue<VectorAggregateTask> getVectorAggregateQueue() {
        return vectorAggregateQueue;
    }

    @Override
    public Sequence getVectorAggregatePubSequence() {
        return vectorAggregatePubSeq;
    }

    @Override
    public Sequence getVectorAggregateSubSequence() {
        return vectorAggregateSubSeq;
    }

    @Override
    public RingQueue<TelemetryTask> getTelemetryQueue() {
        return telemetryQueue;
    }

    @Override
    public Sequence getTelemetryPubSequence() {
        return telemetryPubSeq;
    }

    @Override
    public SCSequence getTelemetrySubSequence() {
        return telemetrySubSeq;
    }
}
