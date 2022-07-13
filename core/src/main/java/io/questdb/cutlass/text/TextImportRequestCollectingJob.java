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

package io.questdb.cutlass.text;
import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;

public class TextImportRequestCollectingJob extends SynchronizedJob {
    private final RingQueue<TextImportRequestTask> requestCollectingQueue;
    private final Sequence requestCollectingSubSeq;
    private final RingQueue<TextImportRequestTask> requestProcessingQueue;
    private final Sequence requestProcessingPubSeq;

    private final CancellationToken cancellationToken = new CancellationToken();

    public TextImportRequestCollectingJob(final CairoEngine engine) {
        MessageBus messageBus = engine.getMessageBus();
        this.requestCollectingQueue = messageBus.getTextImportRequestCollectingQueue();
        this.requestCollectingSubSeq = messageBus.getTextImportRequestCollectingSubSeq();
        this.requestProcessingQueue = messageBus.getTextImportRequestProcessingQueue();
        this.requestProcessingPubSeq = messageBus.getTextImportRequestProcessingPubSeq();
    }

    @Override
    protected boolean runSerially() {
        long collectingCursor = requestCollectingSubSeq.next();
        if (collectingCursor > -1) {
            TextImportRequestTask collectingTask = requestCollectingQueue.get(collectingCursor);
            collectingTask.setStatus(TextImportTask.STATUS_STARTED);
            if (collectingTask.isCancel()) {
                cancellationToken.cancel();
            } else {
                long processingCursor = requestProcessingPubSeq.next();
                if (processingCursor > -1) {
                    TextImportRequestTask processingTask = requestProcessingQueue.get(processingCursor);
                    cancellationToken.reset();
                    processingTask.setCancellationToken(cancellationToken);
                    processingTask.copyFrom(collectingTask);
                    requestProcessingPubSeq.done(processingCursor);
                } else {
                    collectingTask.setStatus(TextImportTask.STATUS_FAILED);
                }
            }
            requestCollectingSubSeq.done(collectingCursor);
            return true;
        }
        return false;
    }
}
