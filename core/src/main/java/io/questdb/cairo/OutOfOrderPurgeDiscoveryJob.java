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

package io.questdb.cairo;

import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.OutOfOrderPurgeDiscoveryTask;

public class OutOfOrderPurgeDiscoveryJob extends AbstractQueueConsumerJob<OutOfOrderPurgeDiscoveryTask> {

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MutableCharSink sink = new StringSink();

    public OutOfOrderPurgeDiscoveryJob(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderPurgeDiscoveryTask> queue,
            Sequence subSeq
    ) {
        super(queue, subSeq);
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        Path path = Path.getThreadLocal(configuration.getRoot());
        final OutOfOrderPurgeDiscoveryTask task = queue.get(cursor);
        path.concat(task.getTableName()).$$dir();

        sink.clear();
        TableUtils.setPathForPartition(sink, task.getPartitionBy(), task.getTimestamp(), false);

        ff.iterateDir(path.$$dir(), new FindVisitor() {
            @Override
            public void onFind(long name, int type) {
//                ff.findName()
            }
        });

        return true;
    }
}
