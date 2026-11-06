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

package io.questdb.test.tasks;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.tasks.PostingSealPurgeTask;
import org.junit.Assert;
import org.junit.Test;

public class PostingSealPurgeTaskTest {
    @Test
    public void testCopyAndClear() {
        TableToken token = new TableToken("t", "t~1", null, 1, false, false, false);
        PostingSealPurgeTask source = new PostingSealPurgeTask();
        PostingSealPurgeTask copy = new PostingSealPurgeTask();
        of(source, token, 17);
        source.copyTo(copy);
        source.clear();
        assertTask(copy, token, 17);
        source.copyTo(copy);
        Assert.assertTrue(copy.isEmpty());
        Assert.assertEquals("", copy.getIndexColumnName().toString());
    }

    @Test
    public void testQueueCopiesTasksAcrossGrowthAndReuse() {
        TableToken token = new TableToken("t", "t~1", null, 1, false, false, false);
        ConcurrentQueue<PostingSealPurgeTask> queue = ConcurrentQueue.createConcurrentQueue(PostingSealPurgeTask::new);
        PostingSealPurgeTask source = new PostingSealPurgeTask();
        PostingSealPurgeTask target = new PostingSealPurgeTask();
        final int taskCount = 1024;
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < taskCount; i++) {
                of(source, token, pass * taskCount + i);
                queue.enqueue(source);
                source.clear();
            }
            Assert.assertEquals(taskCount, queue.getApproximateCount());
            for (int i = 0; i < taskCount; i++) {
                Assert.assertTrue(queue.tryDequeue(target));
                assertTask(target, token, pass * taskCount + i);
                target.clear();
            }
            Assert.assertFalse(queue.tryDequeue(target));
            Assert.assertFalse(queue.hasAvailable());
        }
    }

    private static void assertTask(PostingSealPurgeTask task, TableToken token, int id) {
        Assert.assertFalse(task.isEmpty());
        Assert.assertSame(token, task.getTableToken());
        Assert.assertEquals(columnName(id), task.getIndexColumnName().toString());
        Assert.assertEquals(id - 1L, task.getPostingColumnNameTxn());
        Assert.assertEquals(id * 10L, task.getSealTxn());
        Assert.assertEquals(id * 100L, task.getPartitionTimestamp());
        Assert.assertEquals(id + 3L, task.getPartitionNameTxn());
        Assert.assertEquals(PartitionBy.DAY, task.getPartitionBy());
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, task.getTimestampType());
        Assert.assertEquals(id * 2L, task.getFromTableTxn());
        Assert.assertEquals(id * 2L + 1, task.getToTableTxn());
    }

    private static String columnName(int id) {
        return id % 2 == 0 ? "s" : "a_longer_index_column_name";
    }

    private static void of(PostingSealPurgeTask task, TableToken token, int id) {
        task.of(
                token,
                columnName(id),
                id - 1L,
                id * 10L,
                id * 100L,
                id + 3L,
                PartitionBy.DAY,
                ColumnType.TIMESTAMP_MICRO,
                id * 2L,
                id * 2L + 1
        );
    }
}
