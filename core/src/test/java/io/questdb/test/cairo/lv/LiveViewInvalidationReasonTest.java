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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the live-view invalidation reason that a base table metadata change reports. The WAL apply job
 * invalidates live views on this reason alone, apart from the mat view reason, so mat view invalidation
 * can change while live view invalidation stays as it is. {@code LiveViewBaseDdlTest} drives the same
 * reasons end to end through the apply job.
 */
public class LiveViewInvalidationReasonTest extends AbstractCairoTest {

    @Test
    public void testAlterOperationReportsLiveViewReasonOnlyForColumnDropRenameAndRetype() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken baseToken = new TableToken("base", "base~1", null, 1, true, false, false);
            for (short command = AlterOperation.DO_NOTHING; command <= AlterOperation.SET_EXPIRE; command++) {
                // Partition operations, which invalidate mat views today, never invalidate live views.
                final String expectedReason = switch (command) {
                    case AlterOperation.DROP_COLUMN -> "drop column operation";
                    case AlterOperation.RENAME_COLUMN -> "rename column operation";
                    case AlterOperation.CHANGE_COLUMN_TYPE -> "change column type operation";
                    default -> null;
                };
                try (AlterOperation alterOp = new AlterOperation()) {
                    alterOp.of(TableWriterTask.CMD_ALTER_TABLE, command, baseToken, baseToken.getTableId(), 0);
                    Assert.assertEquals("command=" + command, expectedReason, alterOp.liveViewInvalidationReason());
                }
            }
        });
    }

    @Test
    public void testMetadataChangeReportsNoLiveViewReasonByDefault() {
        final TableMetadataChange change = (svc, contextAllowsAnyStructureChanges) -> 0;
        Assert.assertNull(change.liveViewInvalidationReason());
    }
}
