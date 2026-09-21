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

package io.questdb.test.griffin;

import io.questdb.griffin.CompiledQuery;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the numeric statement-type codes. They are not an internal enumeration: the QWP
 * egress writer puts the code on the {@code EXEC_DONE} frame, where the shipped client
 * decodes it as {@code onExecDone(short opType, ...)} against an unchanged protocol
 * version, and {@code PGPipelineEntry} stores it in {@code sys.telemetry}, which is
 * partitioned by day with a four-week TTL. Inserting a code in the middle therefore
 * gives one number two meanings - across connected old clients immediately, and across
 * telemetry rows for as long as the retention window holds rows written by both
 * binaries.
 * <p>
 * A new statement type must take the next free number at the end. If this test fails,
 * that is the fix; renumbering the expectations to match is not.
 */
public class CompiledQueryTypeCodeTest {

    @Test
    public void testEveryCodeKeepsItsWireValue() {
        Assert.assertEquals(0, CompiledQuery.NONE);
        Assert.assertEquals(1, CompiledQuery.SELECT);
        Assert.assertEquals(2, CompiledQuery.INSERT);
        Assert.assertEquals(3, CompiledQuery.TRUNCATE);
        Assert.assertEquals(4, CompiledQuery.ALTER);
        Assert.assertEquals(5, CompiledQuery.REPAIR);
        Assert.assertEquals(6, CompiledQuery.SET);
        Assert.assertEquals(7, CompiledQuery.DROP);
        Assert.assertEquals(8, CompiledQuery.PSEUDO_SELECT);
        Assert.assertEquals(9, CompiledQuery.CREATE_TABLE);
        Assert.assertEquals(10, CompiledQuery.INSERT_AS_SELECT);
        Assert.assertEquals(11, CompiledQuery.COPY_REMOTE);
        Assert.assertEquals(12, CompiledQuery.RENAME_TABLE);
        Assert.assertEquals(13, CompiledQuery.BACKUP_DATABASE);
        Assert.assertEquals(14, CompiledQuery.UPDATE);
        Assert.assertEquals(17, CompiledQuery.VACUUM);
        Assert.assertEquals(18, CompiledQuery.BEGIN);
        Assert.assertEquals(19, CompiledQuery.COMMIT);
        Assert.assertEquals(20, CompiledQuery.ROLLBACK);
        Assert.assertEquals(21, CompiledQuery.CREATE_TABLE_AS_SELECT);
        Assert.assertEquals(22, CompiledQuery.CHECKPOINT_CREATE);
        Assert.assertEquals(23, CompiledQuery.CHECKPOINT_RELEASE);
        Assert.assertEquals(24, CompiledQuery.DEALLOCATE);
        Assert.assertEquals(25, CompiledQuery.EXPLAIN);
        Assert.assertEquals(26, CompiledQuery.TABLE_RESUME);
        Assert.assertEquals(27, CompiledQuery.TABLE_SET_TYPE);
        Assert.assertEquals(28, CompiledQuery.CREATE_USER);
        Assert.assertEquals(29, CompiledQuery.ALTER_USER);
        Assert.assertEquals(30, CompiledQuery.CANCEL_QUERY);
        Assert.assertEquals(31, CompiledQuery.TABLE_SUSPEND);
        Assert.assertEquals(32, CompiledQuery.CREATE_MAT_VIEW);
        Assert.assertEquals(33, CompiledQuery.REFRESH_MAT_VIEW);
        Assert.assertEquals(34, CompiledQuery.CREATE_VIEW);
        Assert.assertEquals(35, CompiledQuery.COMPILE_VIEW);
        Assert.assertEquals(36, CompiledQuery.ALTER_VIEW);
        Assert.assertEquals(37, CompiledQuery.ALTER_STORAGE_POLICY);
        Assert.assertEquals(38, CompiledQuery.TABLE_REBASE);
        // Live views are the newest statement type and must sit at the end.
        Assert.assertEquals(39, CompiledQuery.CREATE_LIVE_VIEW);
        Assert.assertEquals(40, CompiledQuery.EMPTY);
        Assert.assertEquals(CompiledQuery.EMPTY, CompiledQuery.TYPES_COUNT);
    }
}
