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

package io.questdb.test.cairo.view;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewCompilerTask;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class ViewCompilerTaskTest {
    @Test
    public void testClearResetsEveryField() {
        final ViewCompilerTask task = new ViewCompilerTask();
        task.isBusyRetry = true;
        task.tableToken = new TableToken("v", "v~1", null, 1, TableToken.Type.VIEW, true, false, false, false);
        task.updateTimestamp = 1_750_330_800_000_000L;
        task.clear();
        Assert.assertFalse(task.isBusyRetry);
        Assert.assertNull(task.tableToken);
        Assert.assertEquals(Numbers.LONG_NULL, task.updateTimestamp);
    }
}
