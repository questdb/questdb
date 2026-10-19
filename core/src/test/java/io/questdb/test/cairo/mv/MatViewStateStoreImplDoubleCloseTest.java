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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MatViewStateStoreImplDoubleCloseTest extends AbstractCairoTest {

    @Test
    public void closeIsIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym varchar, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by day");
            drainWalQueue();

            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            Assert.assertNotNull(store.getViewState(engine.verifyTableName("price_1h")));

            store.close();
            Assert.assertNull(
                    "first close must clear the state map",
                    store.getViewState(engine.verifyTableName("price_1h"))
            );

            store.close();
        });
    }
}
