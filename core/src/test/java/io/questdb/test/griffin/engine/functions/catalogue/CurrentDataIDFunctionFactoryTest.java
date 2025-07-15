/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CurrentDataIDFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testUninitializedDataID() throws Exception {
        assertSql("current_data_id\n\n",
                "select current_data_id();");
    }

    @Test
    public void testSetDataID() throws Exception {
        final Uuid newID = new Uuid();
        Rnd rnd = configuration.getRandom();
        newID.of(rnd.nextLong(), rnd.nextLong());
        sink.clear();
        newID.toSink(sink);
        engine.getDataID().set(newID);
        final String id = sink.toString();
        assertSql("current_data_id\n" + id + "\n",
                "select current_data_id();");
    }
}