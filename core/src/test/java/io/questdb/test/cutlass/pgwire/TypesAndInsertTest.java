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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.modern.TypesAndInsertModern;
import io.questdb.std.WeakSelfReturningObjectPool;
import org.junit.Test;

public class TypesAndInsertTest {

    @Test
    public void testReturnToPoolCausesStackOverflow() {
        WeakSelfReturningObjectPool<TypesAndInsertModern> typesAndSelectPool = new WeakSelfReturningObjectPool<>(TypesAndInsertModern::new, 1);
        TypesAndInsertModern i1 = typesAndSelectPool.pop();
        TypesAndInsertModern i2 = typesAndSelectPool.pop();
        TypesAndInsertModern i3 = typesAndSelectPool.pop();

        i1.close();
        i2.close();
        i3.close();
    }
}
