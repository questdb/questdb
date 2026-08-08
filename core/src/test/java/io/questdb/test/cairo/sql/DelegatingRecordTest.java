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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class DelegatingRecordTest {

    @Test
    public void testOverridesAllRecordMethods() throws Exception {
        for (Method method : Record.class.getMethods()) {
            if (method.getDeclaringClass() != Record.class || method.isSynthetic() || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            final Method delegatingMethod = DelegatingRecord.class.getMethod(method.getName(), method.getParameterTypes());

            Assert.assertNotEquals(method.toGenericString(), Record.class, delegatingMethod.getDeclaringClass());
        }
    }
}
