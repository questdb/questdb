/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.collections;

import com.nfsdb.exceptions.JournalRuntimeException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DirectPagedBufferTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetWriteOffsetQuick() throws Exception {
        int pageLen = 128;

        try (DirectPagedBuffer pb = new DirectPagedBuffer(127)) {
            Assert.assertEquals(0, pb.calcOffset(pageLen - 4));
            Assert.assertEquals(pageLen, pb.calcOffset(5));
            Assert.assertEquals(pageLen + 5, pb.calcOffset(8));
        }
    }

    @Test
    public void testGetWriteOffsetWithChecks() throws Exception {
        int pageLen = 127;
        try (DirectPagedBuffer pb = new DirectPagedBuffer(pageLen)) {
            exception.expect(JournalRuntimeException.class);
            pb.calcOffsetChecked(129);
        }
    }
}