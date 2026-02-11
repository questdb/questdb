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

package io.questdb.test.cutlass.line;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.network.Net;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LineSenderExceptionTest {

    @Test
    public void testEmptyMessage() {
        LineSenderException e = new LineSenderException(new RuntimeException());
        String message = e.getMessage();
        assertEquals("", message);
    }

    @Test
    public void testEmptyMessage_withErrNo() {
        LineSenderException e = new LineSenderException(new RuntimeException()).errno(10);
        String message = e.getMessage();
        assertEquals("[10]", message);
    }

    @Test
    public void testMessage_PutAsPrintableWithNonPrintableInput() {
        LineSenderException e = new LineSenderException("non-printable char: ").putAsPrintable("\u0101a");
        String message = e.getMessage();
        assertEquals("non-printable char: āa", message);

    }

    @Test
    public void testMessage_withAppendIPv4() {
        LineSenderException e = new LineSenderException("message ")
                .appendIPv4(Net.parseIPv4("10.0.0.1"));
        String message = e.getMessage();
        assertEquals("message 10.0.0.1", message);

    }

    @Test
    public void testMessage_withAppendIPv4andErrNo() {
        LineSenderException e = new LineSenderException("message ")
                .appendIPv4(Net.parseIPv4("10.0.0.1")).errno(1);
        String message = e.getMessage();
        assertEquals("[1] message 10.0.0.1", message);

    }

    @Test
    public void testMessage_withErrNo() {
        LineSenderException e = new LineSenderException("message").errno(10);
        String message = e.getMessage();
        assertEquals("[10] message", message);
    }

    @Test
    public void testMessage_withPutAppendIPv4andErrNo() {
        LineSenderException e = new LineSenderException("message ")
                .put("[ip=").appendIPv4(Net.parseIPv4("10.0.0.1")).put("]")
                .errno(1);
        String message = e.getMessage();
        assertEquals("[1] message [ip=10.0.0.1]", message);

    }

    @Test
    public void testMessage_withPutAsPrintable() {
        LineSenderException e = new LineSenderException("non-printable char: ").putAsPrintable("test+");
        String message = e.getMessage();
        assertEquals("non-printable char: test+", message);

    }

    @Test
    public void testMessage_withoutErrNo() {
        LineSenderException e = new LineSenderException("message");
        String message = e.getMessage();
        assertEquals("message", message);
    }
}
