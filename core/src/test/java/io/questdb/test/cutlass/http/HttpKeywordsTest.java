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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class HttpKeywordsTest {

    @Test
    public void testIsConnectionClose() {
        // RFC 9110 7.6.1: a comma-separated list of case-insensitive tokens, with optional
        // whitespace around each, and empty list elements, which recipients skip
        for (String connection : new String[]{
                "close",
                "Close",
                "CLOSE",
                "cLoSe",
                " close",
                "close ",
                "\tclose\t",
                "keep-alive, close",
                "close, keep-alive",
                "keep-alive,close",
                "Keep-Alive , Close",
                "Upgrade, close",
                "TE, close, Upgrade",
                ",close",
                "close,",
                ", ,\tclose , ,"
        }) {
            Assert.assertTrue(connection, HttpKeywords.isConnectionClose(new Utf8String(connection)));
        }
        Assert.assertFalse(HttpKeywords.isConnectionClose(null));
        for (String connection : new String[]{
                "",
                " ",
                ",",
                ", ,",
                "keep-alive",
                "Upgrade",
                "keep-alive, Upgrade",
                // whole tokens only
                "closed",
                "xclose",
                "enclose",
                "close-x",
                "clos",
                "lose",
                "close;q=1",
                "close keep-alive",
                "c lose",
                "keep-alive, closed",
                "closed, keep-alive",
                // five bytes, one of them off
                "\u00e7los",
                "cxose",
                "clxse",
                "cloxe",
                "closx"
        }) {
            Assert.assertFalse(connection, HttpKeywords.isConnectionClose(new Utf8String(connection)));
        }
    }
}
