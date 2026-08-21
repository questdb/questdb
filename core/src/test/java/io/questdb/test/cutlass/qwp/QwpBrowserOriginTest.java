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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class QwpBrowserOriginTest {

    @Test
    public void testAcceptsSameOriginBrowserAuthorities() {
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://localhost:9000"),
                new Utf8String("localhost:9000")
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("HTTPS://QUESTDB.EXAMPLE.COM"),
                new Utf8String("questdb.example.com")
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://[::1]:9000"),
                new Utf8String("[::1]:9000")
        ));
    }

    @Test
    public void testRejectsCrossOriginAndMalformedOrigins() {
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://evil.example.com"),
                new Utf8String("questdb.example.com")
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("null"),
                new Utf8String("questdb.example.com")
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com/path"),
                new Utf8String("questdb.example.com")
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://user@questdb.example.com"),
                new Utf8String("questdb.example.com")
        ));
    }
}
