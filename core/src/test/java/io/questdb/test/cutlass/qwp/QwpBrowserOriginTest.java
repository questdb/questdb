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
    public void testAcceptsSameOriginBrowserOrigins() {
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://localhost:9000"),
                new Utf8String("localhost:9000"),
                false
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("HTTPS://QUESTDB.EXAMPLE.COM"),
                new Utf8String("questdb.example.com"),
                true
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://[::1]:9000"),
                new Utf8String("[::1]:9000"),
                false
        ));
    }

    @Test
    public void testRejectsCrossOriginAndMalformedOrigins() {
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://evil.example.com"),
                new Utf8String("questdb.example.com"),
                false
        ));
        // Same authority length, different bytes. Every other rejected case
        // here returns on the scheme or the length check, so this is the only
        // assertion that exercises the authority comparison itself -- without
        // it, a loop that accepted every equal-length authority would pass.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://aaaa.example.com"),
                new Utf8String("bbbb.example.com"),
                false
        ));
        // A forged Host that reproduces a path-bearing Origin byte for byte:
        // only the explicit '/' rejection separates the two.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com/x"),
                new Utf8String("questdb.example.com/x"),
                false
        ));
        // Origin authority is a strict PREFIX of Host: a page on the default
        // port reaching QWP on another one. RFC 6454 makes the port part of
        // the origin, so this is cross-origin. Every other rejection here has
        // the origin authority longer than Host or differing within the
        // compared bytes, so relaxing the length equality to "reject only when
        // longer" would leave them all green and re-open CSWSH from any
        // same-host web app on a different port.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com"),
                new Utf8String("questdb.example.com:9000"),
                false
        ));
        // Degenerate authority: the byte loop never runs, so only the
        // explicit <= 0 guard rejects this.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://"),
                new Utf8String(""),
                false
        ));
        // HTTP/1.1 requires Host, but the parser does not, and byteAt on the
        // production DirectUtf8String is unchecked.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com"),
                null,
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("null"),
                new Utf8String("questdb.example.com"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com/path"),
                new Utf8String("questdb.example.com"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://user@questdb.example.com"),
                new Utf8String("questdb.example.com"),
                false
        ));
        // The four character-class terms of the byte loop, each with a Host
        // forged to mirror the Origin authority byte for byte so the length
        // check cannot decide the case first. Without an equal-length pair the
        // '@' case above returns at the length check (24 bytes vs 19) and none
        // of these terms is load-bearing in any assertion -- all four could be
        // deleted and the suite would stay green.
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://user@questdb.example.com"),
                new Utf8String("user@questdb.example.com"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com?a"),
                new Utf8String("questdb.example.com?a"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com#a"),
                new Utf8String("questdb.example.com#a"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example com"),
                new Utf8String("questdb.example com"),
                false
        ));
    }

    @Test
    public void testRejectsCrossSchemeBrowserOrigins() {
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("https://questdb.example.com"),
                new Utf8String("questdb.example.com"),
                false
        ));
        Assert.assertFalse(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://questdb.example.com"),
                new Utf8String("questdb.example.com"),
                true
        ));
    }
}
