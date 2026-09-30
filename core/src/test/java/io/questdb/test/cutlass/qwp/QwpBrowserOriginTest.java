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

import io.questdb.cutlass.qwp.server.QwpBrowserAllowedOrigins;
import io.questdb.cutlass.qwp.server.QwpBrowserAuthorization;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpBrowserOriginTest {

    @Test
    public void testAcceptsSameOriginBrowserOrigins() {
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://localhost:9000"),
                new Utf8String("localhost:9000"),
                false
        ));
        // Schemes compare case-insensitively on both branches: the HTTPS pair
        // covers the secure prefix, the HTTP pair the plaintext one.
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("HTTPS://QUESTDB.EXAMPLE.COM"),
                new Utf8String("questdb.example.com"),
                true
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("HTTP://LOCALHOST:9000"),
                new Utf8String("localhost:9000"),
                false
        ));
        Assert.assertTrue(QwpIngressHttpProcessor.isSameOrigin(
                new Utf8String("http://[::1]:9000"),
                new Utf8String("[::1]:9000"),
                false
        ));
    }

    @Test
    public void testAllowedOriginsConfigurationAcceptsBrowserSerializedOrigins() {
        // Each value is exactly what a browser sends in the Origin header, so it
        // must load and then match that header byte for byte.
        for (String value : new String[]{
                "https://app.example.com", "http://localhost:3000", "https://app.example.com:8443",
                "http://my_app:3000", "https://my_app.example.com", "https://_app.example.com",
                "https://xn--bcher-kva.example", "https://app.example.com.", "http://127.0.0.1:3000",
                "http://10.0.0.255", "https://[::1]:8443", "https://[2001:db8::1]", "http://[::ffff:7f00:1]:3000",
                "http://[2001:db8:0:1:1:1:1:1]", "http://[2001:db8::1:0:0:1]", "http://[1::]"
        }) {
            QwpBrowserAllowedOrigins allowed = QwpBrowserAllowedOrigins.parse(value);
            Assert.assertTrue(value, allowed.isAllowed(new Utf8String(value)));
        }
    }

    @Test
    public void testAllowedOriginsConfigurationNamesCanonicalForm() {
        assertRejectedWith("https://App.Example.com", "browsers send https://app.example.com");
        assertRejectedWith("https://app.example.com:443", "browsers send https://app.example.com");
        assertRejectedWith("http://localhost:80", "browsers send http://localhost");
        assertRejectedWith("http://[0:0:0:0:0:0:0:1]:3000", "browsers send [::1]");
        assertRejectedWith("https://[2001:DB8::1]", "browsers send [2001:db8::1]");
        assertRejectedWith("http://127.000.000.001:3000", "not in canonical dotted-decimal form");
    }

    @Test
    public void testAllowedOriginsConfigurationRejectsNonOrigins() {
        for (String value : new String[]{
                ",", "https://app.example.com,", ",https://app.example.com",
                "https://*.example.com", "*", "null", "file://app.example.com",
                "https://", "https://user@app.example.com", "https://app.example.com/",
                "https://app.example.com/path", "https://app.example.com?x=1",
                "https://app.example.com#frag", "https://app.example.com:0",
                "https://app.example.com:65536", "https://app.example.com:",
                "https://app.example.com:0443", "https://app.example.com:bad",
                "https://app.example.com evil", "https://app.example.com\r\n",
                // written differently from what a browser sends, so it could never match
                "https://App.Example.com", "https://XN--bcher-kva.example", "HTTPS://app.example.com",
                "https://app.example.com:443", "http://localhost:80", "http://[::1]:80",
                "http://[0:0:0:0:0:0:0:1]:3000", "http://[::0:1]:3000", "http://[0::1]:3000",
                "https://[2001:DB8::1]", "https://[2001:0db8::1]", "https://[2001:db8:0:0:1:0:0:1]",
                "http://[1:0::]", "http://[::ffff:127.0.0.1]:3000", "http://[fe80::1%25en0]:3000",
                "http://127.000.000.001:3000", "http://2130706433:3000", "http://0177.0.0.1:3000",
                "http://127.1:3000", "http://0x7f.0.0.1:3000", "http://127.0.0.1.:3000", "http://256.0.0.1",
                "https://app..example.com", "https://.app.example.com", "https://[::1", "https://[::1]x"
        }) {
            try {
                QwpBrowserAllowedOrigins.parse(value);
                Assert.fail("accepted invalid browser origin config: " + value);
            } catch (IllegalArgumentException expected) {
                // A bad entry must fail the entire configuration, not silently widen the policy.
            }
        }
        Assert.assertSame(QwpBrowserAllowedOrigins.EMPTY, QwpBrowserAllowedOrigins.parse(""));
        Assert.assertSame(QwpBrowserAllowedOrigins.EMPTY, QwpBrowserAllowedOrigins.parse("  "));
    }

    @Test
    public void testAuthorizationSubprotocolDecoding() throws Exception {
        assertMemoryLeak(() -> {
            String value = "Bearer abc123";
            String credential = QwpWireTestFixtures.browserCredentialProtocol(value);
            try (DirectUtf8Sink out = new DirectUtf8Sink(64)) {
                Assert.assertTrue(QwpBrowserAuthorization.decode(new Utf8String("questdb.qwp.v1, " + credential), out));
                Assert.assertEquals(value, out.toString());
                for (String offer : new String[]{
                        credential + ", " + credential, "questdb.qwp.authorization.",
                        "questdb.qwp.authorization.!", credential + "=",
                        QwpWireTestFixtures.browserCredentialProtocol("Basic abc\r\nX: 1")
                }) {
                    out.clear();
                    Assert.assertTrue(QwpBrowserAuthorization.hasCredential(new Utf8String(offer)));
                    Assert.assertFalse(offer, QwpBrowserAuthorization.decode(new Utf8String(offer), out));
                }
                Assert.assertFalse(QwpBrowserAuthorization.hasCredential(new Utf8String("questdb.qwp.v1")));
            }
        });
    }

    @Test
    public void testAuthorizationSubprotocolTokenBoundaries() throws Exception {
        // hasCredential, decode and containsWebSocketProtocol split the offer the
        // same way: tokens end at commas, spaces and tabs around a token are
        // ignored, and empty or blank tokens are skipped.
        assertMemoryLeak(() -> {
            String value = "Basic YWRtaW46cXVlc3Q=";
            String credential = QwpWireTestFixtures.browserCredentialProtocol(value);
            Utf8String qwpV1 = QwpIngressHttpProcessor.WEBSOCKET_PROTOCOL_QWP_V1;
            try (DirectUtf8Sink out = new DirectUtf8Sink(64)) {
                Utf8String padded = new Utf8String(",, \t," + credential + " \t, ,\tquestdb.qwp.v1 ,");
                Assert.assertTrue(QwpBrowserAuthorization.hasCredential(padded));
                Assert.assertTrue(QwpBrowserAuthorization.decode(padded, out));
                Assert.assertEquals(value, out.toString());
                Assert.assertTrue(QwpIngressHttpProcessor.containsWebSocketProtocol(padded, qwpV1));

                out.clear();
                Utf8String duplicate = new Utf8String(credential + ", ,\t," + credential);
                Assert.assertTrue(QwpBrowserAuthorization.hasCredential(duplicate));
                Assert.assertFalse(QwpBrowserAuthorization.decode(duplicate, out));

                for (String offer : new String[]{
                        "", " , \t,", "questdb.qwp.authorization", "x-" + credential,
                        "questdb.qwp.v1 " + credential, "questdb.qwp.v1;" + credential
                }) {
                    out.clear();
                    Utf8String protocols = new Utf8String(offer);
                    Assert.assertFalse(offer, QwpBrowserAuthorization.hasCredential(protocols));
                    Assert.assertFalse(offer, QwpBrowserAuthorization.decode(protocols, out));
                    Assert.assertFalse(offer, QwpIngressHttpProcessor.containsWebSocketProtocol(protocols, qwpV1));
                }
                for (String offer : new String[]{"questdb.qwp.v1", "\tquestdb.qwp.v1\t,", ",questdb.qwp.v1", "a, ,questdb.qwp.v1"}) {
                    Assert.assertTrue(offer, QwpIngressHttpProcessor.containsWebSocketProtocol(new Utf8String(offer), qwpV1));
                }
            }
        });
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

    private static void assertRejectedWith(String value, String expectedReason) {
        try {
            QwpBrowserAllowedOrigins.parse(value);
            Assert.fail("accepted invalid browser origin config: " + value);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedReason));
        }
    }
}
