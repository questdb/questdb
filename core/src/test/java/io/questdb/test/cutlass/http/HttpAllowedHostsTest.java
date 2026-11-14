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

import io.questdb.cutlass.http.HttpAllowedHosts;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class HttpAllowedHostsTest {
    private static final String ERROR_PREFIX = "expected comma-separated host names, domains written with a leading dot such as .example.com, or *; ";

    @Test
    public void testAnyHost() {
        Assert.assertSame(HttpAllowedHosts.ANY, HttpAllowedHosts.parse("*"));
        // * serves any host name wherever it is listed
        final HttpAllowedHosts hosts = HttpAllowedHosts.parse("questdb.example.com, *");
        Assert.assertSame(HttpAllowedHosts.ANY, hosts);
        assertAllowed(hosts, "rebind.example.com:9000", "a.b.example.net", "localhost", "[::1]");
        Assert.assertTrue(hosts.isAllowed(null));
    }

    @Test
    public void testBlankConfigurationServesDefaults() {
        Assert.assertSame(HttpAllowedHosts.DEFAULT, HttpAllowedHosts.parse(null));
        Assert.assertSame(HttpAllowedHosts.DEFAULT, HttpAllowedHosts.parse(""));
        Assert.assertSame(HttpAllowedHosts.DEFAULT, HttpAllowedHosts.parse(" \t "));
    }

    @Test
    public void testConfiguredNamesAndDomains() {
        final HttpAllowedHosts hosts = HttpAllowedHosts.parse("questdb.example.com, .corp.example.com,\tQuestDB.Monitoring., kube, questdb_1.example.com, my-questdb.example.com");
        assertAllowed(
                hosts,
                // an exact name, in any case, with or without the trailing dot of a fully qualified name
                "questdb.example.com",
                "questdb.example.com:9000",
                "QUESTDB.EXAMPLE.COM:9000",
                "questdb.example.com.",
                "questdb.example.com.:9000",
                // a domain covers itself and its subdomains
                "corp.example.com",
                "a.corp.example.com:9000",
                "a.b.corp.example.com",
                "A.Corp.Example.Com",
                // the configuration is case-insensitive and may end a name with a dot
                "questdb.monitoring:9000",
                "questdb_1.example.com:9000",
                "my-questdb.example.com",
                // listing changes nothing for the names served anyway
                "kube",
                "localhost:9000",
                "127.0.0.1"
        );
        assertRejected(
                hosts,
                // names are exact
                "a.questdb.example.com",
                "example.com",
                "questdb.example.co",
                "questdb.example.comx",
                // a domain matches whole labels
                "notcorp.example.com",
                "corp.example.com.evil.net",
                "corp.example.co",
                "rebind.example.net"
        );
    }

    @Test
    public void testDefaultRejectsNamesPublicDnsCanPoint() {
        assertRejected(
                HttpAllowedHosts.DEFAULT,
                "rebind.example.com",
                "rebind.example.com:9000",
                "REBIND.EXAMPLE.COM:9000",
                "rebind.example.com.",
                // a special-use name as a label, not as the domain
                "localhost.rebind.example.com",
                "local.rebind.example.com",
                "rebind.localhostx",
                "rebind.xlocal",
                "rebind.home.arpax",
                // a host name that only starts like an IPv4 address
                "127.0.0.1.rebind.example.com",
                // the last label is not a number, 0x needs hexadecimal digits
                "rebind.0x1g",
                "rebind.0x-1",
                "rebind.0x1@",
                "rebind.0a",
                "rebind.1a",
                // Kubernetes short names are not special-use
                "questdb.monitoring",
                "questdb.monitoring.svc",
                // an empty last label is not a number
                "rebind..",
                // bytes outside ASCII never match a special-use domain
                "rebind.\u00e9xample.com",
                "rebind.\u00e9cal"
        );
    }

    @Test
    public void testDefaultServesNamesPublicDnsCannotPoint() {
        Assert.assertTrue(HttpAllowedHosts.DEFAULT.isAllowed(null));
        assertAllowed(
                HttpAllowedHosts.DEFAULT,
                // no name
                "",
                ":9000",
                ".",
                // IPv4 and IPv6 addresses
                "127.0.0.1",
                "127.0.0.1:9000",
                "192.168.1.10:9000",
                "10.0.0.1.",
                "[::1]",
                "[::1]:9000",
                "[2001:db8::1]:443",
                // a browser takes a name whose last label is a number for an IPv4 address
                "rebind.1",
                "rebind.0x7f",
                "rebind.0X7F",
                "rebind.0x",
                "rebind.01",
                "1.2.3.0x0a:9000",
                // single-label names
                "localhost",
                "localhost:9000",
                "LOCALHOST:9000",
                "localhost.",
                "localhost.:9000",
                "questdb:9000",
                "\u00fcber",
                // special-use domains
                "app.localhost:9000",
                "APP.LOCALHOST",
                "mac.local",
                "questdb.monitoring.svc.cluster.local:9000",
                "foo.local.:9000",
                "host.docker.internal:9000",
                "nas.home.arpa",
                "site.test"
        );
    }

    @Test
    public void testInvalidEntries() {
        assertInvalid("a.example.com,,b.example.com", "empty entry, check for a stray comma: ");
        assertInvalid("a.example.com,", "empty entry, check for a stray comma: ");
        assertInvalid(" ,a.example.com", "empty entry, check for a stray comma: ");
        assertInvalid("*.example.com", "write .example.com to serve example.com and its subdomains, no other wildcard is supported: *.example.com");
        assertInvalid("questdb.example.com:9000", "host names are listed without a port: questdb.example.com:9000");
        assertInvalid("::1", "host names are listed without a port: ::1");
        assertInvalid("[::1]", "IP addresses are always served, list host names only: [::1]");
        assertInvalid("10.0.0.5", "IP addresses are always served, list host names only: 10.0.0.5");
        assertInvalid("10.0.0.5.", "IP addresses are always served, list host names only: 10.0.0.5.");
        assertInvalid("0x7f", "IP addresses are always served, list host names only: 0x7f");
        assertInvalid("host.0x1F", "IP addresses are always served, list host names only: host.0x1F");
        assertInvalid(".5", "IP addresses are always served, list host names only: .5");
        assertInvalid("a b.example.com", "a host name may contain only letters, digits, '-', '_' and '.': a b.example.com");
        assertInvalid("questdb.example.com/exec", "a host name may contain only letters, digits, '-', '_' and '.': questdb.example.com/exec");
        assertInvalid("admin@questdb.example.com", "a host name may contain only letters, digits, '-', '_' and '.': admin@questdb.example.com");
        assertInvalid("\u00e4.example.com", "a host name may contain only letters, digits, '-', '_' and '.': \u00e4.example.com");
        assertInvalid("a..example.com", "empty label: a..example.com");
        assertInvalid("...example.com", "empty label: ...example.com");
        assertInvalid(".", "empty host name: .");
        assertInvalid("..", "empty host name: ..");
        assertInvalid("questdb\u0001.example.com", "control character: questdb\u0001.example.com");
    }

    private static void assertAllowed(HttpAllowedHosts hosts, String... hostHeaders) {
        for (String host : hostHeaders) {
            Assert.assertTrue(host, hosts.isAllowed(new Utf8String(host)));
        }
    }

    private static void assertInvalid(String value, String message) {
        try {
            HttpAllowedHosts.parse(value);
            Assert.fail("expected an error for " + value);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(ERROR_PREFIX + message, e.getMessage());
        }
    }

    private static void assertRejected(HttpAllowedHosts hosts, String... hostHeaders) {
        for (String host : hostHeaders) {
            Assert.assertFalse(host, hosts.isAllowed(new Utf8String(host)));
        }
    }
}
