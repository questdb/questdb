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

package io.questdb.test.tools;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class TlsProxyRule implements TestRule {
    private static final String DEFAULT_KEYSTORE = "/keystore/server.keystore";
    private static final char[] DEFAULT_KEYSTORE_PASSWORD = "questdb".toCharArray();

    private final String dstHost;
    private final int dstPort;
    private final String keystore;
    private final char[] keystorePassword;
    private int srcPort = -1;
    private TlsProxy tlsProxy;

    private TlsProxyRule(String dstHost, int dstPort, String keystore, char[] keystorePassword) {
        this.dstHost = dstHost;
        this.dstPort = dstPort;
        this.keystore = keystore;
        this.keystorePassword = keystorePassword;
    }

    public static TlsProxyRule toHostAndPort(String host, int port) {
        return new TlsProxyRule(host, port, DEFAULT_KEYSTORE, DEFAULT_KEYSTORE_PASSWORD);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                tlsProxy = new TlsProxy(dstHost, dstPort, keystore, keystorePassword);
                srcPort = tlsProxy.start();
                try {
                    base.evaluate();
                } finally {
                    tlsProxy.stop();
                }
            }
        };
    }

    public int getListeningPort() {
        if (srcPort < 0) {
            throw new IllegalStateException("test is not running yet!");
        }
        return srcPort;
    }

    public void killAfterAccepting() {
        if (tlsProxy != null) {
            tlsProxy.killAfterAccepting();
        }
    }

    public void killConnections() {
        if (tlsProxy != null) {
            tlsProxy.killConnections();
        }
    }
}
