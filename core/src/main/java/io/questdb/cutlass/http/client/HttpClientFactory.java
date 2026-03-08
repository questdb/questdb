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

package io.questdb.cutlass.http.client;

import io.questdb.ClientTlsConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.network.JavaTlsClientSocketFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import io.questdb.std.Os;

public class HttpClientFactory {

    public static HttpClient newInsecureTlsInstance() {
        return newInstance(DefaultHttpClientConfiguration.INSTANCE, JavaTlsClientSocketFactory.INSECURE_NO_VALIDATION);
    }

    public static HttpClient newInstance(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        switch (Os.type) {
            case Os.LINUX:
                return new HttpClientLinux(configuration, socketFactory);
            case Os.DARWIN:
            case Os.FREEBSD:
                return new HttpClientOsx(configuration, socketFactory);
            case Os.WINDOWS:
                return new HttpClientWindows(configuration, socketFactory);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static HttpClient newPlainTextInstance() {
        return newPlainTextInstance(DefaultHttpClientConfiguration.INSTANCE);
    }

    public static HttpClient newPlainTextInstance(HttpClientConfiguration configuration) {
        return newInstance(configuration, PlainSocketFactory.INSTANCE);
    }

    public static HttpClient newTlsInstance(HttpClientConfiguration configuration, ClientTlsConfiguration tlsConfig) {
        return newInstance(configuration, new JavaTlsClientSocketFactory(tlsConfig));
    }
}
