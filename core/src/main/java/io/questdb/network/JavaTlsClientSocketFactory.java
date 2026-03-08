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

package io.questdb.network;

import io.questdb.ClientTlsConfiguration;
import io.questdb.log.Log;

public final class JavaTlsClientSocketFactory implements SocketFactory {
    public static final SocketFactory DEFAULT = new JavaTlsClientSocketFactory(ClientTlsConfiguration.DEFAULT);
    public static final SocketFactory INSECURE_NO_VALIDATION = new JavaTlsClientSocketFactory(ClientTlsConfiguration.INSECURE_NO_VALIDATION);
    private final ClientTlsConfiguration tlsConfig;

    public JavaTlsClientSocketFactory(ClientTlsConfiguration tlsConfig) {
        this.tlsConfig = tlsConfig;
    }

    @Override
    public Socket newInstance(NetworkFacade nf, Log log) {
        return new JavaTlsClientSocket(nf, log, tlsConfig);
    }
}
