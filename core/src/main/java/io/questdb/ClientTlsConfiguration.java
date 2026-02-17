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

package io.questdb;

public class ClientTlsConfiguration {
    public static final int TLS_VALIDATION_MODE_FULL = 0;
    public static final ClientTlsConfiguration DEFAULT = new ClientTlsConfiguration(null, null, TLS_VALIDATION_MODE_FULL);
    public static final int TLS_VALIDATION_MODE_NONE = 1;
    public static final ClientTlsConfiguration INSECURE_NO_VALIDATION = new ClientTlsConfiguration(null, null, TLS_VALIDATION_MODE_NONE);
    private final int tlsValidationMode;
    private final char[] trustStorePassword;
    private final String trustStorePath;

    public ClientTlsConfiguration(String trustStorePath, char[] trustStorePassword, int tlsValidationMode) {
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.tlsValidationMode = tlsValidationMode;
    }

    public int tlsValidationMode() {
        return tlsValidationMode;
    }

    public char[] trustStorePassword() {
        return trustStorePassword;
    }

    public String trustStorePath() {
        return trustStorePath;
    }
}
