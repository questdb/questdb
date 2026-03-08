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

import io.questdb.cutlass.auth.UsernamePasswordMatcher;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_CREDENTIALS;
import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;

public class DynamicUsernamePasswordMatcher implements UsernamePasswordMatcher, QuietCloseable {
    private final DirectUtf8Sink defaultPassword;
    private final SimpleReadWriteLock lock;
    private final PGConfiguration pgWireConfig;
    private final DirectUtf8Sink readOnlyPassword;
    private final ServerConfiguration serverConfig;
    private long serverConfigVersion;

    /**
     * Creates instance of dynamic username/password matcher. When serverConfiguration is provided,
     * it acts as "factory" for the PG wire configuration. The matcher will be checking if
     * pgWireConfiguration changed on the server configuration.
     *
     * @param serverConfig server configuration that is triggering reload. It is nullable. When null,
     *                     username and password are not reloaded.
     * @param pgWireConfig the PG wire configuration instance.
     */
    public DynamicUsernamePasswordMatcher(@Nullable ServerConfiguration serverConfig, PGConfiguration pgWireConfig) {
        this.serverConfig = serverConfig;
        this.serverConfigVersion = serverConfig != null ? serverConfig.getVersion() : 0;
        this.pgWireConfig = pgWireConfig;
        this.defaultPassword = new DirectUtf8Sink(4);
        this.defaultPassword.put(pgWireConfig.getDefaultPassword());
        this.readOnlyPassword = new DirectUtf8Sink(4);
        this.readOnlyPassword.put(pgWireConfig.getReadOnlyPassword());
        this.lock = new SimpleReadWriteLock();
    }

    @Override
    public void close() {
        Misc.free(defaultPassword);
        Misc.free(readOnlyPassword);
    }

    @Override
    public byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        if (serverConfig != null && serverConfig.getVersion() != serverConfigVersion) {
            if (lock.writeLock().tryLock()) {
                try {
                    // Update the cached pgwire config, if no one did it in front of us
                    if (serverConfig.getVersion() != serverConfigVersion) {
                        // Update the default and readonly user password sinks
                        defaultPassword.clear();
                        defaultPassword.put(pgWireConfig.getDefaultPassword());
                        readOnlyPassword.clear();
                        readOnlyPassword.put(pgWireConfig.getReadOnlyPassword());
                        serverConfigVersion = serverConfig.getVersion();
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        lock.readLock().lock();
        try {
            if (username.length() == 0) {
                return AUTH_TYPE_NONE;
            }
            if (Chars.equals(username, pgWireConfig.getDefaultUsername())) {
                return verifyPassword(defaultPassword, passwordPtr, passwordLen);
            } else if (pgWireConfig.isReadOnlyUserEnabled() && Chars.equals(username, pgWireConfig.getReadOnlyUsername())) {
                return verifyPassword(readOnlyPassword, passwordPtr, passwordLen);
            } else {
                return AUTH_TYPE_NONE;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private byte verifyPassword(DirectUtf8Sink expectedPwd, long passwordPtr, int passwordLen) {
        if (Utf8s.equals(expectedPwd, passwordPtr, passwordLen)) {
            return AUTH_TYPE_CREDENTIALS;
        }
        return AUTH_TYPE_NONE;
    }
}
