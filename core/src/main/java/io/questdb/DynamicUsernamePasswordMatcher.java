/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cutlass.pgwire.PGWireConfiguration;
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
    private final DirectUtf8Sink readOnlyPassword;
    private final ServerConfiguration serverConfiguration;
    private PGWireConfiguration pgwireConfiguration;

    /**
     * Creates instance of dynamic username/password matcher. When serverConfiguration is provided,
     * it acts as "factory" for the PG wire configuration. The matcher will be checking if
     * pgWireConfiguration changed on the server configuration.
     *
     * @param serverConfiguration server configuration that is triggering reload. It is nullable. When null,
     *                            username and password are not reloaded.
     * @param pgWireConfiguration the initial PG wire configuration instance.
     */
    public DynamicUsernamePasswordMatcher(@Nullable ServerConfiguration serverConfiguration, PGWireConfiguration pgWireConfiguration) {
        this.serverConfiguration = serverConfiguration;
        this.pgwireConfiguration = pgWireConfiguration;
        this.defaultPassword = new DirectUtf8Sink(4);
        this.defaultPassword.put(this.pgwireConfiguration.getDefaultPassword());
        this.readOnlyPassword = new DirectUtf8Sink(4);
        this.readOnlyPassword.put(this.pgwireConfiguration.getReadOnlyPassword());
        lock = new SimpleReadWriteLock();
    }

    @Override
    public void close() {
        Misc.free(defaultPassword);
        Misc.free(readOnlyPassword);
    }

    @Override
    public byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        if (serverConfiguration != null && serverConfiguration.getPGWireConfiguration() != pgwireConfiguration) {
            if (lock.writeLock().tryLock()) {
                try {
                    // Update the cached pgwire config
                    pgwireConfiguration = serverConfiguration.getPGWireConfiguration();
                    // Update the default and readonly user password sinks
                    defaultPassword.clear();
                    defaultPassword.put(pgwireConfiguration.getDefaultPassword());
                    readOnlyPassword.clear();
                    readOnlyPassword.put(pgwireConfiguration.getReadOnlyPassword());
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
            if (Chars.equals(username, pgwireConfiguration.getDefaultUsername())) {
                return verifyPassword(defaultPassword, passwordPtr, passwordLen);
            } else if (pgwireConfiguration.isReadOnlyUserEnabled() && Chars.equals(username, pgwireConfiguration.getReadOnlyUsername())) {
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
