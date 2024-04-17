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

import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.UsernamePasswordMatcher;
import io.questdb.std.Chars;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;

public class DynamicUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final ServerConfiguration serverConfiguration;
    private final SimpleReadWriteLock lock;
    private final DirectUtf8Sink defaultUserPasswordSink;
    private final DirectUtf8Sink readOnlyUserPasswordSink;
    private PGWireConfiguration pgwireConfiguration;

    public DynamicUsernamePasswordMatcher(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
        this.pgwireConfiguration = serverConfiguration.getPGWireConfiguration();

        this.defaultUserPasswordSink = new DirectUtf8Sink(4);
        this.defaultUserPasswordSink.put(this.pgwireConfiguration.getDefaultPassword());
        this.readOnlyUserPasswordSink = new DirectUtf8Sink(4);
        this.readOnlyUserPasswordSink.put(this.pgwireConfiguration.getReadOnlyPassword());

        lock = new SimpleReadWriteLock();
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        if (this.serverConfiguration.getPGWireConfiguration() != this.pgwireConfiguration) {

            if (this.lock.writeLock().tryLock()) {
                // Update the cached pgwire config
                this.pgwireConfiguration = this.serverConfiguration.getPGWireConfiguration();

                // Update the default and readonly user password sinks
                this.defaultUserPasswordSink.clear();
                this.defaultUserPasswordSink.put(this.pgwireConfiguration.getDefaultPassword());
                this.readOnlyUserPasswordSink.clear();
                this.readOnlyUserPasswordSink.put(this.pgwireConfiguration.getReadOnlyPassword());
            }
        }

        this.lock.readLock().lock();

        try {
            if (Chars.equals(username, this.pgwireConfiguration.getDefaultUsername())) {
                return Vect.memeq(this.defaultUserPasswordSink.ptr(), passwordPtr, passwordLen);
            } else if (Chars.equals(username, this.pgwireConfiguration.getReadOnlyUsername())) {
                if (!this.pgwireConfiguration.isReadOnlyUserEnabled()) {
                    return false;
                }
                return Vect.memeq(this.readOnlyUserPasswordSink.ptr(), passwordPtr, passwordLen);
            } else {
                return false;
            }

        }
        finally {
            this.lock.readLock().unlock();
        }
    }
}
