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

package io.questdb.cutlass.http;

import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.str.Utf8Sink;

public interface HttpChunkedResponse extends Utf8Sink {
    void bookmark();

    void done() throws PeerDisconnectedException, PeerIsSlowToReadException;

    HttpResponseHeader headers();

    boolean resetToBookmark();

    void sendChunk(boolean done) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendHeader() throws PeerDisconnectedException, PeerIsSlowToReadException;

    void shutdownWrite();

    void status(int status, CharSequence contentType);

    int writeBytes(long srcAddr, int len);
}
