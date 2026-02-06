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

package io.questdb.std;

public interface IOURingFacade {

    void close(long ptr);

    default long create(int capacity) {
        return create(capacity, 0);
    }

    long create(int capacity, int flags);

    int errno();

    boolean isAvailable();

    IOURing newInstance(int capacity);

    int registerBuffers(long ptr, long iovecs, int count);

    int registerFilesSparse(long ptr, int count);

    int submit(long ptr);

    int submitAndWait(long ptr, int waitNr);

    int unregisterBuffers(long ptr);

    int unregisterFiles(long ptr);

    int updateRegisteredFiles(long ptr, int offset, long fdsAddr, int count);
}
