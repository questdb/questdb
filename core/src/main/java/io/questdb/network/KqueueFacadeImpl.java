/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class KqueueFacadeImpl implements KqueueFacade {
    public static final KqueueFacade INSTANCE = new KqueueFacadeImpl();

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents) {
        return KqueueAccessor.kevent(kq, changeList, nChanges, eventList, nEvents);
    }

    @Override
    public int kqueue() {
        return KqueueAccessor.kqueue();
    }

    @Override
    public long pipe() {
        return KqueueAccessor.pipe();
    }

    @Override
    public int readPipe(int fd) {
        return KqueueAccessor.readPipe(fd);
    }

    @Override
    public int writePipe(int fd) {
        return KqueueAccessor.writePipe(fd);
    }
}
