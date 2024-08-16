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

package io.questdb.network;

/**
 * pipe(2)-based suspend event object. Used on OS X and BSD in combination with kqueue.
 */
public class PipeSuspendEvent extends SuspendEvent {

    private final KqueueFacade kqf;
    private final long readEndFd;
    private final long writeEndFd;

    public PipeSuspendEvent(KqueueFacade kqf) {
        this.kqf = kqf;
        long fds = kqf.pipe();
        if (fds < 0) {
            throw NetworkError.instance(kqf.getNetworkFacade().errno(), "could not create PipeSuspendEvent");
        }
        this.readEndFd = kqf.getNetworkFacade().bumpFdCount((int) (fds >>> 32));
        this.writeEndFd = kqf.getNetworkFacade().bumpFdCount((int) fds);
    }

    @Override
    public void _close() {
        kqf.getNetworkFacade().close(readEndFd);
        kqf.getNetworkFacade().close(writeEndFd);
    }

    @Override
    public boolean checkTriggered() {
        return kqf.readPipe(readEndFd) == 1;
    }

    @Override
    public long getFd() {
        return readEndFd;
    }

    @Override
    public void trigger() {
        if (kqf.writePipe(writeEndFd) < 0) {
            throw NetworkError.instance(kqf.getNetworkFacade().errno())
                    .put("could not write to pipe [fd=").put(writeEndFd)
                    .put(']');
        }
    }
}
