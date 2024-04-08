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

public class KqueueAccessor {
    public static final short EVFILT_READ;
    public static final short SIZEOF_KEVENT;
    static final short EVFILT_VNODE;
    static final short NOTE_WRITE;
    static final short DATA_OFFSET;
    static final short EVFILT_WRITE;
    static final short EV_ADD;
    static final short EV_DELETE;
    static final short EV_ONESHOT;
    static final short EV_CLEAR;
    static final short FD_OFFSET;
    static final short FILTER_OFFSET;
    static final short FLAGS_OFFSET;

    static native short getDataOffset();

    static native short getEvAdd();

    static native short getEvDelete();
    static native short getEvClear();

    static native short getEvOneshot();

    static native short getEvfiltRead();

    static native short getEvfiltWrite();
    static native short getEvfiltVnode();
    static native short getNoteWrite();

    static native short getFdOffset();

    static native short getFilterOffset();

    static native short getFlagsOffset();

    static native short getSizeofKevent();

    static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents, int timeout);
    static native int keventRegister(int kq, long changeList, int nChanges);
    static native int keventGetBlocking(int kq, long eventList, int nEvents);
    static native long evSet(long ident, int filter, int flags, int fflags, long data);

    static native int kqueue();

    static native long pipe();

    static native int readPipe(int fd);

    static native int writePipe(int fd);

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        EVFILT_VNODE = getEvfiltVnode();
        NOTE_WRITE = getNoteWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
        EV_DELETE = getEvDelete();
        EV_CLEAR = getEvClear();
    }
}
