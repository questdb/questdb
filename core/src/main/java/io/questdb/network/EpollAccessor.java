/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class EpollAccessor {
    public static final short SIZEOF_EVENT;
    public static final int EPOLLIN;
    public static final int EPOLLOUT;
    public static final int EPOLL_CTL_ADD;
    public static final int EPOLL_CTL_MOD;
    public static final int EPOLL_CTL_DEL;
    static final short DATA_OFFSET;
    static final short EVENTS_OFFSET;
    static final int EPOLLONESHOT;
    static final int EPOLLET;

    static {
        DATA_OFFSET = getDataOffset();
        EVENTS_OFFSET = getEventsOffset();
        SIZEOF_EVENT = getEventSize();
        EPOLLIN = getEPOLLIN();
        EPOLLET = getEPOLLET();
        EPOLLOUT = getEPOLLOUT();
        EPOLLONESHOT = getEPOLLONESHOT();
        EPOLL_CTL_ADD = getCtlAdd();
        EPOLL_CTL_MOD = getCtlMod();
        EPOLL_CTL_DEL = getCtlDel();
    }

    static native long epollCreate();

    static native int epollCtl(long epfd, int op, long fd, long eventPtr);

    static native int epollWait(long epfd, long eventPtr, int eventCount, int timeout);

    static native short getDataOffset();

    static native short getEventsOffset();

    static native short getEventSize();

    static native int getEPOLLIN();

    static native int getEPOLLET();

    static native int getEPOLLOUT();

    static native int getEPOLLONESHOT();

    static native int getCtlAdd();

    static native int getCtlMod();

    static native int getCtlDel();
}
