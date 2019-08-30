/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
