/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.net.ha.model;

public final class Command {
    public static final byte SET_KEY_CMD = 0x01;
    public static final byte DELTA_REQUEST_CMD = 0x02;
    public static final byte CLIENT_READY_CMD = 0x03;
    public static final byte JOURNAL_DELTA_CMD = 0x04;
    public static final byte SERVER_READY_CMD = 0x05;
    public static final byte SERVER_HEARTBEAT = 0x06;
    public static final byte CLIENT_DISCONNECT = 0x07;
    public static final byte PROTOCOL_VERSION = 0x08;
    public static final byte HANDSHAKE_COMPLETE = 0x09;
    public static final byte AUTHORIZATION = 0x0a;
    public static final byte CLUSTER_VOTE = 0x0b;
    public static final byte SERVER_SHUTDOWN = 0x0c;
    public static final byte ELECTION = 0x0d;
    public static final byte ELECTED = 0x0e;
    public static final byte UNAUTHENTIC = (byte) 0xFC;
    public static final byte UNKNOWN_CMD = (byte) 0xFE;
    public static final int BUFFER_SIZE = 3;
    public static final char AUTHENTICITY_KEY = 0xFAFB;
}
