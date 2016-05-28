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

public enum Command {

    SET_KEY_CMD(0x01),
    DELTA_REQUEST_CMD(0x02),
    CLIENT_READY_CMD(0x03),
    JOURNAL_DELTA_CMD(0x04),
    SERVER_READY_CMD(0x05),
    SERVER_HEARTBEAT(0x06),
    CLIENT_DISCONNECT(0x07),
    PROTOCOL_VERSION(0x08),
    HANDSHAKE_COMPLETE(0x09),
    AUTHORIZATION(0x0a),
    CLUSTER_VOTE(0x0b),
    SERVER_SHUTDOWN(0x0c),
    ELECTION(0x0d),
    ELECTED(0x0e),
    UNAUTHENTIC(0xFC),
    UNKNOWN_CMD(0xFE);

    public static final int BUFFER_SIZE = 3;
    public static final char AUTHENTICITY_KEY = 0xFAFB;

    private final int cmd;

    Command(int cmd) {
        this.cmd = cmd;
    }

    public static Command fromByte(byte b) {
        for (int i = 0, l = Command.values().length; i < l; i++) {
            Command c = Command.values()[i];
            if (c.cmd == b) {
                return c;
            }
        }
        return UNKNOWN_CMD;
    }

    public byte getCmd() {
        return (byte) cmd;
    }
}
