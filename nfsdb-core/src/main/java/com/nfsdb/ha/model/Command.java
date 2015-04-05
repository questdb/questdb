/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ha.model;

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
