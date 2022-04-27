/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.ctrl;

import java.io.IOException;

import io.questdb.cutlass.msgpack.MsgPackReader;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;

public class CtrlConnectionContext implements IOContext {

    private final IODispatcher<CtrlConnectionContext> dispatcher;
    private final long fd;
    private final MsgPackReader msgPackReader = new MsgPackReader();

    public CtrlConnectionContext(IODispatcher<CtrlConnectionContext> dispatcher, long fd) {
        this.dispatcher = dispatcher;
        this.fd = fd;
    }

    @Override
    public void close() {
        try {
            msgPackReader.close();
        } catch (IOException e) {
            // TODO: [adam] Can I throw instead?
            e.printStackTrace();
        }
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        // TODO: [adam] What's this for?
        return false;
    }

    @Override
    public IODispatcher<CtrlConnectionContext> getDispatcher() {
        return dispatcher;
    }

}
