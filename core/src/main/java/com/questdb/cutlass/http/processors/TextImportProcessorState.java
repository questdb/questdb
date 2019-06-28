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

package com.questdb.cutlass.http.processors;

import com.questdb.cairo.CairoEngine;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.text.TextConfiguration;
import com.questdb.cutlass.text.TextLoader;
import com.questdb.std.Misc;
import com.questdb.std.Mutable;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;

import java.io.Closeable;

class TextImportProcessorState implements Mutable, Closeable {
    public static final int STATE_OK = 0;
    //    public static final int STATE_INVALID_FORMAT = 1;
    public static final int STATE_DATA_ERROR = 2;
    final TextLoader textLoader;
    public int columnIndex = 0;
    String stateMessage;
    boolean analysed = false;
    int messagePart = TextImportProcessor.MESSAGE_UNKNOWN;
    int responseState = TextImportProcessor.RESPONSE_PREFIX;
    boolean forceHeader = false;
    int state;
    boolean json = false;

    TextImportProcessorState(
            TextConfiguration configuration,
            CairoEngine engine
    ) throws JsonException {
        this.textLoader = new TextLoader(
                configuration,
                engine,
                DateLocaleFactory.INSTANCE,
                new DateFormatFactory(),
                com.questdb.std.microtime.DateLocaleFactory.INSTANCE,
                new com.questdb.std.microtime.DateFormatFactory()
        );
    }

    @Override
    public void clear() {
        responseState = TextImportProcessor.RESPONSE_PREFIX;
        columnIndex = 0;
        messagePart = TextImportProcessor.MESSAGE_UNKNOWN;
        analysed = false;
        state = STATE_OK;
        textLoader.clear();
    }

    @Override
    public void close() {
        clear();
        Misc.free(textLoader);
    }
}
