/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.text;

import io.questdb.std.SwarUtils;

public class GenericTextLexer extends AbstractTextLexer {
    private byte delimiter;
    private long delimiterMask;

    public GenericTextLexer(TextConfiguration textConfiguration) {
        super(textConfiguration);
    }

    public void of(byte delimiter) {
        this.delimiter = delimiter;
        this.delimiterMask = SwarUtils.broadcast(delimiter);
    }

    @Override
    protected void doSwitch(long lo, long hi, byte b) throws LineLimitException {
        if (b == delimiter) {
            onColumnDelimiter(lo);
        } else if (b == '"') {
            onQuote();
        } else if (b == '\n' || b == '\r') {
            onLineEnd(hi);
        } else {
            checkEol(lo);
        }
    }

    @Override
    protected long getDelimiterMask() {
        return delimiterMask;
    }
}

