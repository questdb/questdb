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

package io.questdb.cutlass.line;

public class TruncatedLineProtoLexer extends LineProtoLexer {
    private boolean finishedLine;

    public TruncatedLineProtoLexer(int maxMeasurementSize) {
        super(maxMeasurementSize);
    }

    public long parseLine(long bytesPtr, long hi) {
        finishedLine = false;
        final long atPtr = parsePartial(bytesPtr, hi);
        if (finishedLine) {
            return atPtr;
        }
        clear();
        return -1;
    }

    @Override
    protected boolean partialComplete() {
        return finishedLine;
    }

    @Override
    protected void doSkipLineComplete() {
        finishedLine = true;
    }

    @Override
    protected void onEol() throws LineProtoException {
        finishedLine = true;
        super.onEol();
    }

    @Override
    public void parse(long bytesPtr, long hi) {
        throw new UnsupportedOperationException();
    }

    public CharSequenceCache getCharSequenceCache() {
        return charSequenceCache;
    }
}
