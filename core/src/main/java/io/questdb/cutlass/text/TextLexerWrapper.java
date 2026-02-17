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

import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

public class TextLexerWrapper implements QuietCloseable {
    private final TextConfiguration configuration;
    private final CsvTextLexer csvLexer;
    private GenericTextLexer genericTextLexer;

    public TextLexerWrapper(TextConfiguration configuration) {
        this.configuration = configuration;
        this.csvLexer = new CsvTextLexer(configuration);
    }

    @Override
    public void close() {
        Misc.free(csvLexer);
        genericTextLexer = Misc.free(genericTextLexer);
    }

    public AbstractTextLexer getLexer(byte delimiter) {
        if (delimiter == ',') {
            csvLexer.clear();
            return csvLexer;
        } else {
            if (genericTextLexer == null) {
                this.genericTextLexer = new GenericTextLexer(configuration);
            }
            this.genericTextLexer.clear();
            this.genericTextLexer.of(delimiter);
            return this.genericTextLexer;
        }
    }
}
