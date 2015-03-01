/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.io.parser;

import com.nfsdb.utils.Unsafe;

public class PipeParser extends AbstractTextParser {

    @Override
    public void parse(long lo, long len, int maxLine) {
        long hi = lo + len;
        long ptr = lo;

        OUT:
        while (ptr < hi) {
            byte c = Unsafe.getUnsafe().getByte(ptr++);

            if (useLineRollBuf) {
                putToRollBuf(c);
            }

            this.fieldHi++;

            if (delayedOutQuote && c != '"') {
                inQuote = delayedOutQuote = false;
            }

            switch (c) {
                case '"':
                    quote();
                    break;
                case '|':

                    if (eol) {
                        uneol(lo);
                    }

                    if (inQuote || ignoreEolOnce) {
                        break;
                    }
                    stashField();
                    fieldIndex++;
                    break;
                case '\r':
                case '\n':

                    if (inQuote) {
                        break;
                    }

                    if (eol) {
                        this.fieldLo = this.fieldHi;
                        break;
                    }

                    stashField();

                    if (ignoreEolOnce) {
                        ignoreEolOnce();
                        break;
                    }

                    triggerLine(ptr);

                    if (lineCount > maxLine) {
                        break OUT;
                    }
                    break;
                default:
                    if (eol) {
                        uneol(lo);
                    }
            }
        }

        if (useLineRollBuf) {
            return;
        }

        if (eol) {
            this.fieldLo = 0;
        } else {
            rollLine(lo, hi);
            useLineRollBuf = true;
        }
    }
}
