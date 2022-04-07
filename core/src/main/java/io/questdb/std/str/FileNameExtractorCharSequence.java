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

package io.questdb.std.str;

public class FileNameExtractorCharSequence extends AbstractCharSequence {

    private final static ThreadLocal<FileNameExtractorCharSequence> SINGLETON =
            ThreadLocal.withInitial(FileNameExtractorCharSequence::new);

    private static final char separator;

    static {
        separator = System.getProperty("file.separator").charAt(0);
    }

    private CharSequence base;
    private int lo;
    private int hi;

    public static CharSequence get(CharSequence that) {
        return SINGLETON.get().of(that);
    }

    @Override
    public int length() {
        return hi - lo;
    }

    @Override
    public char charAt(int index) {
        return base.charAt(lo + index);
    }

    public CharSequence of(CharSequence base) {
        this.base = base;
        this.hi = base.length();
        this.lo = 0;
        for (int i = hi - 1; i > -1; i--) {
            if (base.charAt(i) == separator) {
                this.lo = i + 1;
                break;
            }
        }
        return this;
    }
}
