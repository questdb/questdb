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

package io.questdb.std;

import io.questdb.std.ex.FatalError;
import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.io.IOException;

public final class Misc {
    public static final String EOL = "\r\n";
    public static final int CACHE_LINE_SIZE = 64;
    private final static ThreadLocal<StringSink> tlBuilder = new ThreadLocal<>(StringSink::new);

    private Misc() {
    }

    @SuppressWarnings("SameReturnValue")
    public static <T> T free(T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    public static StringSink getThreadLocalBuilder() {
        StringSink b = tlBuilder.get();
        b.clear();
        return b;
    }

    public static <T> void freeObjList(ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                list.setQuick(i, free(list.getQuick(i)));
            }
        }
    }

    public static <T> void freeObjListAndKeepObjects(ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                free(list.getQuick(i));
            }
        }
    }
}
