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

package io.questdb.cairo;

public class WriteMode {
    public static final int DIRECT = 0;
    public static final int WAL = 1;

    private WriteMode() {
    }

    public static boolean isDirectKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isWalKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static String nameOf(int writeMode) {
        switch (writeMode) {
            case 0:
                return "Direct";
            case 1:
                return "WAL";
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static int valueOf(CharSequence mode) {
        if (isDirectKeyword(mode)) return DIRECT;
        if (isWalKeyword(mode)) return WAL;
        throw new UnsupportedOperationException();
    }
}
