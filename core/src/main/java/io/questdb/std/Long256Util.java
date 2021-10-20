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

public class Long256Util {

    // this method is used by byte-code generator
    public static int compare(Long256 a, Long256 b) {

        if (a.getLong3() < b.getLong3()) {
            return -1;
        }

        if (a.getLong3() > b.getLong3()) {
            return 1;
        }

        if (a.getLong2() < b.getLong2()) {
            return -1;
        }

        if (a.getLong2() > b.getLong2()) {
            return 1;
        }

        if (a.getLong1() < b.getLong1()) {
            return -1;
        }

        if (a.getLong1() > b.getLong1()) {
            return 1;
        }

        return Long.compare(a.getLong0(), b.getLong0());
    }


    public static boolean isValidString(CharSequence text, int len) {
        return len > 2 && ((len & 1) == 0) && len < 67 && text.charAt(0) == '0' && text.charAt(1) == 'x';
    }
}
