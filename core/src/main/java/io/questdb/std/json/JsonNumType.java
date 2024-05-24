/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std.json;


public class JsonNumType {
    /** Not a number */
    public final static int UNSET = 0;

    /** A binary64 number */
    public final static int FLOATING_POINT_NUMBER = 1;

    /** A signed integer that fits in a 64-bit word using two's complement */
    public final static int SIGNED_INTEGER = 2;

    /** A positive integer larger or equal to 1<<63 */
    public final static int UNSIGNED_INTEGER = 3;

    /** A big integer that does not fit in a 64-bit word */
    public final static int BIG_INTEGER = 4;
}
