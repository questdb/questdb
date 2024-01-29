/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

/**
 * Hash function for native memory, most suitable for keys of 16 bytes and above.
 */
public final class Hash64FunctionFactory {

    private Hash64FunctionFactory() {
    }

    /**
     * Creates hash function suitable for usage in hash tables
     * with potentially large var-size keys (but not only).
     *
     * @param keySize key size in bytes in case of fixed-size key or -1 in case of var-size key
     */
    public static Hash64Function createFunction(long keySize) {
        //#if jdk.version==8
        if (keySize == -1 && Os.isX86()) {
            return new Crc32CFunction();
        }
        //#endif
        return Hash64MemFunction.INSTANCE;
    }
}
