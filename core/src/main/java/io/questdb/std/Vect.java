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

public final class Vect {

    public static native double avgDouble(long pDouble, long count);

    public static native double avgInt(long pInt, long count);

    public static native double avgLong(long pLong, long count);

    public static native boolean hasNull(long pInt, long count);

    public static native int getSupportedInstructionSet();

    public static String getSupportedInstructionSetName() {
        int inst = getSupportedInstructionSet();
        String base;
        if (inst >= 10) {
            base = "AVX512";
        } else if (inst >= 8) {
            base = "AVX2";
        } else if (inst >= 5) {
            base = "SSE4.1";
        } else if (inst >= 2) {
            base = "SSE2";
        } else {
            base = "Vanilla";
        }
        return " [" + base + "," + Vect.getSupportedInstructionSet() + "]";
    }

    public static native double maxDouble(long pDouble, long count);

    public static native int maxInt(long pInt, long count);

    public static native long maxLong(long pLong, long count);

    public static native double minDouble(long pDouble, long count);

    public static native int minInt(long pInt, long count);

    public static native long minLong(long pLong, long count);

    public static native double sumDouble(long pDouble, long count);

    public static native double sumDoubleKahan(long pDouble, long count);

    public static native double sumDoubleNeumaier(long pDouble, long count);

    public static native long sumInt(long pInt, long count);

    public static native long sumLong(long pLong, long count);

    public static native void radixSort(long pLongData, long count);
}
