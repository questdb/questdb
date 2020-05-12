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

#include <cfloat>
#include <cmath>
#include <bitset>
#include "sort.h"

#define MAX_VECTOR_SIZE 512

// Define function name depending on which instruction set we compile for
#if INSTRSET >= 10

#define QUICKSORT F_AVX512(quicksort)

#elif INSTRSET >= 8

#define QUICKSORT F_AVX2(quicksort)

#elif INSTRSET >= 5

#define QUICKSORT F_SSE41(quicksort)

#elif INSTRSET >= 2

#define QUICKSORT F_SSE2(quicksort)

#else

#endif

#ifdef QUICKSORT

int64_t QUICKSORT(int64_t *d, int64_t count) {
    Vec8q inputVec;
    const int step = 8;
    const auto *lim = d + count;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim_vec = lim - remainder;
    int64_t *pivotPointer = d + count / 2;
    int64_t p = *pivotPointer;
    Vec8q leftVec;
    Vec8q rightVec;
    Vec8qb bVec;
    int64_t *left[count];
    int64_t *right[count];
    int64_t leftPtr = 0;
    int64_t rightPtr = 0;
    int64_t leftMaskAsArr[8] = {0};
    int64_t rightMaskAsArr[8] = {0};
    for (; d < lim_vec; d += step) {
        _mm_prefetch(d + 63 * step, _MM_HINT_T1);
        inputVec.load(d);
        bVec = inputVec > p;
        leftVec = select(bVec, -1, inputVec);
        rightVec = select(bVec, inputVec, -1);
        int64_t rightCount = horizontal_count(bVec);
        int64_t leftCount = step - rightCount;
        uint8_t rightMask = to_bits(bVec);
        uint8_t leftMask = 256 - rightMask;
//        buildMaskArray(leftMask, leftMaskAsArr);
//        buildMaskArray(rightMask, leftMaskAsArr);
        Vec8q leftArrVec(0);
        leftArrVec.load_partial(leftCount, leftMaskAsArr);
        Vec8q rightArrVec(0);
        rightArrVec.load_partial(rightCount, rightMaskAsArr);
        Vec8q leftPermuted = lookup8(leftArrVec, leftVec).cutoff(leftCount);
        Vec8q rightPermuted = lookup8(rightArrVec, rightVec).cutoff(rightCount);
        int64_t leftElements[leftCount];
        int64_t rightElements[rightCount];
        leftPermuted.store_partial(leftCount, leftElements);
        rightPermuted.store_partial(rightCount, rightElements);
//        std::copy(leftElements, leftElements + leftCount, left + leftPtr);
//        std::copy(rightElements, rightElements + rightCount, right + rightPtr);
//        leftPtr += leftCount;
//        rightPtr += rightCount;
    }
    return p;
}

//TODO compile all 256 combinations (instead of creating them at runtime)
//void buildMaskArray(uint8_t mask, int64_t *a) {
//    std::bitset<16> bar(mask);
//    int64_t count = 0;
//    for (int i = 0; i < bar.size(); i++) {
//        if (bar.test(i)) {
//            a[count++] = i;
//        }
//    }
//}

#endif

#if INSTRSET < 5

// Dispatchers
LONG_LONG_DISPATCHER(quicksort)

#endif  // INSTRSET == 2
