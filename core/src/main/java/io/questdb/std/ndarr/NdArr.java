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

package io.questdb.std.ndarr;

import io.questdb.std.Arc;
import io.questdb.std.DirectIntSequence;
import io.questdb.std.bytes.DirectByteSequence;

/** An N-dimensional Array */
public interface NdArr {
    /** Get the array's type */
    int getType();

    /** Constant returned from `NdArrFormat`. */
    int getFormat();

    /** Buffer to the CSR/CSC row or column pointers array. */
    Arc<DirectIntSequence> getSparsePointers();

    /** Buffer to the CSR/CSC column or row indices array. */
    Arc<DirectIntSequence> getSparseIndices();

    /** Buffer to the sparse values or dense flattened values. */
    Arc<DirectByteSequence> getValues();

    /** Number of values to skip reading before applying the strides logic to access the dense array */
    int getValuesOffset();

    /** Number of values readable, after skipping <code>getValuesOffset</code>. */
    int getValuesCount();

    /** Number of dimensions of this array. */
    int getDimsCount();

    /** Get the <i>N-th</i> dimension's size, as element count. */
    int getDim(int dimIndex);

    /**
     * Get the <i>N-th</i> dimension's stride, as element count.
     * <p>The returned stride expresses the number of elements to skip to read the next
     *    element in that dimension.</p>
     * <p><strong>IMPORTANT:</strong>
     * <ul>
     *     <li>A stride can be <code>0</code>, in case of broadcasting, or
     *         <code>&lt; 0</code> in case of reversing of data.</li>
     *     <li>Most libraries support strides expressed at the byte level.
     *         Since we also support packed arrays (e.g. bool bit arrays),
     *         the stride here is expressed in the element count space instead.</li>
     * </ul></p>
     * */
    int getStride(int dimIndex);
}
