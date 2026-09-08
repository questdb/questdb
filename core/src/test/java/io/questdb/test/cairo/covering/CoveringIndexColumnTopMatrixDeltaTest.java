/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import org.junit.BeforeClass;

/**
 * The column-top matrix over the delta row-id encoding, i.e. POSTING_DELTA. It matters most for
 * {@code testSampleByFirstLastOnPartitionCarryingTheColumn}: SqlCodeGenerator routes SAMPLE BY
 * first/last on {@code IndexType.isBitmap()}, and every posting kind -- POSTING, POSTING_DELTA and
 * POSTING_EF -- has to stay off that factory, which walks the index through a frame cursor none of
 * them implement. The base class only exercised the default encoding.
 */
public class CoveringIndexColumnTopMatrixDeltaTest extends CoveringIndexColumnTopMatrixTest {
    @BeforeClass
    public static void setUpDelta() {
        setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "delta");
    }
}
