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

package io.questdb.cairo;

/**
 * Type driver for the stored decimal family: DECIMAL8 to DECIMAL256 are one type stored at
 * six widths, so they share one class with one instance per tag. Precision and scale are part
 * of the encoded column type and are passed as an argument where a method needs them. The
 * DECIMAL pseudo tag, which only resolves function overloads, has no driver.
 */
public final class DecimalTypeDriver extends FixedSizeTypeDriver {
    public static final DecimalTypeDriver DECIMAL128 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL128, 4);
    public static final DecimalTypeDriver DECIMAL16 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL16, 1);
    public static final DecimalTypeDriver DECIMAL256 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL256, 5);
    public static final DecimalTypeDriver DECIMAL32 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL32, 2);
    public static final DecimalTypeDriver DECIMAL64 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL64, 3);
    public static final DecimalTypeDriver DECIMAL8 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL8, 0);

    private DecimalTypeDriver(ColumnTypeTag tag, int pow2Width) {
        super(tag, pow2Width);
    }
}
