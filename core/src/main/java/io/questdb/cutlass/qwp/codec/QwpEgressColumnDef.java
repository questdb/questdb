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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.protocol.QwpConstants;

import java.nio.charset.StandardCharsets;

/**
 * Flyweight definition of one result-set column: name, QuestDB column type,
 * and the derived QWP wire type code. For decimal types the scale is recorded;
 * for geohash types the precision in bits.
 */
public class QwpEgressColumnDef {
    private static final byte[] EMPTY_NAME = new byte[0];
    private String name;
    /**
     * UTF-8 encoded {@link #name}, cached at {@link #of} time so the schema writer
     * doesn't have to re-encode per emit (and worst-case-size estimation can match the
     * real encoded length without a second allocation).
     */
    private byte[] nameUtf8 = new byte[0];
    private int precisionBits; // geohash only
    private int questdbColumnType;
    private int scale; // decimal only
    private byte wireType;

    public String getName() {
        return name;
    }

    public byte[] getNameUtf8() {
        return nameUtf8;
    }

    public int getPrecisionBits() {
        return precisionBits;
    }

    public int getQuestdbColumnType() {
        return questdbColumnType;
    }

    public int getScale() {
        return scale;
    }

    public byte getWireType() {
        return wireType;
    }

    public void of(String name, int questdbColumnType) {
        this.name = name;
        this.nameUtf8 = name == null ? EMPTY_NAME : name.getBytes(StandardCharsets.UTF_8);
        this.questdbColumnType = questdbColumnType;
        this.wireType = QwpColumnTypeMapper.toWireType(questdbColumnType);
        if (wireType == QwpConstants.TYPE_DECIMAL64
                || wireType == QwpConstants.TYPE_DECIMAL128
                || wireType == QwpConstants.TYPE_DECIMAL256) {
            this.scale = ColumnType.getDecimalScale(questdbColumnType);
        } else {
            this.scale = 0;
        }
        if (wireType == QwpConstants.TYPE_GEOHASH) {
            this.precisionBits = ColumnType.getGeoHashBits(questdbColumnType);
        } else {
            this.precisionBits = 0;
        }
    }
}
