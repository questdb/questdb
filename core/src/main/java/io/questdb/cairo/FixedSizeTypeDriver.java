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
 * Base of the drivers for types stored as a fixed number of bytes per row. Storage-level
 * behaviour that depends only on the width lives here; per-type behaviour is abstract and
 * implemented by each leaf, so that javac lists every leaf when a method is added.
 * <p>
 * Each leaf declares its width once, as its data-movement tier ({@link #getMovement()}); the
 * width and log2 width derive from it. {@link ColumnType#isFixedSize(int)} is not the same fact:
 * SYMBOL and INTERVAL have a fixed width and a driver here, yet it reports them as not
 * fixed-size, and it reports an encoded geohash or decimal type as not fixed-size while their
 * tags are (a quirk that method keeps).
 */
public abstract class FixedSizeTypeDriver implements TypeDriver {
    private final PhysicalDescriptor.Movement movement;
    private final ColumnTypeTag tag;

    protected FixedSizeTypeDriver(ColumnTypeTag tag, PhysicalDescriptor.Movement movement) {
        assert movement != PhysicalDescriptor.Movement.VAR : "fixed-size type with a var-size layout: " + tag;
        this.tag = tag;
        this.movement = movement;
    }

    /**
     * Derived from the storage NULL: the low {@link #getWidth()} bytes of
     * {@code getNullLong(0)}, sign-extended, for a value up to 8 bytes wide; 0 for wider
     * values, which no long slot can hold.
     */
    @Override
    public long getNullAsLong() {
        return switch (movement) {
            case W1 -> (byte) getNullLong(0);
            case W2 -> (short) getNullLong(0);
            case W4 -> (int) getNullLong(0);
            case W8 -> getNullLong(0);
            case W16, W32, VAR -> 0L;
        };
    }

    @Override
    public final PhysicalDescriptor.Movement getMovement() {
        return movement;
    }

    /**
     * log2 of the width in bytes, as {@link ColumnType#pow2SizeOf(int)} reports it.
     */
    public final int getPow2Width() {
        return movement.pow2Size();
    }

    @Override
    public final ColumnTypeTag getTag() {
        return tag;
    }

    /**
     * Width of one value in bytes, as {@link ColumnType#sizeOf(int)} reports it.
     */
    public final int getWidth() {
        return movement.size();
    }

    /**
     * The name of a type whose only named encoding is its bare tag.
     */
    static String nameOfBareTag(int columnType, short tagCode, String name) {
        return columnType == tagCode ? name : ColumnType.UNKNOWN_NAME;
    }
}
