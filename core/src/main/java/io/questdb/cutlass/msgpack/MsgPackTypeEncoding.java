/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.msgpack;

public final class MsgPackTypeEncoding {

    // https://github.com/msgpack/msgpack/blob/master/spec.md

    public static final byte FIXINT_FIRST = (byte)0x00;
    public static final byte FIXINT_LAST = (byte)0x7f;
    public static final byte FIXMAP_FIRST = (byte)0x80;
    public static final byte FIXMAP_LAST = (byte)0x8f;
    public static final byte FIXARRAY_FIRST = (byte)0x90;
    public static final byte FIXARRAY_LAST = (byte)0x9f;
    public static final byte FIXSTR_FIRST = (byte)0xa0;
    public static final byte FIXSTR_LAST = (byte)0xbf;
    public static final byte NIL = (byte)0xc0;
    public static final byte _NEVER_USED = (byte)0xc1;
    public static final byte FALSE = (byte)0xc2;
    public static final byte TRUE = (byte)0xc3;
    public static final byte BIN8 = (byte)0xc4;
    public static final byte BIN16 = (byte)0xc5;
    public static final byte BIN32 = (byte)0xc6;
    public static final byte EXT8 = (byte)0xc7;
    public static final byte EXT16 = (byte)0xc8;
    public static final byte EXT32 = (byte)0xc9;
    public static final byte FLOAT32 = (byte)0xca;
    public static final byte FLOAT64 = (byte)0xcb;
    public static final byte UINT8 = (byte)0xcc;
    public static final byte UINT16 = (byte)0xcd;
    public static final byte UINT32 = (byte)0xce;
    public static final byte UINT64 = (byte)0xcf;
    public static final byte INT8 = (byte)0xd0;
    public static final byte INT16 = (byte)0xd1;
    public static final byte INT32 = (byte)0xd2;
    public static final byte INT64 = (byte)0xd3;
    public static final byte FIXEXT1 = (byte)0xd4;
    public static final byte FIXEXT2 = (byte)0xd5;
    public static final byte FIXEXT4 = (byte)0xd6;
    public static final byte FIXEXT8 = (byte)0xd7;
    public static final byte FIXEXT16 = (byte)0xd8;
    public static final byte STR8 = (byte)0xd9;
    public static final byte STR16 = (byte)0xda;
    public static final byte STR32 = (byte)0xdb;
    public static final byte ARRAY16 = (byte)0xdc;
    public static final byte ARRAY32 = (byte)0xdd;
    public static final byte MAP16 = (byte)0xde;
    public static final byte MAP32 = (byte)0xdf;
    public static final byte NEGATIVE_FIXINT_FIRST = (byte)0xe0;
    public static final byte NEGATIVE_FIXINT_LAST = (byte)0xff;

    // Lookup any encoding byte value to a `MsgPackType` int value.
    private static final int[] TYPE_MAPPINGS_TABLE = new int[256];

    private static final void register(byte typeEncoding, int type) {
        final int index = typeEncoding & 0xff;  // unsigned cast.
        TYPE_MAPPINGS_TABLE[index] = type;
    }

    private static final void registerRange(byte typeEncodingFirst, byte typeEncodingLast, int type) {
        int index = typeEncodingFirst & 0xff;  // unsigned cast
        final int last = typeEncodingLast & 0xff;  // unsigned cast
        for (; index <= last; ++index) {
            TYPE_MAPPINGS_TABLE[index] = type;
        }
    }

    static {
        registerRange(FIXINT_FIRST, FIXINT_LAST, MsgPackType.INT);
        registerRange(FIXMAP_FIRST, FIXMAP_LAST, MsgPackType.MAP);
        registerRange(FIXARRAY_FIRST, FIXARRAY_LAST, MsgPackType.ARRAY);
        registerRange(FIXSTR_FIRST, FIXSTR_LAST, MsgPackType.STR);
        register(NIL, MsgPackType.NIL);
        register(_NEVER_USED, MsgPackType._INVALID);
        register(FALSE, MsgPackType.BOOL);
        register(TRUE, MsgPackType.BOOL);
        register(BIN8, MsgPackType.BIN);
        register(BIN16, MsgPackType.BIN);
        register(BIN32, MsgPackType.BIN);
        register(EXT8, MsgPackType.EXT);
        register(EXT16, MsgPackType.EXT);
        register(EXT32, MsgPackType.EXT);
        register(FLOAT32, MsgPackType.FLOAT);
        register(FLOAT64, MsgPackType.FLOAT);
        register(UINT8, MsgPackType.INT);
        register(UINT16, MsgPackType.INT);
        register(UINT32, MsgPackType.INT);
        register(UINT64, MsgPackType.INT);
        register(INT8, MsgPackType.INT);
        register(INT16, MsgPackType.INT);
        register(INT32, MsgPackType.INT);
        register(INT64, MsgPackType.INT);
        register(FIXEXT1, MsgPackType.EXT);
        register(FIXEXT2, MsgPackType.EXT);
        register(FIXEXT4, MsgPackType.EXT);
        register(FIXEXT8, MsgPackType.EXT);
        register(FIXEXT16, MsgPackType.EXT);
        register(STR8, MsgPackType.INT);
        register(STR16, MsgPackType.INT);
        register(STR32, MsgPackType.INT);
        register(ARRAY16, MsgPackType.INT);
        register(ARRAY32, MsgPackType.INT);
        register(MAP16, MsgPackType.INT);
        register(MAP32, MsgPackType.INT);
        registerRange(NEGATIVE_FIXINT_FIRST, NEGATIVE_FIXINT_LAST, MsgPackType.INT);
    }

    /** Look up a `MsgPackType` for a given leading byte. */
    public static int toType(byte leading) {
        final int index = leading & 0xff;  // unsigned cast
        return TYPE_MAPPINGS_TABLE[index];
    }

    private MsgPackTypeEncoding() {}
}
