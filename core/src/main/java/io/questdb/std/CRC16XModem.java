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

package io.questdb.std;

/**
 * Used https://github.com/lammertb/libcrc/blob/master/src/crc16.c
 * and https://github.com/TobiasBengtsson/crc-fast-rs/blob/master/crc-fast-gen/src/lib.rs
 * as references.
 * <p>Usage:
 * <pre>
 *     short crc16 = 0;
 *     crc16 = CRC16XModem.calc(crc16, [... the data you want to compute for ...]);
 *     crc16 = CRC16XModem.calc(crc16, [... the data you want to compute for ...]);
 *     crc16 = CRC16XModem.calc(crc16, [... the data you want to compute for ...]);
 *     crc16 = CRC16XModem.calc(crc16, [... the data you want to compute for ...]);
 * </pre></p>
 * <p><strong>N.B.</strong>: There are different overloads of <code>calc</code> for different inputs.</p>
 * <p>You can keep on adding more data to the same CRC value with multiple calls to <code>calc()</code>.</p>
 */
public class CRC16XModem {
    private static final int POLYNOMIAL = 0x11021;
    // Compare with https://crccalc.com/?crc=0&method=CRC-16/XMODEM&datatype=ascii&outtype=hex
    private static final short[] TAB;

    private CRC16XModem() {
    }

    public static short calc(short crc, long ptr, int size) {
        for (int index = 0; index < size; ++index) {
            final byte b = Unsafe.getUnsafe().getByte(ptr + index);
            crc = calc(crc, b);
        }
        return crc;
    }

    public static short calc(short crc, int value) {
        // b0 is the least significant byte, i.e. extracting as little endian.
        byte b0 = (byte) value;
        byte b1 = (byte) (value >> 8);
        byte b2 = (byte) (value >> 16);
        byte b3 = (byte) (value >> 24);
        return calc(calc(calc(calc(crc, b0), b1), b2), b3);
    }

    // TODO(amunra): Test against what we get back from the `crc16-xmodem-fast` rust crate.
    public static short calc(short crc, byte b) {
        final short crcHiByte = (short) (crc >>> 8);
        final short tabIndex = (short) ((crc ^ (short) b) & (short) 0x00FF);
        return (short) (crcHiByte ^ TAB[tabIndex]);
    }

    static {
        TAB = new short[256];
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ POLYNOMIAL;
                } else {
                    crc = crc << 1;
                }
            }
            TAB[i] = (short) (crc & 0xFFFF);
        }
    }
}
