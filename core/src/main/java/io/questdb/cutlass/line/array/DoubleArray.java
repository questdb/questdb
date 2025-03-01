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

package io.questdb.cutlass.line.array;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.Unsafe;

/**
 * Flattens an any dimensional double array (up to 16 dimensions) and stores it in the request buffer.
 * <p>
 * Checks for regularity in the array's shape during processing.
 * <p>
 * To enhance runtime performance, a separate method is enumerated for each dimension, rather than using reflection.
 */

// todo replace with JNI
public class DoubleArray {

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[] array) {
        checkCapacity(checkCapacity, (long) array.length * Double.BYTES);
        for (double v : array) {
            Unsafe.getUnsafe().putDouble(addr, v);
            addr += Double.BYTES;
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][] array) {
        int length = array[0].length;
        for (double[] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][] array) {
        int length = array[0].length;
        for (double[][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][] array) {
        int length = array[0].length;
        for (double[][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][] array) {
        int length = array[0].length;
        for (double[][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static long processArrayData(long addr, CheckCapacity checkCapacity, double[][][][][][][][][][][][][][][][] array) {
        int length = array[0].length;
        for (double[][][][][][][][][][][][][][][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            addr = processArrayData(addr, checkCapacity, v);
        }
        return addr;
    }

    public static void processArrayShape(long addr, double[] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
    }

    public static void processArrayShape(long addr, double[][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    public static void processArrayShape(long addr, double[][][][][][][][][][][][][][][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(addr, array.length);
        addr += Integer.BYTES;
        processArrayShape(addr, array[0]);
    }

    private static void checkCapacity(CheckCapacity func, long addr) {
        if (func != null) {
            func.checkCapacity(addr);
        }
    }
}
