/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import java.io.Closeable;

public class RedBlackTree implements Mutable, Closeable {
    // P(8) + L + R + C(1) + REF
    private static final int BLOCK_SIZE = 8 + 8 + 8 + 1 + 8;
    private static final int O_LEFT = 8;
    private static final int O_RIGHT = 16;
    private static final int O_COLOUR = 24;
    private static final int O_REF = 25;

    private static final byte RED = 1;
    private static final byte BLACK = 0;
    private final MemoryPages mem;
    private final LongComparator comparator;
    private final LongIterator cursor = new LongIterator();
    private long root = -1;

    public RedBlackTree(LongComparator comparator, int keyPageSize) {
        this.comparator = comparator;
        this.mem = new MemoryPages(keyPageSize);
    }

    public void add(long value) {
        if (root == -1) {
            putParent(value);
            return;
        }

        comparator.setLeft(value);

        long p = root;
        long parent;
        int cmp;
        do {
            parent = p;
            long r = refOf(p);
            cmp = comparator.compare(r);
            if (cmp <= 0) {
                p = leftOf(p);
            } else {
                p = rightOf(p);
            }
        } while (p > -1);

        p = allocateBlock();
        setParent(p, parent);
        setRef(p, value);

        if (cmp <= 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    @Override
    public void clear() {
        root = -1;
        this.mem.clear();
    }

    @Override
    public void close() {
        mem.close();
    }

    public LongComparator getComparator() {
        return comparator;
    }

    public LongIterator iterator() {
        long p = root;
        if (p != -1) {
            long l;
            while ((l = leftOf(p)) != -1) {
                p = l;
            }
        }
        cursor.current = p;
        return cursor;
    }

    private static void setLeft(long blockAddress, long left) {
        Unsafe.getUnsafe().putLong(blockAddress + O_LEFT, left);
    }

    private static long rightOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_RIGHT);
    }

    private static long leftOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_LEFT);
    }

    private static void setParent(long blockAddress, long parent) {
        Unsafe.getUnsafe().putLong(blockAddress, parent);
    }

    private static long refOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_REF);
    }

    private static void setRef(long blockAddress, long recRef) {
        Unsafe.getUnsafe().putLong(blockAddress + O_REF, recRef);
    }

    private static void setRight(long blockAddress, long right) {
        Unsafe.getUnsafe().putLong(blockAddress + O_RIGHT, right);
    }

    private static long parentOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress);
    }

    private static long parent2Of(long blockAddress) {
        return parentOf(parentOf(blockAddress));
    }

    private static void setColor(long blockAddress, byte colour) {
        if (blockAddress == -1) {
            return;
        }
        Unsafe.getUnsafe().putByte(blockAddress + O_COLOUR, colour);
    }

    private static byte colorOf(long blockAddress) {
        return blockAddress == -1 ? BLACK : Unsafe.getUnsafe().getByte(blockAddress + O_COLOUR);
    }

    private static long successor(long current) {
        long p = rightOf(current);
        if (p != -1) {
            long l;
            while ((l = leftOf(p)) != -1) {
                p = l;
            }
        } else {
            p = parentOf(current);
            long ch = current;
            while (p != -1 && ch == rightOf(p)) {
                ch = p;
                p = parentOf(p);
            }
        }
        return p;
    }

    private long allocateBlock() {
        long p = mem.allocate(BLOCK_SIZE);
        setLeft(p, -1);
        setRight(p, -1);
        setColor(p, BLACK);
        return p;
    }

    private void fix(long x) {
        setColor(x, RED);

        while (x != -1 && x != root && colorOf(parentOf(x)) == RED) {
            if (parentOf(x) == leftOf(parent2Of(x))) {
                long y = rightOf(parent2Of(x));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parent2Of(x), RED);
                    x = parent2Of(x);
                } else {
                    if (x == rightOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parent2Of(x), RED);
                    rotateRight(parent2Of(x));
                }
            } else {
                long y = leftOf(parent2Of(x));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parent2Of(x), RED);
                    x = parent2Of(x);
                } else {
                    if (x == leftOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parent2Of(x), RED);
                    rotateLeft(parent2Of(x));
                }
            }
        }
        setColor(root, BLACK);
    }

    private void putParent(long value) {
        root = allocateBlock();
        setRef(root, value);
        setParent(root, -1);
        setLeft(root, -1);
        setRight(root, -1);
    }

    private void rotateLeft(long p) {
        if (p != -1) {
            long r = rightOf(p);
            setRight(p, leftOf(r));
            if (leftOf(r) != -1) {
                setParent(leftOf(r), p);
            }
            setParent(r, parentOf(p));
            if (parentOf(p) == -1) {
                root = r;
            } else if (leftOf(parentOf(p)) == p) {
                setLeft(parentOf(p), r);
            } else {
                setRight(parentOf(p), r);
            }
            setLeft(r, p);
            setParent(p, r);
        }
    }

    private void rotateRight(long p) {
        if (p != -1) {
            long l = leftOf(p);
            setLeft(p, rightOf(l));
            if (rightOf(l) != -1) {
                setParent(rightOf(l), p);
            }
            setParent(l, parentOf(p));
            if (parentOf(p) == -1) {
                root = l;
            } else if (rightOf(parentOf(p)) == p) {
                setRight(parentOf(p), l);
            } else {
                setLeft(parentOf(p), l);
            }
            setRight(l, p);
            setParent(p, l);
        }
    }

    public interface LongComparator {
        int compare(long right);

        void setLeft(long left);
    }

    public static class LongIterator {
        private long current;

        public boolean hasNext() {
            return current != -1;
        }

        public long next() {
            long result = refOf(current);
            current = successor(current);
            return result;
        }
    }
}
