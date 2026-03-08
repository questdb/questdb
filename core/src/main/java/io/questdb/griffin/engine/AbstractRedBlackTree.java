/*******************************************************************************
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

package io.questdb.griffin.engine;

import io.questdb.cairo.Reopenable;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * A native memory heap-based red-black tree. Used in ORDER BY factories.
 * <p>
 * Each block ref value stores compressed offsets. A compressed offset contains
 * an offset to the address of the referenced block in the heap memory
 * compressed to an int. Block addresses are 8-byte aligned.
 */
public abstract class AbstractRedBlackTree implements Mutable, Reopenable {
    protected static final byte BLACK = 0;
    protected static final int EMPTY = -1; // empty reference; used to mark leaves/sentinels
    // parent is at offset 0
    protected static final long OFFSET_LEFT = 4;
    protected static final byte RED = 1;
    // P + L + R + C + REF + LAST_REF
    private static final long BLOCK_SIZE = 4 + 4 + 4 + 4 + 4 + 4; // 24, must be divisible by 8
    private static final long MAX_KEY_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
    private static final long OFFSET_COLOUR = 12;
    // offset to last reference in value chain (kept to avoid having to traverse whole chain on each addition)
    private static final long OFFSET_LAST_REF = 20;
    private static final long OFFSET_REF = 16;
    private static final long OFFSET_RIGHT = 8;
    private final long initialKeyHeapSize;
    private final long maxKeyHeapSize;
    protected int root = -1;
    private long keyHeapLimit;
    private long keyHeapPos;
    private long keyHeapSize;
    private long keyHeapStart;

    public AbstractRedBlackTree(long keyPageSize, int keyMaxPages) {
        assert keyPageSize >= BLOCK_SIZE;
        keyHeapSize = initialKeyHeapSize = keyPageSize;
        keyHeapStart = keyHeapPos = Unsafe.malloc(keyHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
        keyHeapLimit = keyHeapStart + keyHeapSize;
        maxKeyHeapSize = Math.min(keyPageSize * keyMaxPages, MAX_KEY_HEAP_SIZE_LIMIT);
    }

    @Override
    public void clear() {
        root = -1;
        keyHeapPos = keyHeapStart;
    }

    @Override
    public void close() {
        root = -1;
        if (keyHeapStart != 0) {
            keyHeapStart = Unsafe.free(keyHeapStart, keyHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            keyHeapLimit = keyHeapPos = 0;
            keyHeapSize = 0;
        }
    }

    @Override
    public void reopen() {
        if (keyHeapStart == 0) {
            keyHeapSize = initialKeyHeapSize;
            keyHeapStart = keyHeapPos = Unsafe.malloc(keyHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            keyHeapLimit = keyHeapStart + keyHeapSize;
        }
    }

    public long size() {
        return (keyHeapPos - keyHeapStart) / BLOCK_SIZE;
    }

    private static int compressKeyOffset(long rawOffset) {
        return (int) (rawOffset >> 3);
    }

    private static long uncompressKeyOffset(int offset) {
        return ((long) offset) << 3;
    }

    private void checkKeyCapacity() {
        if (keyHeapPos + BLOCK_SIZE > keyHeapLimit) {
            final long newHeapSize = keyHeapSize << 1;
            if (newHeapSize > maxKeyHeapSize) {
                throw LimitOverflowException.instance().put("limit of ").put(maxKeyHeapSize).put(" memory exceeded in RedBlackTree");
            }
            long newHeapPos = Unsafe.realloc(keyHeapStart, keyHeapSize, newHeapSize, MemoryTag.NATIVE_TREE_CHAIN);

            keyHeapSize = newHeapSize;
            long delta = newHeapPos - keyHeapStart;
            keyHeapPos += delta;

            this.keyHeapStart = newHeapPos;
            this.keyHeapLimit = newHeapPos + newHeapSize;
        }
    }

    private void rotateLeft(int p) {
        if (p != -1) {
            final int r = rightOf(p);
            final int lr = leftOf(r);
            setRight(p, lr);
            if (lr != -1) {
                setParent(lr, p);
            }
            final int pp = parentOf(p);
            setParent(r, pp);
            if (pp == -1) {
                root = r;
            } else if (leftOf(pp) == p) {
                setLeft(pp, r);
            } else {
                setRight(pp, r);
            }
            setLeft(r, p);
            setParent(p, r);
        }
    }

    private void rotateRight(int p) {
        if (p != -1) {
            final int l = leftOf(p);
            final int rl = rightOf(l);
            setLeft(p, rl);
            if (rl != -1) {
                setParent(rl, p);
            }
            final int pp = parentOf(p);
            setParent(l, pp);
            if (pp == -1) {
                root = l;
            } else if (rightOf(pp) == p) {
                setRight(pp, l);
            } else {
                setLeft(pp, l);
            }
            setRight(l, p);
            setParent(p, l);
        }
    }

    protected int allocateBlock() {
        checkKeyCapacity();
        final int offset = compressKeyOffset(keyHeapPos - keyHeapStart);
        setLeft(offset, -1);
        setRight(offset, -1);
        setColor(offset, BLACK);
        keyHeapPos += BLOCK_SIZE;
        return offset;
    }

    protected byte colorOf(int blockOffset) {
        return blockOffset == -1 ? BLACK : Unsafe.getUnsafe().getByte(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_COLOUR);
    }

    protected int findMaxNode() {
        int p = root;
        int parent;
        do {
            parent = p;
            p = rightOf(p);
        } while (p > -1);
        return parent;
    }

    protected int findMinNode() {
        int p = root;
        int parent;
        do {
            parent = p;
            p = leftOf(p);
        } while (p > -1);
        return parent;
    }

    void fixDelete(int node, int parent) {
        if (root == EMPTY) {
            return;
        }

        boolean isLeftChild = parent != EMPTY && leftOf(parent) == node;

        while (node != root && colorOf(node) == BLACK) {
            if (isLeftChild) { // node is left child of parent
                int sibling = rightOf(parent);
                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parent, RED);
                    rotateLeft(parent);
                    sibling = rightOf(parent);
                }
                if (colorOf(leftOf(sibling)) == BLACK && colorOf(rightOf(sibling)) == BLACK) {
                    setColor(sibling, RED);
                    node = parent;
                    parent = parentOf(parent);
                    isLeftChild = parent != EMPTY && leftOf(parent) == node;
                } else {
                    if (colorOf(rightOf(sibling)) == BLACK) {
                        setColor(leftOf(sibling), BLACK);
                        setColor(sibling, RED);
                        rotateRight(sibling);
                        sibling = rightOf(parent);
                    }

                    setColor(sibling, colorOf(parent));
                    setColor(parent, BLACK);
                    if (rightOf(sibling) != EMPTY) {
                        setColor(rightOf(sibling), BLACK);
                    }
                    rotateLeft(parent);
                    break;
                }
            } else { // node is right child of parent, left/right expressions are reversed
                int sibling = leftOf(parent);
                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parent, RED);
                    rotateRight(parent);
                    sibling = leftOf(parent);
                }
                if (colorOf(leftOf(sibling)) == BLACK && colorOf(rightOf(sibling)) == BLACK) {
                    setColor(sibling, RED);
                    node = parent;
                    parent = parentOf(parent);
                    isLeftChild = parent != EMPTY && leftOf(parent) == node;
                } else {
                    if (colorOf(leftOf(sibling)) == BLACK) {
                        setColor(rightOf(sibling), BLACK);
                        setColor(sibling, RED);
                        rotateLeft(sibling);
                        sibling = leftOf(parent);
                    }

                    setColor(sibling, colorOf(parent));
                    setColor(parent, BLACK);
                    if (leftOf(sibling) != EMPTY) {
                        setColor(leftOf(sibling), BLACK);
                    }
                    rotateRight(parent);
                    break;
                }
            }
        }

        if (node != EMPTY) {
            setColor(node, BLACK);
        }
    }

    protected void fixInsert(int x) {
        setColor(x, RED);

        int px;
        while (x != -1 && x != root && colorOf(px = parentOf(x)) == RED) {
            int p20x = parent2Of(x);
            if (px == leftOf(p20x)) {
                int y = rightOf(p20x);
                if (colorOf(y) == RED) {
                    setColor(px, BLACK);
                    setColor(y, BLACK);
                    setColor(p20x, RED);
                    x = p20x;
                } else {
                    if (x == rightOf(px)) {
                        x = px;
                        rotateLeft(x);
                        px = parentOf(x);
                        p20x = parent2Of(x);
                    }
                    setColor(px, BLACK);
                    setColor(p20x, RED);
                    rotateRight(p20x);
                }
            } else {
                int y = leftOf(p20x);
                if (colorOf(y) == RED) {
                    setColor(px, BLACK);
                    setColor(y, BLACK);
                    setColor(p20x, RED);
                    x = p20x;
                } else {
                    if (x == leftOf(px)) {
                        x = parentOf(x);
                        rotateRight(x);
                        px = parentOf(x);
                        p20x = parent2Of(x);
                    }
                    setColor(px, BLACK);
                    setColor(p20x, RED);
                    rotateLeft(p20x);
                }
            }
        }
        setColor(root, BLACK);
    }

    protected int lastRefOf(int blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_LAST_REF);
    }

    protected int leftOf(int blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_LEFT);
    }

    protected int parent2Of(int blockOffset) {
        return parentOf(parentOf(blockOffset));
    }

    protected int parentOf(int blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getInt(keyHeapStart + uncompressKeyOffset(blockOffset));
    }

    protected void putParent(int value) {
        root = allocateBlock();
        setRef(root, value);
        setParent(root, -1);
    }

    protected int refOf(int blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_REF);
    }

    // based on Thomas Cormen's Introduction to Algorithm's
    protected int remove(int node) {
        int nodeToRemove;
        if (leftOf(node) == EMPTY || rightOf(node) == EMPTY) {
            nodeToRemove = node;
        } else {
            nodeToRemove = successor(node);
        }

        int current = leftOf(nodeToRemove) != EMPTY ? leftOf(nodeToRemove) : rightOf(nodeToRemove);
        int parent = parentOf(nodeToRemove);
        if (current != EMPTY) {
            setParent(current, parent);
        }

        if (parent == EMPTY) {
            root = current;
        } else {
            if (leftOf(parent) == nodeToRemove) {
                setLeft(parent, current);
            } else {
                setRight(parent, current);
            }
        }

        if (nodeToRemove != node) {
            int tmp = refOf(nodeToRemove);
            setRef(nodeToRemove, refOf(node));
            setRef(node, tmp);
        }

        if (colorOf(nodeToRemove) == BLACK) {
            fixDelete(current, parent);
        }

        return nodeToRemove;
    }

    // methods below check for -1 to simulate sentinel value and thus simplify insert/remove methods
    protected int rightOf(int blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_RIGHT);
    }

    protected void setColor(int blockOffset, byte colour) {
        Unsafe.getUnsafe().putByte(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_COLOUR, colour);
    }

    protected void setLastRef(int blockOffset, int recRef) {
        Unsafe.getUnsafe().putInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_LAST_REF, recRef);
    }

    protected void setLeft(int blockOffset, int left) {
        Unsafe.getUnsafe().putInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_LEFT, left);
    }

    protected void setParent(int blockOffset, int parent) {
        Unsafe.getUnsafe().putInt(keyHeapStart + uncompressKeyOffset(blockOffset), parent);
    }

    protected void setRef(int blockOffset, int recRef) {
        Unsafe.getUnsafe().putInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_REF, recRef);
    }

    protected void setRight(int blockOffset, int right) {
        Unsafe.getUnsafe().putInt(keyHeapStart + uncompressKeyOffset(blockOffset) + OFFSET_RIGHT, right);
    }

    protected int successor(int current) {
        int p = rightOf(current);
        if (p != -1) {
            int l;
            while ((l = leftOf(p)) != -1) {
                p = l;
            }
        } else {
            p = parentOf(current);
            int ch = current;
            while (p != -1 && ch == rightOf(p)) {
                ch = p;
                p = parentOf(p);
            }
        }
        return p;
    }
}
