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

package io.questdb.griffin.engine;

import io.questdb.cairo.Reopenable;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * A native memory heap-based red-black tree. Used in ORDER BY factories.
 */
public abstract class AbstractRedBlackTree implements Mutable, Reopenable {
    protected static final byte BLACK = 0;
    protected static final long EMPTY = -1; // empty reference; used to mark leaves/sentinels
    // parent is at offset 0
    protected static final long OFFSET_LEFT = 8;
    protected static final byte RED = 1;
    // P(8) + L + R + C(1) + REF + LAST_REF
    private static final long BLOCK_SIZE = 8 + 8 + 8 + 1 + 8 + 8; // 41 (it would be good to align to power of two, but entry would use way too much memory)
    private static final long OFFSET_COLOUR = 24;
    // offset to last reference in value chain (kept to avoid having to traverse whole chain on each addition)
    private static final long OFFSET_LAST_REF = 33;
    private static final long OFFSET_REF = 25;
    private static final long OFFSET_RIGHT = 16;
    private final long initialKeyHeapSize;
    private final long maxKeyHeapSize;
    protected long root = -1;
    private long keyHeapLimit;
    private long keyHeapPos;
    private long keyHeapSize;
    private long keyHeapStart;

    public AbstractRedBlackTree(long keyPageSize, int keyMaxPages) {
        assert keyPageSize >= BLOCK_SIZE;
        keyHeapSize = initialKeyHeapSize = keyPageSize;
        keyHeapStart = keyHeapPos = Unsafe.malloc(keyHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
        keyHeapLimit = keyHeapStart + keyHeapSize;
        maxKeyHeapSize = keyPageSize * keyMaxPages;
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

    private void checkCapacity() {
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

    private void rotateLeft(long p) {
        if (p != -1) {
            final long r = rightOf(p);
            final long lr = leftOf(r);
            setRight(p, lr);
            if (lr != -1) {
                setParent(lr, p);
            }
            final long pp = parentOf(p);
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

    private void rotateRight(long p) {
        if (p != -1) {
            final long l = leftOf(p);
            final long rl = rightOf(l);
            setLeft(p, rl);
            if (rl != -1) {
                setParent(rl, p);
            }
            final long pp = parentOf(p);
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

    protected long allocateBlock() {
        checkCapacity();
        long offset = keyHeapPos - keyHeapStart;
        setLeft(offset, -1);
        setRight(offset, -1);
        setColor(offset, BLACK);
        keyHeapPos += BLOCK_SIZE;
        return offset;
    }

    protected byte colorOf(long blockOffset) {
        return blockOffset == -1 ? BLACK : Unsafe.getUnsafe().getByte(keyHeapStart + blockOffset + OFFSET_COLOUR);
    }

    protected long findMaxNode() {
        long p = root;
        long parent;
        do {
            parent = p;
            p = rightOf(p);
        } while (p > -1);
        return parent;
    }

    protected long findMinNode() {
        long p = root;
        long parent;
        do {
            parent = p;
            p = leftOf(p);
        } while (p > -1);
        return parent;
    }

    void fixDelete(long node, long parent) {
        if (root == EMPTY) {
            return;
        }

        boolean isLeftChild = parent != EMPTY && leftOf(parent) == node;

        while (node != root && colorOf(node) == BLACK) {
            if (isLeftChild) { // node is left child of parent
                long sibling = rightOf(parent);
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
                long sibling = leftOf(parent);
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

    protected void fixInsert(long x) {
        setColor(x, RED);

        long px;
        while (x != -1 && x != root && colorOf(px = parentOf(x)) == RED) {
            long p20x = parent2Of(x);
            if (px == leftOf(p20x)) {
                long y = rightOf(p20x);
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
                long y = leftOf(p20x);
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

    protected long lastRefOf(long blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getLong(keyHeapStart + blockOffset + OFFSET_LAST_REF);
    }

    protected long leftOf(long blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getLong(keyHeapStart + blockOffset + OFFSET_LEFT);
    }

    protected long parent2Of(long blockOffset) {
        return parentOf(parentOf(blockOffset));
    }

    protected long parentOf(long blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getLong(keyHeapStart + blockOffset);
    }

    protected void putParent(long value) {
        root = allocateBlock();
        setRef(root, value);
        setParent(root, -1);
    }

    protected long refOf(long blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getLong(keyHeapStart + blockOffset + OFFSET_REF);
    }

    // based on Thomas Cormen's Introduction to Algorithm's
    protected long remove(long node) {
        long nodeToRemove;
        if (leftOf(node) == EMPTY || rightOf(node) == EMPTY) {
            nodeToRemove = node;
        } else {
            nodeToRemove = successor(node);
        }

        long current = leftOf(nodeToRemove) != EMPTY ? leftOf(nodeToRemove) : rightOf(nodeToRemove);
        long parent = parentOf(nodeToRemove);
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
            long tmp = refOf(nodeToRemove);
            setRef(nodeToRemove, refOf(node));
            setRef(node, tmp);
        }

        if (colorOf(nodeToRemove) == BLACK) {
            fixDelete(current, parent);
        }

        return nodeToRemove;
    }

    // methods below check for -1 to simulate sentinel value and thus simplify insert/remove methods
    protected long rightOf(long blockOffset) {
        return blockOffset == -1 ? -1 : Unsafe.getUnsafe().getLong(keyHeapStart + blockOffset + OFFSET_RIGHT);
    }

    protected void setColor(long blockOffset, byte colour) {
        Unsafe.getUnsafe().putByte(keyHeapStart + blockOffset + OFFSET_COLOUR, colour);
    }

    protected void setLastRef(long blockOffset, long recRef) {
        Unsafe.getUnsafe().putLong(keyHeapStart + blockOffset + OFFSET_LAST_REF, recRef);
    }

    protected void setLeft(long blockOffset, long left) {
        Unsafe.getUnsafe().putLong(keyHeapStart + blockOffset + OFFSET_LEFT, left);
    }

    protected void setParent(long blockOffset, long parent) {
        Unsafe.getUnsafe().putLong(keyHeapStart + blockOffset, parent);
    }

    protected void setRef(long blockOffset, long recRef) {
        Unsafe.getUnsafe().putLong(keyHeapStart + blockOffset + OFFSET_REF, recRef);
    }

    protected void setRight(long blockOffset, long right) {
        Unsafe.getUnsafe().putLong(keyHeapStart + blockOffset + OFFSET_RIGHT, right);
    }

    protected long successor(long current) {
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
}
