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

package io.questdb.griffin.engine;

import io.questdb.cairo.Reopenable;
import io.questdb.std.MemoryPages;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

public abstract class AbstractRedBlackTree implements Mutable, Reopenable {
    // parent is at offset 0
    protected static final int O_LEFT = 8;
    // P(8) + L + R + C(1) + REF
    private static final int BLOCK_SIZE = 8 + 8 + 8 + 1 + 8; // 33(it would be good to align to power of two, but entry would use way too much memory)
    private static final int O_RIGHT = 16;
    private static final int O_COLOUR = 24;
    private static final int O_REF = 25;

    protected static final byte RED = 1;
    protected static final byte BLACK = 0;

    protected static final byte EMPTY = -1;//empty reference; used to mark leaves/sentinels
    protected final MemoryPages mem;
    protected long root = -1;

    public AbstractRedBlackTree(long keyPageSize, int keyMaxPages) {
        assert keyPageSize >= getBlockSize();
        this.mem = new MemoryPages(keyPageSize, keyMaxPages);
    }

    @Override
    public void clear() {
        root = -1;
        this.mem.clear();
    }

    @Override
    public void close() {
        root = -1;
        Misc.free(mem);
    }

    @Override
    public void reopen() {
        mem.reopen();
    }

    public long size() {
        return mem.countNumberOf(getBlockSize());
    }

    protected static void setLeft(long blockAddress, long left) {
        Unsafe.getUnsafe().putLong(blockAddress + O_LEFT, left);
    }

    //methods below check for -1 to simulate sentinel value and thus simplify insert/remove methods
    protected static long rightOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_RIGHT);
    }

    protected static long leftOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_LEFT);
    }

    protected static void setParent(long blockAddress, long parent) {
        Unsafe.getUnsafe().putLong(blockAddress, parent);
    }

    protected static long refOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_REF);
    }

    protected static void setRef(long blockAddress, long recRef) {
        Unsafe.getUnsafe().putLong(blockAddress + O_REF, recRef);
    }

    protected static void setRight(long blockAddress, long right) {
        Unsafe.getUnsafe().putLong(blockAddress + O_RIGHT, right);
    }

    protected static long parentOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress);
    }

    protected static long parent2Of(long blockAddress) {
        return parentOf(parentOf(blockAddress));
    }

    protected static void setColor(long blockAddress, byte colour) {
        Unsafe.getUnsafe().putByte(blockAddress + O_COLOUR, colour);
    }

    protected static byte colorOf(long blockAddress) {
        return blockAddress == -1 ? BLACK : Unsafe.getUnsafe().getByte(blockAddress + O_COLOUR);
    }

    protected static long successor(long current) {
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

    protected long allocateBlock() {
        long p = mem.allocate(getBlockSize());
        setLeft(p, -1);
        setRight(p, -1);
        setColor(p, BLACK);
        return p;
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

    protected long findMinNode() {
        long p = root;
        long parent;
        do {
            parent = p;
            p = leftOf(p);
        } while (p > -1);

        return parent;
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
    
    protected int getBlockSize() {
        return BLOCK_SIZE;
    }

    protected void putParent(long value) {
        root = allocateBlock();
        setRef(root, value);
        setParent(root, -1);
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

    //based on Thomas Cormen's Introduction to Algorithm's
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

    void fixDelete(long node, long parent) {
        if (root == EMPTY) {
            return;
        }

        boolean isLeftChild = parent != EMPTY && leftOf(parent) == node;

        while (node != root && colorOf(node) == BLACK) {
            if (isLeftChild) {//node is left child of parent
                long sibling = rightOf(parent);
                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parent, RED);
                    rotateLeft(parent);
                    sibling = rightOf(parent);
                }
                if (colorOf(leftOf(sibling)) == BLACK &&
                        colorOf(rightOf(sibling)) == BLACK) {
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
            } else {//node is right child of parent, left/right expressions are reversed
                long sibling = leftOf(parent);
                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parent, RED);
                    rotateRight(parent);
                    sibling = leftOf(parent);
                }
                if (colorOf(leftOf(sibling)) == BLACK &&
                        colorOf(rightOf(sibling)) == BLACK) {
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

        if (node != EMPTY)
            setColor(node, BLACK);
    }
}
