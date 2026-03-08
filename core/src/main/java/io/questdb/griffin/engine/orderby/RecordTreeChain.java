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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.MemoryPages;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class RecordTreeChain implements Closeable, Mutable, Reopenable {
    private static final byte BLACK = 0;
    // P(8) + L + R + C(1) + REF + TOP
    private static final int BLOCK_SIZE = 8 + 8 + 8 + 1 + 8 + 8;
    private static final int O_COLOUR = 24;
    private static final int O_LEFT = 8;
    private static final int O_REF = 25;
    private static final int O_RIGHT = 16;
    private static final int O_TOP = 33;
    private static final byte RED = 1;
    private final RecordComparator comparator;
    private final TreeCursor cursor = new TreeCursor();
    private final MemoryPages mem;
    private final RecordChain recordChain;
    private final Record recordChainRecord;
    private long root = -1;

    public RecordTreeChain(
            @NotNull ColumnTypes columnTypes,
            @NotNull RecordSink recordSink,
            @NotNull RecordComparator comparator,
            long keyPageSize,
            int keyMaxPages,
            long valuePageSize,
            int valueMaxPages
    ) {
        try {
            this.comparator = comparator;
            this.mem = new MemoryPages(keyPageSize, keyMaxPages);
            this.recordChain = new RecordChain(columnTypes, recordSink, valuePageSize, valueMaxPages);
            this.recordChainRecord = this.recordChain.getRecordB();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        root = -1;
        mem.clear();
        recordChain.clear();
    }

    @Override
    public void close() {
        root = -1;
        Misc.free(recordChain);
        Misc.free(mem);
        Misc.free(cursor);
    }

    public TreeCursor getCursor(RecordCursor base) {
        cursor.of(base);
        return cursor;
    }

    public void put(Record record) {
        if (root == -1) {
            putParent(record);
            return;
        }

        comparator.setLeft(record);

        long p = root;
        long parent;
        int cmp;
        do {
            parent = p;
            long r = refOf(p);
            recordChain.recordAt(recordChainRecord, r);
            cmp = comparator.compare(recordChainRecord);
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                setRef(p, recordChain.put(record, r));
                return;
            }
        } while (p > -1);

        p = allocateBlock();
        setParent(p, parent);
        long r = recordChain.put(record, -1L);
        setTop(p, r);
        setRef(p, r);

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    @Override
    public void reopen() {
        mem.reopen();
    }

    public long size() {
        return recordChain.size();
    }

    private static byte colorOf(long blockAddress) {
        return blockAddress == -1 ? BLACK : Unsafe.getUnsafe().getByte(blockAddress + O_COLOUR);
    }

    private static long leftOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_LEFT);
    }

    private static long parent2Of(long blockAddress) {
        return parentOf(parentOf(blockAddress));
    }

    private static long parentOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress);
    }

    private static long refOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_REF);
    }

    private static long rightOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_RIGHT);
    }

    private static void setColor(long blockAddress, byte colour) {
        if (blockAddress == -1) {
            return;
        }
        Unsafe.getUnsafe().putByte(blockAddress + O_COLOUR, colour);
    }

    private static void setLeft(long blockAddress, long left) {
        Unsafe.getUnsafe().putLong(blockAddress + O_LEFT, left);
    }

    private static void setParent(long blockAddress, long parent) {
        Unsafe.getUnsafe().putLong(blockAddress, parent);
    }

    private static void setRef(long blockAddress, long recRef) {
        Unsafe.getUnsafe().putLong(blockAddress + O_REF, recRef);
    }

    private static void setRight(long blockAddress, long right) {
        Unsafe.getUnsafe().putLong(blockAddress + O_RIGHT, right);
    }

    private static void setTop(long blockAddress, long recRef) {
        Unsafe.getUnsafe().putLong(blockAddress + O_TOP, recRef);
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

    private static long topOf(long blockAddress) {
        return blockAddress == -1 ? -1 : Unsafe.getUnsafe().getLong(blockAddress + O_TOP);
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

    private void putParent(Record record) {
        root = allocateBlock();
        long r = recordChain.put(record, -1L);
        setTop(root, r);
        setRef(root, r);
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

    public class TreeCursor implements RecordCursor {
        private RecordCursor baseCursor;
        private long current;

        @Override
        public void close() {
            // base cursor's lifecycle is managed externally, so we don't close it here
            current = -1;
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (recordChain.hasNext()) {
                return true;
            }

            current = successor(current);
            if (current == -1) {
                return false;
            }

            recordChain.of(topOf(current));
            return recordChain.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            // no state to preserve
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void toTop() {
            long p = root;
            if (p != -1) {
                while (leftOf(p) != -1) {
                    p = leftOf(p);
                }
            }
            recordChain.of(topOf(current = p));
        }

        private void of(RecordCursor base) {
            this.baseCursor = base;
            recordChain.setSymbolTableResolver(base);
            toTop();
        }
    }
}
