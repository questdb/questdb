/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl.sort;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.join.hash.RecordDequeue;
import com.nfsdb.ql.ops.AbstractRecordSource;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.Mutable;
import com.nfsdb.store.MemoryPages;

import java.io.Closeable;

public class RBTreeSortedRecordSource extends AbstractRecordSource implements Mutable, RecordSource, Closeable {
    // P(8) + L + R + C(1) + REC
    private static final int BLOCK_SIZE = 8 + 8 + 8 + 1 + 8;
    private static final int O_LEFT = 8;
    private static final int O_RIGHT = 16;
    private static final int O_COLOUR = 24;
    private static final int O_REF = 25;
    private static final byte RED = 1;
    private static final byte BLACK = 0;
    private final RecordDequeue records;
    private final MemoryPages mem;
    private final RecordComparator comparator;
    private final RecordSource recordSource;
    private final AscendingCursor ascendingCursor = new AscendingCursor();
    private long root = 0;

    public RBTreeSortedRecordSource(RecordSource recordSource, RecordComparator comparator) {
        this.recordSource = recordSource;
        this.comparator = comparator;
        // todo: extract config
        this.mem = new MemoryPages(1024 * 1024);
        this.records = new RecordDequeue(recordSource.getMetadata(), 4 * 1024 * 1024);
    }

    @Override
    public void clear() {
        root = 0;
        this.mem.clear();
        records.clear();
    }

    @Override
    public void close() {
        records.close();
        mem.close();
    }

    @Override
    public RecordMetadata getMetadata() {
        return records.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        RecordCursor cursor = recordSource.prepareCursor(factory);
        records.setStorageFacade(cursor.getStorageFacade());
        buildMap(cursor);
        ascendingCursor.setup();
        return ascendingCursor;
    }

    @Override
    public void reset() {
        records.clear();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    public void setStorageFacade(StorageFacade facade) {
        this.records.setStorageFacade(facade);
    }

    private static void setLeft(long blockAddress, long left) {
        Unsafe.getUnsafe().putLong(blockAddress + O_LEFT, left);
    }

    private static long rightOf(long blockAddress) {
        return blockAddress == 0 ? 0 : Unsafe.getUnsafe().getLong(blockAddress + O_RIGHT);
    }

    private static long leftOf(long blockAddress) {
        return blockAddress == 0 ? 0 : Unsafe.getUnsafe().getLong(blockAddress + O_LEFT);
    }

    private static void setParent(long blockAddress, long parent) {
        Unsafe.getUnsafe().putLong(blockAddress, parent);
    }

    private static long refOf(long blockAddress) {
        return blockAddress == 0 ? 0 : Unsafe.getUnsafe().getLong(blockAddress + O_REF);
    }

    private static void setRef(long blockAddress, long recRef) {
        Unsafe.getUnsafe().putLong(blockAddress + O_REF, recRef);
    }

    private static void setRight(long blockAddress, long right) {
        Unsafe.getUnsafe().putLong(blockAddress + O_RIGHT, right);
    }

    private static long parentOf(long blockAddress) {
        return blockAddress == 0 ? 0 : Unsafe.getUnsafe().getLong(blockAddress);
    }

    private static long parent2Of(long blockAddress) {
        return parentOf(parentOf(blockAddress));
    }

    private static void setColor(long blockAddress, byte colour) {
        if (blockAddress == 0) {
            return;
        }
        Unsafe.getUnsafe().putByte(blockAddress + O_COLOUR, colour);
    }

    private static byte colorOf(long blockAddress) {
        return blockAddress == 0 ? BLACK : Unsafe.getUnsafe().getByte(blockAddress + O_COLOUR);
    }

    private long allocateBlock() {
        long p = mem.addressOf(mem.allocate(BLOCK_SIZE));
        setLeft(p, 0);
        setRight(p, 0);
        setColor(p, BLACK);
        return p;
    }

    private void buildMap(RecordCursor cursor) {
        while (cursor.hasNext()) {
            put(cursor.next());
        }
    }

    private void fix(long x) {
        setColor(x, RED);

        while (x != 0 && x != root && colorOf(parentOf(x)) == RED) {
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

    private void put(Record record) {
        if (root == 0) {
            root = allocateBlock();
            setRef(root, records.append(record, -1));
            setParent(root, 0);
            setLeft(root, 0);
            setRight(root, 0);
            return;
        }

        comparator.setLeft(record);

        long p = root;
        long parent;
        int cmp;
        do {
            parent = p;
            cmp = comparator.compare(records.recordAt(refOf(p)));
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                setRef(p, records.append(record, refOf(p)));
                return;
            }
        } while (p > 0);

        p = allocateBlock();
        setParent(p, parent);
        setRef(p, records.append(record, -1));

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    private void rotateLeft(long p) {
        if (p != 0) {
            long r = rightOf(p);
            setRight(p, leftOf(r));
            if (leftOf(r) != 0) {
                setParent(leftOf(r), p);
            }
            setParent(r, parentOf(p));
            if (parentOf(p) == 0) {
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
        if (p != 0) {
            long l = leftOf(p);
            setLeft(p, rightOf(l));
            if (rightOf(l) != 0) {
                setParent(rightOf(l), p);
            }
            setParent(l, parentOf(p));
            if (parentOf(p) == 0) {
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

    private class AscendingCursor extends AbstractImmutableIterator<Record> implements RecordCursor {

        private long current;

        @Override
        public Record getByRowId(long rowId) {
            return null;
        }

        @Override
        public RecordMetadata getMetadata() {
            return records.getMetadata();
        }

        @Override
        public StorageFacade getStorageFacade() {
            return records.getStorageFacade();
        }

        @Override
        public boolean hasNext() {
            return current != 0;
        }

        @Override
        public Record next() {
            long t = current;
            long p = rightOf(t);
            if (p != 0) {
                long l;
                while ((l = leftOf(p)) != 0) {
                    p = l;
                }
            } else {
                p = parentOf(t);
                long ch = t;
                while (p != 0 && ch == rightOf(p)) {
                    ch = p;
                    p = parentOf(p);
                }
            }
            current = p;
            return records.recordAt(refOf(t));
        }

        private void setup() {
            long p = root;
            if (p != 0) {
                while (leftOf(p) != 0) {
                    p = leftOf(p);
                }
            }
            current = p;
        }
    }
}
