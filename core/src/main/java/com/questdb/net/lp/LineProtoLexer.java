package com.questdb.net.lp;

import com.questdb.misc.Unsafe;
import com.questdb.std.Mutable;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.SplitByteSequence;

import java.io.Closeable;

public class LineProtoLexer implements Mutable, Closeable {
    public static final int EVT_MEASUREMENT = 1;
    public static final int EVT_TAG_VALUE = 2;
    public static final int EVT_FIELD_VALUE = 3;
    public static final int EVT_TAG_NAME = 4;
    public static final int EVT_FIELD_NAME = 5;
    public static final int EVT_TIMESTAMP = 6;
    public static final int EVT_END = 7;

    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final DirectByteCharSequence reserveDbcs = new DirectByteCharSequence();
    private final SplitByteSequence sbcs = new SplitByteSequence();

    private int state = EVT_MEASUREMENT;
    private boolean escape = false;
    private long rollPtr = 0;
    private int rollCapacity = 0;
    private int rollSize = 0;

    @Override
    public void clear() {
        rollSize = 0;
        escape = false;
        state = EVT_MEASUREMENT;
    }

    @Override
    public void close() {
        if (rollPtr > 0) {
            Unsafe.free(rollPtr, rollCapacity);
        }
    }

    public void parse(long lo, int len, LineProtoListener listener) {
        long p = lo;
        long hi = lo + len;
        long _lo = p;

        while (p < hi) {
            char c = (char) Unsafe.getUnsafe().getByte(p++);
            if (escape) {
                escape = false;
                continue;
            }

            switch (c) {
                case ',':
                    switch (state) {
                        case EVT_MEASUREMENT:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_MEASUREMENT);
                            state = EVT_TAG_NAME;
                            break;
                        case EVT_TAG_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_TAG_VALUE);
                            state = EVT_TAG_NAME;
                            break;
                        case EVT_FIELD_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_FIELD_VALUE);
                            state = EVT_FIELD_NAME;
                            break;
                        default:
                            throw new RuntimeException("unexpected ,");
                    }
                    break;
                case '=':
                    switch (state) {
                        case EVT_TAG_NAME:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_TAG_NAME);
                            state = EVT_TAG_VALUE;
                            break;
                        case EVT_FIELD_NAME:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_FIELD_NAME);
                            state = EVT_FIELD_VALUE;
                            break;
                        default:
                            throw new RuntimeException("unexpected =");
                    }
                    break;
                case '\\':
                    escape = true;
                    continue;
                case ' ':
                    switch (state) {
                        case EVT_MEASUREMENT:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_MEASUREMENT);
                            state = EVT_FIELD_NAME;
                            break;
                        case EVT_TAG_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_TAG_VALUE);
                            state = EVT_FIELD_NAME;
                            break;
                        case EVT_FIELD_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_FIELD_VALUE);
                            state = EVT_TIMESTAMP;
                            break;
                        default:
                            throw new RuntimeException("unexpected space");
                    }
                    break;
                case '\n':
                case '\r':
                    switch (state) {
                        case EVT_MEASUREMENT:
                            // empty line?
                            break;
                        case EVT_TAG_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_TAG_VALUE);
                            break;
                        case EVT_FIELD_VALUE:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_FIELD_VALUE);
                            break;
                        case EVT_TIMESTAMP:
                            listener.onEvent(makeByteSeq(_lo, p), EVT_TIMESTAMP);
                            break;
                        default:
                            throw new RuntimeException("unexpected EOL");
                    }

                    if (state != EVT_MEASUREMENT) {
                        listener.onEvent(null, EVT_END);
                        clear();
                    }
                    break;
                default:
                    // normal byte
                    continue;
            }
            _lo = p;
        }

        if (_lo < hi) {
            rollLine(_lo, hi);
        }
    }

    public void parseLast(LineProtoListener listener) {
        if (state != EVT_MEASUREMENT) {
            parseLast0(listener);
        }
    }

    private ByteSequence makeByteSeq(long _lo, long hi) {
        return rollSize > 0 ? makeByteSeq0(_lo, hi) : dbcs.of(_lo, hi - 1);
    }

    private ByteSequence makeByteSeq0(long _lo, long hi) {
        ByteSequence sequence;
        if (_lo == hi - 1) {
            sequence = dbcs.of(rollPtr, rollPtr + rollSize);
        } else {
            sequence = sbcs.of(dbcs.of(rollPtr, rollPtr + rollSize), reserveDbcs.of(_lo, hi - 1));
        }
        rollSize = 0;
        return sequence;
    }

    private void parseLast0(LineProtoListener listener) {
        if (state == EVT_TIMESTAMP) {
            if (rollSize > 0) {
                listener.onEvent(dbcs.of(rollPtr, rollPtr + rollSize), EVT_TIMESTAMP);
            }
        } else if (rollSize == 0) {
            throw new RuntimeException("unexpected EOL");
        }

        switch (state) {
            case EVT_TAG_VALUE:
                listener.onEvent(dbcs.of(rollPtr, rollPtr + rollSize), EVT_TAG_VALUE);
                break;
            case EVT_FIELD_VALUE:
                listener.onEvent(dbcs.of(rollPtr, rollPtr + rollSize), EVT_FIELD_VALUE);
                break;
            case EVT_TIMESTAMP:
                break;
            default:
                throw new RuntimeException("unexpected EOL");
        }
        listener.onEvent(null, EVT_END);
    }

    private void rollLine(long lo, long hi) {
        int len = (int) (hi - lo);
        int requiredCapacity = rollSize + len;

        if (requiredCapacity > rollCapacity) {
            long p = Unsafe.malloc(requiredCapacity);
            if (rollSize > 0) {
                Unsafe.getUnsafe().copyMemory(rollPtr, p, rollSize);
            }

            if (rollPtr > 0) {
                Unsafe.free(rollPtr, rollCapacity);
            }
            rollPtr = p;
            rollCapacity = requiredCapacity;
        }
        Unsafe.getUnsafe().copyMemory(lo, rollPtr + rollSize, len);
        rollSize = requiredCapacity;
    }
}
