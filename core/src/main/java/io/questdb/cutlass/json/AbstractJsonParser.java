package io.questdb.cutlass.json;

import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.DirectUtf8Sink;

public abstract class AbstractJsonParser implements JsonParser, Mutable, QuietCloseable {

    private final ObjectPool<FloatingCharSequence> csPool;
    private final JsonLexer lexer;
    private long buf;
    private long bufCapacity = 0;
    private int bufSize = 0;

    protected AbstractJsonParser(int initialCacheSize, int cacheSizeLimit, int initialStringPoolCapacity) {
        csPool = new ObjectPool<>(FloatingCharSequence::new, initialStringPoolCapacity);
        lexer = new JsonLexer(initialCacheSize, cacheSizeLimit);
    }

    @Override
    public void clear() {
        lexer.clear();
        csPool.clear();
    }

    @Override
    public void close() {
        clear();
        Misc.free(lexer);
        if (bufCapacity > 0) {
            Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            bufCapacity = 0;
        }
    }

    public void parse(DirectUtf8Sink utf8Sink) throws JsonException {
        parse(utf8Sink.lo(), utf8Sink.hi());
    }

    public void parse(long lo, long hi) throws JsonException {
        lexer.parse(lo, hi, this);
    }

    private static void strcpyw(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + ((long) i << 1), value.charAt(i));
        }
    }

    protected CharSequence copy(CharSequence tag) {
        final int l = tag.length() * 2;
        final long n = bufSize + l;
        if (n > bufCapacity) {
            long ptr = Unsafe.malloc(n * 2, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            Vect.memcpy(ptr, buf, bufSize);
            if (bufCapacity > 0) {
                Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            }
            buf = ptr;
            bufCapacity = n * 2;
        }

        strcpyw(tag, l / 2, buf + bufSize);
        CharSequence cs = csPool.next().of(bufSize, l / 2);
        bufSize += l;
        return cs;
    }

    private class FloatingCharSequence extends AbstractCharSequence implements Mutable {

        private int len;
        private int offset;

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(buf + offset + index * 2L);
        }

        @Override
        public void clear() {
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        protected final CharSequence _subSequence(int start, int end) {
            FloatingCharSequence that = csPool.next();
            that.of(this.offset + 2 * start, end - start);
            return that;
        }

        CharSequence of(int lo, int len) {
            this.offset = lo;
            this.len = len;
            return this;
        }
    }
}
