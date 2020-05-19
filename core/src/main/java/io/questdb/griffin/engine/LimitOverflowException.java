package io.questdb.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.std.ThreadLocal;

public class LimitOverflowException extends CairoException {
    private static final ThreadLocal<LimitOverflowException> tlException = new ThreadLocal<>(LimitOverflowException::new);

    public static LimitOverflowException instance(long limit) {
        LimitOverflowException ex = tlException.get();
        ex.message.clear();
        ex.put("limit of ").put(limit).put(" exceeded");
        return ex;
    }

}
