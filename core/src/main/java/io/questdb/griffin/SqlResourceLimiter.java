package io.questdb.griffin;

public interface SqlResourceLimiter {
    public static final SqlResourceLimiter NOP_LIMITER = new SqlResourceLimiter() {
        @Override
        public void checkLimits(long nRows) {
        }
    };

    void checkLimits(long nRows);
}
