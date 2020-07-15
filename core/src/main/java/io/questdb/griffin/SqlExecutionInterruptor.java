package io.questdb.griffin;

public interface SqlExecutionInterruptor {
    public static final SqlExecutionInterruptor NOP_INTERRUPTOR = new SqlExecutionInterruptor() {
        @Override
        public void checkInterrupted() {
        }
    };

    void checkInterrupted();
}
