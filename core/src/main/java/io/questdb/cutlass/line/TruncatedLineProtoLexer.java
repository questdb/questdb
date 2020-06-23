package io.questdb.cutlass.line;

public class TruncatedLineProtoLexer extends LineProtoLexer {
    private boolean finishedLine;

    public TruncatedLineProtoLexer(int maxMeasurementSize) {
        super(maxMeasurementSize);
    }

    public long parseLine(long bytesPtr, long hi) {
        finishedLine = false;
        long atPtr = parsePartial(bytesPtr, hi);
        if (!finishedLine) {
            return -1;
        }
        return atPtr;
    }

    @Override
    protected boolean partialComplete() {
        return finishedLine;
    }

    @Override
    protected void doSkipLineComplete() {
        finishedLine = true;
    }

    @Override
    protected void onEol() throws LineProtoException {
        finishedLine = true;
        super.onEol();
    }

    @Override
    public void parse(long bytesPtr, long hi) {
        throw new UnsupportedOperationException();
    }

    public CharSequenceCache getCharSequenceCache() {
        return charSequenceCache;
    }
}
