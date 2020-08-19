package io.questdb.std;

public abstract class Long256FromCharSequenceDecoder {
    protected abstract void onDecoded(long l0, long l1, long l2, long l3);

    public void decode(final CharSequence hexString, final int startPos, final int endPos) throws NumericException {
        final int minPos = startPos - 16;
        int lim = endPos;
        int p = lim - 16;
        long l0 = parse64BitGroup(startPos, minPos, hexString, p, lim);
        lim = p;
        p = lim - 16;
        long l1 = parse64BitGroup(startPos, minPos, hexString, p, lim);
        lim = p;
        p -= 16;
        long l2 = parse64BitGroup(startPos, minPos, hexString, p, lim);
        lim = p;
        p -= 16;
        if (p > startPos) {
            // hex string too long
            throw NumericException.INSTANCE;
        }
        long l3 = parse64BitGroup(startPos, minPos, hexString, p, lim);

        onDecoded(l0, l1, l2, l3);
    }

    private long parse64BitGroup(int startPos, int minPos, CharSequence hexString, int p, int lim) throws NumericException {
        assert minPos == startPos - 16;
        if (p >= startPos) {
            return Numbers.parseHexLong(hexString, p, lim);
        }

        if (p > minPos) {
            return Numbers.parseHexLong(hexString, startPos, lim);
        }

        return 0;
    }
}
