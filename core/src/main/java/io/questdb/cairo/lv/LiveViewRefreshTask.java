package io.questdb.cairo.lv;

import io.questdb.cairo.TableToken;

public class LiveViewRefreshTask {
    public TableToken baseTableToken;
    public long seqTxn;

    public void clear() {
        baseTableToken = null;
        seqTxn = 0;
    }
}
