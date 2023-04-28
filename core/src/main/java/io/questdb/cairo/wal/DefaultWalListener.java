package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;

public class DefaultWalListener implements WalListener {
    @SuppressWarnings("EmptyMethod")
    @Override
    public void dataTxnCommitted(TableToken tableToken, long txn) {

    }

    @SuppressWarnings("EmptyMethod")
    @Override
    public void nonDataTxnCommitted(TableToken tableToken, long txn) {

    }

    @SuppressWarnings("EmptyMethod")
    @Override
    public void segmentClosed(TableToken tabletoken, int walId, int segmentId) {

    }

    @SuppressWarnings("EmptyMethod")
    @Override
    public void tableDropped(TableToken tableToken) {

    }
}
