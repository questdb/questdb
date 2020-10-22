package io.questdb.cutlass.http;

public interface HttpClientStateListener {
    void onClosed();

    void onReceived(int nBytes);
}
