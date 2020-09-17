package io.questdb.cutlass.http;

public interface HttpClientStateListener {
    void onStartingRequest();

    void onClosed();

    void onReceived(int nBytes);
}
