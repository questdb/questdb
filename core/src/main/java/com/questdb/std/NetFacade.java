package com.questdb.std;

public interface NetFacade {
    boolean bind(long fd, CharSequence IPv4Address, int port);

    void close(long fd);

    void freeMsgHeaders(long msgVec);

    long getMMsgBuf(long msg);

    long getMMsgBufLen(long msg);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    long msgHeaders(int msgBufferSize, int msgCount);

    int recv(long fd, long buf, int bufLen);

    @SuppressWarnings("SpellCheckingInspection")
    int recvmmsg(long fd, long msgVec, int msgCount);

    int setRcvBuf(long fd, int size);

    long socketUdp();
}
