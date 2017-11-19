package com.questdb.std;

public class NetFacadeImpl implements NetFacade {
    public static final NetFacadeImpl INSTANCE = new NetFacadeImpl();

    @Override
    public boolean bindTcp(long fd, CharSequence IPv4Address, int port) {
        return Net.bindTcp(fd, IPv4Address, port);
    }

    @Override
    public boolean bindUdp(long fd, CharSequence IPv4Address, int port) {
        return Net.bindUdp(fd, IPv4Address, port);
    }

    @Override
    public void close(long fd) {
        Net.close(fd);
    }

    @Override
    public void freeMsgHeaders(long msgVec) {
        Net.freeMsgHeaders(msgVec);
    }

    @Override
    public long getMMsgBuf(long msg) {
        return Net.getMMsgBuf(msg);
    }

    @Override
    public long getMMsgBufLen(long msg) {
        return Net.getMMsgBufLen(msg);
    }

    @Override
    public boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return Net.join(fd, bindIPv4Address, groupIPv4Address);
    }

    @Override
    public long msgHeaders(int msgBufferSize, int msgCount) {
        return Net.msgHeaders(msgBufferSize, msgCount);
    }

    @Override
    public int recvmmsg(long fd, long msgVec, int msgCount) {
        return Net.recvmmsg(fd, msgVec, msgCount);
    }

    @Override
    public int recv(long fd, long buf, int bufLen) {
        return Net.recv(fd, buf, bufLen);
    }

    @Override
    public int setRcvBuf(long fd, int size) {
        return Net.setRcvBuf(fd, size);
    }

    @Override
    public long socketUdp() {
        return Net.socketUdp();
    }
}
