package com.questdb.net.udp.receiver;

public interface ReceiverConfiguration {
    CharSequence getBindIPv4Address();

    CharSequence getGroupIPv4Address();

    int getMsgBufferSize();

    int getMsgCount();

    int getPort();

    int getReceiveBufferSize();
}
