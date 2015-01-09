/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.config;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.exceptions.JournalRuntimeException;

import java.lang.reflect.Field;
import java.net.*;
import java.util.List;

public class NetworkConfig {
    public static final int DEFAULT_DATA_PORT = 7075;
    private static final int DEFAULT_MULTICAST_PORT = 4446;
    private static final String DEFAULT_MULTICAST_ADDRESS_IPV4 = "230.100.12.4";
    private static final String DEFAULT_MULTICAST_ADDRESS_IPV6_1 = "FF02:231::4500";
    private static final int DEFAULT_SO_RCVBUF = 32768;
    private static final NetworkInterface defaultInterface;

    static {
        try {
            Field f = NetworkInterface.class.getDeclaredField("defaultInterface");
            f.setAccessible(true);
            defaultInterface = (NetworkInterface) f.get(null);
        } catch (Exception e) {
            throw new JournalRuntimeException("Cannot lookup default network interface", e);
        }
    }

    private final SslConfig sslConfig = new SslConfig();
    protected InetAddress hostAddress;
    private int port = DEFAULT_DATA_PORT;
    private int multicastPort = DEFAULT_MULTICAST_PORT;
    private InetAddress multicastAddress;
    private int soRcvBuf = DEFAULT_SO_RCVBUF;
    private String ifName = null;
    private String hostname = "0.0.0.0";
    private boolean enableMulticast = true;
    private NetworkInterface networkInterface;

    public static boolean isInet6(InetAddress address) {
        return address instanceof Inet6Address;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMulticastPort() {
        return multicastPort;
    }

    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }

    public InetAddress getMulticastAddress() throws JournalNetworkException {
        if (multicastAddress == null) {
            NetworkInterface ifn = getNetworkInterface();
            try {
                if (!ifn.supportsMulticast()) {
                    throw new JournalNetworkException("Multicast is not supported on " + ifn.getName());
                }

                if (ifn.getInterfaceAddresses().size() == 0) {
                    throw new JournalNetworkException("No IP addresses assigned to " + ifn.getName());
                }

                if (isInet6(ifn.getInterfaceAddresses().get(0).getAddress())) {
                    multicastAddress = InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS_IPV6_1);
                } else {
                    multicastAddress = InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS_IPV4);
                }
            } catch (Exception e) {
                throw new JournalNetworkException(e);
            }
        }

        return multicastAddress;
    }

    public void setMulticastAddress(InetAddress multicastAddress) {
        this.multicastAddress = multicastAddress;
    }

    public String getIfName() {
        return ifName;
    }

    public void setIfName(String ifName) {
        this.ifName = ifName;
    }

    public int getSoRcvBuf() {
        return soRcvBuf;
    }

    public void setSoRcvBuf(int soRcvBuf) {
        this.soRcvBuf = soRcvBuf;
    }

    private NetworkInterface checkNetworkInterface(NetworkInterface ifn, String message) throws JournalNetworkException {
        if (ifn == null) {
            throw new JournalNetworkException(message);
        }

        try {
            if (!ifn.isUp()) {
                throw new JournalNetworkException("Network interface is DOWN: " + ifn);
            }
        } catch (SocketException e) {
            throw new JournalNetworkException("Cannot validate network interface", e);
        }
        return networkInterface = ifn;
    }

    public NetworkInterface getNetworkInterface() throws JournalNetworkException {
        if (networkInterface != null) {
            return networkInterface;
        }

        try {
            if (hostname != null) {
                hostAddress = InetAddress.getByName(hostname);
            }

            if (ifName != null) {
                return checkNetworkInterface(NetworkInterface.getByName(ifName), "Cannot find interface: " + ifName);
            }

            if (hostAddress != null && !hostAddress.isAnyLocalAddress()) {
                return checkNetworkInterface(NetworkInterface.getByInetAddress(hostAddress), "Cannot find network interface for host: " + hostname);
            }

            NetworkInterface ifn = NetworkInterface.getByInetAddress(InetAddress.getByName(InetAddress.getLocalHost().getHostName()));
            if (ifn != null && ifn.isUp()) {
                return networkInterface = ifn;
            }
            return checkNetworkInterface(defaultInterface, "System does not have default network interface");

        } catch (SocketException e) {
            throw new JournalNetworkException("Cannot get network interface", e);
        } catch (UnknownHostException e) {
            throw new JournalNetworkException(e);
        }
    }

    public InetSocketAddress getInterfaceSocketAddress() throws JournalNetworkException {
        NetworkInterface ifn = getNetworkInterface();
        List<InterfaceAddress> address = ifn.getInterfaceAddresses();
        if (address.size() == 0) {
            throw new JournalNetworkException("Interface does not have addresses: " + ifn);
        }

        if (hostAddress == null) {
            return new InetSocketAddress(address.get(0).getAddress(), getPort());
        } else {
            for (int i = 0, sz = address.size(); i < sz; i++) {
                InetAddress addr = address.get(i).getAddress();
                if (addr.getClass().equals(hostAddress.getClass())) {
                    return new InetSocketAddress(addr, getPort());
                }
            }

            throw new JournalNetworkException("Network interface " + ifn + " does not have " + (isInet6(hostAddress) ? "IPV6" : "IPV4") + " address");
        }


    }

    public SslConfig getSslConfig() {
        return sslConfig;
    }

    public boolean isEnableMulticast() {
        return enableMulticast;
    }

    public void setEnableMulticast(boolean enableMulticast) {
        this.enableMulticast = enableMulticast;
    }
}
