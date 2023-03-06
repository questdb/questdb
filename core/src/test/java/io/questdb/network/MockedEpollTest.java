/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.network;

import io.questdb.cutlass.http.HttpConnectionContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class MockedEpollTest {

    private static Epoll epoll;
    private static IODispatcherLinux ioDispatcherLinux;
    private static IOContextFactory<HttpConnectionContext> ioContextFactory;
    @BeforeClass
    public static void setUp(){
        epoll = mock(Epoll.class);
        IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration();
        ioContextFactory = mock(IOContextFactory.class);
        ioDispatcherLinux = new IODispatcherLinux(configuration, ioContextFactory, epoll);
    }

    @Test
    public void testRegister(){
        verify(epoll, times(1)).listen(anyInt()); // once in constructor
        ioDispatcherLinux.registerListenerFd();
        verify(epoll, times(2)).listen(anyInt());
    }

    @Test
    public void testUnregister(){
        ioDispatcherLinux.unregisterListenerFd();
        verify(epoll, times(1)).removeListen(anyInt());
    }
    
    // Test methods in Class ioDispatcherLinux
    @Test
    public void testCloseIoDispatcherLinux() {
        ioDispatcherLinux.close();
        Assert.assertTrue("closed", ioDispatcherLinux.closed);
    }

    // Test methods in Class Epoll
    @Test
    public void testEpoll(){
        ioDispatcherLinux.close();
        verify(epoll, times(1)).close();
    }
}
