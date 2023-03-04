Find a feature that could be mocked (that is not already) and would be good to be tested with mocking. Document this feature and how mocking would allow for a type of behavior checking not afforded without mocking. (40%)
Write a test case using Mockito that uses mocking to test that feature. (40%)

### Testable Design
**Informal, Modify it before use**
In the original implementation of QuestDB, it is not possible to test the interaction between `IODispatcherLinux` and `Epoll`. This is because:
1. `epoll` is a `private final` field of `IODispatcherLinux`.
2. `epoll` is instantiated as a `new` object in the constructor of `IODispatcherLinux`.
3. `Epoll` is a `final` class

To summarize, class `Epoll` and `IODispatcherLinux` are closely coupled. `Epoll` can not be mocked. The interaction between `IODispatcherLinux` and `Epoll` can not be directly tested.

Therefore, we changed the source codes into a testable design:
1. Change the `epoll` field from `private` to `protected` 
2. Overload the constructor of `IODispatcherLinux` (in `io/questdb/network/IODispatcherLinux.java`) so that a mock object of `Epoll` can be passed in
3. Deleted the `final` modifier of `Epoll` class

### New Test Cases of Mocking
We added some new test cases in `io.questdb.network.MockedEpollTest` to test the interaction between `IODispatcherLinux` and `Epoll` using the Mockito framework.

```java
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
}
```

First, the `epoll` and `ioContextFactory` are mocked. The `ioDispatcherLinux` object is instantiated with the help of the mocked objects.

Next, the function `io.questdb.network.IODispatcherLinux#registerListenerFd` is tested. This function should have been called once when the object `ioDispatcherLinux` is constructed. As a part of the `registerListenerFd` function, `io.questdb.network.Epoll#listen` should have been called once on the mocked `epoll` object. After being explicitly called in the test case, it should have been called twice. They are verified by the `verify` function provided by the Mockito framework.

Lastly, the function `io.questdb.network.IODispatcherLinux#unregisterListenerFd` is tested. As a part of the function, `io.questdb.network.Epoll#removeListen` should have been called once. It is also verified by the `verify` function from Mockito.

As the file descriptor may change in different environments, `anyInt()` is used in the verification instead of a specific integer.

To conclude, the interaction between the `IODispatcherLinux` and `Epoll`, specifically the process of registering and unregistering the listener file descriptors behave as expected and is verified with the help of the mocked objects.
