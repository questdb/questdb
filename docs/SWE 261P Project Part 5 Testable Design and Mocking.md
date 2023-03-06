# SWE 261P Project Part 5. Testable Design. Mocking of QuestDB

**Team Member: Jane He, Fengnan Sun, Ming-Hua Tsai**

**GitHub username: [SiyaoIsHiding](https://github.com/SiyaoIsHiding), [SoniaSun810](https://github.com/SoniaSun810), [alimhtsai](https://github.com/alimhtsai)**

**Table of Contents**
+ [1. Testable Design](#1-Testable Design)
    + [1.1 Aspects to Make a Testable Design](#11-Aspects to Make a Testable Design)
    + [1.2 Goals to Make a Testable Design](#12-Goals to Make a Testable Design)
+ [2. Mocking](#2-Mocking)
    + [2.1 Definition of Mocking](#21-Definition of Mocking)
    + [2.2 The Utility of Mocking](#22-The Utility of Mocking)
    + [2.3 Mock Testing vs Traditional Unit Testing](#23-Mock Testing vs Traditional Unit Testing)
+ [3. Documentation of Existing Code](#3-Documentation of Existing Code)
    + [3.1 A Difficult-Testing Code Example](#31-A Difficult-Testing Code Example)
    + [3.2 Advice to Fix the Code and Implementation](#32-Advice to Fix the Code and Implementation)
+ [4. New Test Cases](#4-New Test Cases)
    + [4.1 Mocking](#21-Mocking)
    + [4.2 Testable Design](#22-Testable Design)


# 1. Testable Design

## 1.1 Aspects to Make a Testable Design

When it comes to designing a system or software, there are many aspects that can make it more testable. Some of these include creating modular and cohesive code, using consistent naming conventions, minimizing dependencies, etc. Additionally, designing with testability in mind from the start can make it easier to create and execute tests, as well as identify and fix issues more quickly. Overall, a testable design is one that is structured, organized, and designed to facilitate testing at every stage of the development process. 

The professor emphasized that creating a testable design is crucial for ensuring software quality and reliability. By avoiding certain coding practices, such as using **private complex methods**, **static methods**, **hardcoding in "new" statements**, **logic in constructors**, and **the singleton pattern**, developers can create more modular and testable code that is easier to maintain and improve over time. Other aspects that can help us achieve testable design are as follows. 

- **Modularity**: A testable design is often broken down into smaller modules or components. These modules are designed to be self-contained and have well-defined interfaces, which makes them easier to test in isolation.
- **Separation of concerns**: A testable design separates different concerns or responsibilities into different modules or components. This ensures that each module is responsible for a specific task, which makes it easier to test and debug.
- **Loose coupling**: A testable design avoids tight coupling between different modules or components. This means that each module or component should be able to function independently of other modules, which makes it easier to test them in isolation.
- **Encapsulation**: A testable design encapsulates the internal state of each module or component. This means that the state of a module can only be modified through a well-defined interface. This makes it easier to test the module, as the internal state can be controlled and verified.
- **Clear and concise interfaces**: A testable design has clear and concise interfaces between different modules or components. This makes it easier to test each module in isolation, as the inputs and outputs of each module are well-defined.
- **Error handling**: A testable design has well-defined error handling mechanisms. This means that the design should be able to handle errors and exceptions in a consistent and predictable manner, which makes it easier to test and debug.

## 1.2 Goals to Make a Testable Design

A testable design should be built with the goal of ensuring that the design meets the required specifications and is robust, reliable, and efficient.

- **Facilitate testing**: A testable design is designed to be easily and effectively tested. This means that it should be easy to write test cases for each module or component, and that these test cases should be able to verify the correct functioning of the design.
- **Improve reliability**: A testable design is designed to be reliable. This means that it should be able to handle errors and exceptions in a consistent and predictable manner, and that it should be able to function correctly in different environments and conditions.
- **Simplify maintenance**: A testable design is designed to be easy to maintain. This means that it should be easy to modify and update each module or component without affecting other parts of the design.

# 2. Mocking

## 2.1 Definition of Mocking

Software testing mocking is a technique that involves creating fake or simulated objects, functions, or services to replicate the behavior of the real components of a software application. These simulated components, called "mocks," are designed to emulate the behavior of the real components and allow software developers and testers to isolate and test specific parts of an application in isolation. Mock is useful for interaction testing, as opposed to state testing. 

## 2.2 The Utility of Mocking

The utility of mocking lies in its ability to help developers and testers ensure that their software applications are functioning as intended. By using mocks, developers can simulate the behavior of the various components of an application and test them in isolation, without having to worry about the behavior of other parts of the system. This can help to identify bugs and other issues that may be difficult to find in a complex, integrated system. In addition, mocking can help to speed up the software development process by allowing developers to test their code more quickly and efficiently. By isolating specific parts of an application and testing them in isolation, developers can quickly identify and fix issues before moving on to the next phase of development.

The main advantages of mock testing are:

- **Isolation of code**: Mock testing allows developers to isolate and test specific parts of an application without having to worry about the behavior of other parts of the system. This can help to identify bugs and other issues that may be difficult to find in a complex, integrated system.
- **Speed of testing**: Since mock testing focuses on specific parts of an application, it can help developers to test their code more quickly and efficiently. By using mocks, developers can simulate the behavior of various components and test them in isolation, without having to wait for the entire system to be up and running.
- **Reduces dependencies**: Mock testing can help to reduce dependencies on external services or resources. By creating mock objects or functions, developers can test their code without having to rely on external resources that may be difficult to set up or access.
- **Easier to maintain**: Mock testing can make it easier to maintain and update code. By using mocks, developers can make changes to specific parts of an application without having to worry about the behavior of other parts of the system. This can make it easier to update code and fix issues.

## 2.3 **Mock Testing vs Traditional Unit Testing**

Below are some of the [differences between mock testing and traditional unit testing](https://www.geeksforgeeks.org/software-testing-mock-testing/):

| Mock Testing | Traditional Unit Testing |
| --- | --- |
| In mock testing, assertions are done by mock objects and no assertions are required from unit testing | While in traditional unit testing assertions are done for dependencies. |
| In mock testing, it is focused on how the fake/mock object can be incorporated and tested. | In traditional unit testing, it was focused on how the real object can be incorporated and tested. |
| Mock testing is more about behavior-based verification. | Traditional unit testing is more about state-based verification. |

# 3. Documentation of Existing Code

## 3.1 A Difficult-Testing Code Example

QuestDB is considered to be a well-designed and testable system due to several factors, like **modular architecture**, **code simplicity**, **strong adherence to standards**, **continuous integration and testing**, **open-source community**, etc. QuestDB's design and development practices make it a well-designed and testable system that is easy to work with and maintain. This, in turn, makes it a popular choice for developers who are looking for a fast, reliable, and scalable database solution. Its current test coverage is already very high (96% for class coverage, 90% for method coverage, 91% for line coverage). We tried hard to find some untestable cases in QuestDB based on the 5 main rules in testable design that mentioned in class:

1. **Avoid complex private methods**: complex logic in private methods can be a source for bugs that cannot be found by direct testing.
2. **Avoid static methods**: static methods are methods that belong to a class rather than an instance of the class (i.e. object). They can make it difficult to isolate and test the behavior of a component.
3. **Be careful hardcoding in “new”**: We need to allow the object reference to be created outside the method and passed in (dependency injection). 
4. **Avoid logic in constructors**: better to have a more simple constructor and have the functionality placed in another method.
5. **Avoid singleton pattern**: there is appropriate use of this pattern, however, make sure that it is something that does not need to be swapped out for testing.

While its design is very testable and robust, we found a case in QuestDB where the code does not align with testable design principle that a “new” is being used within a method. Their current code is:

**Original `IODispatcherLinux` class**

```java
public class IODispatcherLinux<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private static final int EVM_DEADLINE = 1;
    private static final int EVM_ID = 0;
    private static final int EVM_OPERATION_ID = 2;
    protected final LongMatrix pendingEvents = new LongMatrix(3);
    **private final Epoll epoll;**
    // the final ids are shifted by 1 bit which is reserved to distinguish socket operations (0) and suspend events (1);
    // id 0 is reserved for operations on the server fd
    private long idSeq = 1;

    public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        **this.epoll = new Epoll(configuration.getEpollFacade(), configuration.getEventCapacity());** 
        registerListenerFd();
    }
```

**Original `Epoll` class**

```java
**public final class Epoll implements Closeable** {
    private static final Log LOG = LogFactory.getLog(Epoll.class);

    private final int capacity;
    private final EpollFacade epf;
    private final int epollFd;
    private final long events;
    private long _rPtr;
    private boolean closed = false;

    public Epoll(EpollFacade epf, int capacity) {
        this.epf = epf;
        this.capacity = capacity;
        this.events = this._rPtr = Unsafe.calloc(EpollAccessor.SIZEOF_EVENT * (long) capacity, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        this.epollFd = epf.epollCreate();
        if (epollFd < 0) {
            throw NetworkError.instance(epf.errno(), "could not create epoll");
        }
        Files.bumpFileCount(this.epollFd);
    }
```

Epoll is a scalable I/O event notification mechanism in Linux operating system. It is used to monitor multiple file descriptors (sockets, file handles, etc.) to see if any of them have I/O events pending. Epoll is a replacement for the older and less scalable `select` and `poll` mechanisms, and it provides better performance in high-volume and high-concurrency I/O applications. Epoll is commonly used in network servers, where it can handle thousands or even millions of client connections efficiently.

In the context of QuestDB, Epoll is used in the `IODispatcherLinux` class to handle I/O events on network sockets. The class uses Epoll to efficiently manage the selection of sockets that are ready for I/O operations, allowing QuestDB to handle a large number of concurrent network connections with low latency and high throughput.

However, in the original implementation of class `IODispatcherLinux` of QuestDB (src/main/java/io/questdb/network/IODispatcherLinux.java), it is not possible to test the interaction between `IODispatcherLinux` and `Epoll` for the following reasons:

1. `Epoll` is a `private final` field of `IODispatcherLinux`, meaning that it cannot be directly accessed or modified from outside the class. This makes it difficult to write tests that can verify the behavior of the `IODispatcherLinux` ****class when interacting with the `Epoll` field.

2. `Epoll` is instantiated as a `new` object in the constructor of `IODispatcherLinux`, which means that it is tightly coupled to the implementation of `IODispatcherLinux`. This makes it difficult to substitute `Epoll` ****with a test double or a mock object, which would allow for more flexible and targeted testing.

3. `Epoll` is a `final` class. Since a `final` class cannot be extended or subclassed, it cannot be mocked using most mocking frameworks, which typically rely on subclassing or extending the original class to create a mock object.

## 3.2 Advice to Fix the Code and Implementation

Since class `Epoll` and `IODispatcherLinux` are closely coupled, `Epoll` can not be mocked and the interaction between `IODispatcherLinux` and `Epoll` can not be directly tested. Therefore, we changed the codes into a testable design by allowing the `Epoll` object reference to be created outside the `IODispatcherLinux` method and passed in (i.e. dependency injection). 

We modified the code in the following steps:

1. Change the `Epoll` field from `private final` to `protected final`. By changing a `private` field to `protected`, we allow subclasses and other classes in the same package to access the field. This means that we can write tests which subclass the object under test or place the test class in the same package, and access the field directly to set up the object's state for testing. This makes the object more testable, without compromising its encapsulation or making the field fully `public`.

2. Overload the constructor of `IODispatcherLinux` so that a mock object of `Epoll` can be passed in. By overloading the constructor of the class, we allow a mock object to be passed in as a parameter to the constructor. The developer can isolate and test individual components of the code without needing to involve all the dependencies of the component being tested.

3. Deleted the `final` modifier of `Epoll` class. Final classes are typically designed to provide specific behavior that cannot be altered by other classes, so creating a mock object that overrides the behavior of a final class can lead to unexpected results. Therefore, in order to create a mock object of `Epoll`, we delete the `final` modifier of it. 

**Modified `IODispatcherLinux` class**

```java
public class IODispatcherLinux<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private static final int EVM_DEADLINE = 1;
    private static final int EVM_ID = 0;
    private static final int EVM_OPERATION_ID = 2;
    protected final LongMatrix pendingEvents = new LongMatrix(3);
    **protected final Epoll epoll;**
    // the final ids are shifted by 1 bit which is reserved to distinguish socket operations (0) and suspend events (1);
    // id 0 is reserved for operations on the server fd
    private long idSeq = 1;

    **public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            Epoll epoll
    ) {
        super(configuration, ioContextFactory);
        this.epoll = epoll;
        registerListenerFd();
    }**

    public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.epoll = new Epoll(configuration.getEpollFacade(), configuration.getEventCapacity());
        registerListenerFd();
    }
```

**Modified `Epoll` class**

```java
**public class Epoll implements Closeable** {
    private static final Log LOG = LogFactory.getLog(Epoll.class);

    private final int capacity;
    private final EpollFacade epf;
    private final int epollFd;
    private final long events;
    private long _rPtr;
    private boolean closed = false;

    public Epoll(EpollFacade epf, int capacity) {
        this.epf = epf;
        this.capacity = capacity;
        this.events = this._rPtr = Unsafe.calloc(EpollAccessor.SIZEOF_EVENT * (long) capacity, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        this.epollFd = epf.epollCreate();
        if (epollFd < 0) {
            throw NetworkError.instance(epf.errno(), "could not create epoll");
        }
        Files.bumpFileCount(this.epollFd);
    }
```

Mocking the `Epoll` object would allow for a type of behavior checking that may not be afforded without mocking because it would allow for isolated testing of the `IODispatcherLinux` object's behavior when interacting with the `Epoll` object.

In the original code where `Epoll` and `IODispatcherLinux` were closely coupled, it would be difficult to test the behavior of `IODispatcherLinux` without also testing the behavior of `Epoll`, and any errors or issues with `Epoll` could potentially cause false positives or negatives in testing the behavior of `IODispatcherLinux`. After modifying the current source codes to a testable design, we are able to mock the `Epoll` object. As a result, the behavior of `IODispatcherLinux` can be tested in isolation from the behavior of `Epoll`, allowing for more accurate testing of `IODispatcherLinux`'s behavior.

# 4. New Test Cases

## 4.1 Mocking

We added 2 test cases in `io.questdb.network.MockedEpollTest` to test the interaction between `IODispatcherLinux` and `Epoll` using the Mockito framework.

First, the `epoll` and `ioContextFactory` are mocked. The `ioDispatcherLinux` object is instantiated with the help of the mocked objects. Next, the function `io.questdb.network.IODispatcherLinux#registerListenerFd` is tested. This function should have been called once when the object `ioDispatcherLinux` is constructed. As a part of the `registerListenerFd` function, `io.questdb.network.Epoll#listen` should have been called once on the mocked `epoll` object. After being explicitly called in the test case, it should have been called twice. They are verified by the `verify` function provided by the Mockito framework.

Lastly, the function `io.questdb.network.IODispatcherLinux#unregisterListenerFd` is tested. As a part of the function, `io.questdb.network.Epoll#removeListen` should have been called once. It is also verified by the `verify` function from Mockito. As the file descriptor may change in different environments, `anyInt()` is used in the verification instead of a specific integer.

To conclude, the interaction between the `IODispatcherLinux` and `Epoll`, specifically the process of registering and unregistering the listener file descriptors behave as expected and is verified with the help of the mocked objects.

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

## 4.2 Testable Design

To make the `Epoll` object work on a MacOS machine, we need to use a mock object because the `IoDispatcherLinux` can only be initialized on a Linux machine. Thus, to create testable code, we must mock objects.

We added 2 JUnit test cases in `io.questdb.network.MockedEpollTest` to test the functionality of `IODispatcherLinux` and `Epoll`. First, we initialized the `ioDispatcherLinux` object with the mocked `Epoll` and `ioContextFactory`. We then tested the `io.questdb.network.IODispatcherLinux#close` function to confirm that both the `ioDispatcherLinux` and `Epoll` objects are closed properly. 

By implementation of these tests, we increased the method coverage of `Epoll` and `IoDispatcherLinux` by 10% and 15% respectively, which was 0% previously. 

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
		
		// Test methods in Class ioDispatcherLinux
		@Test
    public void testCloseioDispatcherLinux() {
        ioDispatcherLinux.close();
        Assert.assertTrue("closed", ioDispatcherLinux.closed);
    }

		// Test methods in Class Epoll
    @Test
    public void testClose(){
        ioDispatcherLinux.close();
        verify(ioDispatcherLinux.epoll, times(1)).close();
    }
}
```