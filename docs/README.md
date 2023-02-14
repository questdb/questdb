<div align="center">
  <a href="https://questdb.io/" target="blank"><img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/></a>
</div>
<p>&nbsp;</p>

# SWE 261P Project Part 2: Functional Testing and Finite State Machines of QuestDB

**Team Member: Jane He, Fengnan Sun, Ming-Hua Tsai**

**GitHub username: [SiyaoIsHiding](https://github.com/SiyaoIsHiding), [SoniaSun810](https://github.com/SoniaSun810), [alimhtsai](https://github.com/alimhtsai)**

**Table of Contents**
1. [Finite Models for Testing](#first)
2. [Implementations of Finite State Machine](#second)
3. [New JUnit Test Cases of Finite State Machine](#third)

---

## 1. Finite Models for Testing <a name="first"></a>

[A finite-state machine (FSM) or finite-state automaton (FSA, plural: automata)](https://en.wikipedia.org/wiki/Finite-state_machine), finite automaton, or simply a state machine, is a mathematical model of computation. It is an abstract machine that can be in exactly one of a finite number of states at any given time. They are useful for testing because they allow you to isolate specific parts of a system and test them in a controlled environment without having to worry about the complexity of the overall system.

Advantages of using FSM to implement testing:

- Allow testers to identify potential issues and bugs early in the development process, before they become a major problem.
- Make it easier to perform large-scale and repetitive testing. This can also reduce the cost of testing, as it minimizes the need for manual intervention.
- Help to test the performance of a system, by modeling different load conditions and measuring how the system behaves under stress.

## 2. Implementations of Finite State Machine <a name="second"></a>

We have selected a component of the TABLE in QuestDB to construct our finite state machine, which comprises two distinct states, referred to as nodes: `Empty Table` and `Non-empty Table`. These two states represent the current state of the table and determine the actions that can be performed on it.

Transitions between these two states are executed through two primary actions, referred to as edges: `Insert data` and `Truncate table`. These two edges represent the two key actions that can be performed on a table and determine the resulting state of the table. Additionally, there is a third edge, `Update data`, which can be performed on a `Non-empty Table` state.

1. When starting a QuestDB, the first step is to create a new table. This is accomplished by executing a CREATE TABLE statement in SQL. Upon successful execution, the database transitions into the `Empty Table` state, representing that the table has been created, but no data has been inserted into it.
2. From the `Empty Table` state, we can `Insert data` into the table by executing an INSERT INTO statement in SQL. This results in a transition of the database into the `Non-empty Table` state, representing that the table now contains data.
3. Once in the `Non-empty Table` state, we have the ability to `Update data` within the table. This can be achieved by executing an UPDATE statement in SQL. Importantly, performing this action does not result in a transition of the database state, as the contents of the table have changed, but the overall state of the table remains `Non-empty Table`.
4. When we wish to clear the contents of the table, we can `Truncate table` by executing a TRUNCATE TABLE statement in SQL. This causes all data within the table to be removed, effectively making the table empty once again. Upon successful execution of the TRUNCATE TABLE statement, the database transitions back to the `Empty Table` state.

<p align="center">
  <img src="FSM.png" />
</p>

## 3. New JUnit Test Cases of Finite State Machine <a name="third"></a>

We have enhanced our previous finite state machine implementation by incorporating 6 JUnit test cases located in the `core/src/test/java/testingdebugging/FSMTest.java` file. By utilizing JUnit, we are able to automate the testing process and verify the proper functioning of our finite state machine. This helps to identify and fix potential bugs, and ensures the code remains reliable and consistent as changes are made.

### (1) `#testQuestDBStateMachineAtEmptyTableState`

This test case evaluates whether the state is in the `Empty Table` state.

Upon executing the CREATE TABLE statement, a new table named "tab" is created with two columns, "id" and "text", having data types of integer and string, respectively. When we query the "tab" in QuestDB, the actual response matches the expected outcome which show the header of the “tab”, and the table is empty.

| Attribute                | Content                                |
|--------------------------|----------------------------------------|
| The statement I executed | create table tab (id int, text string) |
| The pattern I queried    | tab                                    |
| The expected response    | id\ttext\n                             |
| The actual response      | id\ttext\n                             |

### (2) `#testQuestDBStateMachineAtEmptyTableStateInsert`

This test case evaluates the transition from the `Empty Table` state to the `Non-empty Table` state through the `Insert data` action.

An empty table named "tab" is used for this test. The INSERT statement is executed to add data to the table. Upon querying the "tab" in QuestDB, the actual response matches the expected outcome, showcasing the current data information.

| Attribute                | Content                                                                      |
|--------------------------|------------------------------------------------------------------------------|
| The statement I executed | create table tab (id int, text string)<br>insert into tab values (1, 'test') |
| The pattern I queried    | tab                                                                          |
| The expected response    | id\ttext\n + "1\ttest\n                                                      |
| The actual response      | id\ttext\n + "1\ttest\n                                                      |

```java
public void createTab() throws SqlException {
    compiler.compile("create table tab (id int, text string)", sqlExecutionContext);
}

@Test
public void testQuestDBStateMachineAtEmptyTableState() throws Exception {
    assertMemoryLeak(() -> {
                createTab();
                assertSql("'tab'", "id\ttext\n");
                assertQuery(
                        "count\n" + "0\n",
                        "select count() from tab",
                        null,
                        false,
                        true
                );
            }
    );
}

@Test
public void testQuestDBStateMachineAtEmptyTableStateInsert() throws Exception {
    assertMemoryLeak(
            () -> {
                createTab();
                executeInsert("insert into tab values (1, 'test')");
                assertSql("'tab'", "id\ttext\n" + "1\ttest\n");
            }
    );
}
```

### (3) `#testQuestDBStateMachineAtEmptyTableStateUpdate`

This test case assesses the transition from the `Empty Table` state to the `Empty Table` state through the `Update data` action.

An empty table named "tab" is used for this test. The UPDATE statement is executed to modify the specified data in the table. When querying the "tab" in QuestDB, the actual response aligns with the expected outcome, displaying the information of the empty table “tab”.

| Attribute                | Content                                                                                     |
|--------------------------|---------------------------------------------------------------------------------------------|
| The statement I executed | create table tab (id int, text string)<br>update tab set text = 'test2' where text = 'test' |
| The pattern I queried    | tab                                                                                         |
| The expected response    | id\ttext\n                                                                                  |
| The actual response      | id\ttext\n                                                                                  |

### (4) `#testQuestDBStateMachineAtNonEmptyTableStateUpdate`

This test case assesses the transition from the `Non-empty Table` state to the `Non-empty Table` state through the `Update data` action.

An empty table named "tab" is used for this test. Initially, data is inserted into the table through the execution of the INSERT statement. Next, the UPDATE statement is executed to modify the previously inserted data. The actual response upon querying the "tab" in QuestDB matches the expected outcome, displaying the updated information of the "tab".

| Attribute                | Content                                                                                                                           |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| The statement I executed | create table tab (id int, text string)<br>insert into tab values (1, 'test')<br>update tab set text = 'test2' where text = 'test' |
| The pattern I queried    | tab                                                                                                                               |
| The expected response    | id\ttext\n + "1\ttest2\n                                                                                                          |
| The actual response      | id\ttext\n + "1\ttest2\n                                                                                                          |

```java
public void createTab() throws SqlException {
    compiler.compile("create table tab (id int, text string)", sqlExecutionContext);
}

@Test
public void testQuestDBStateMachineAtEmptyTableStateUpdate() throws Exception {
    assertMemoryLeak(() -> {
        createTab();
        executeUpdate("update tab set text = 'test2' where text = 'test'");
        assertSql("'tab'", "id\ttext\n");
        assertQuery(
                "count\n" + "0\n",
                "select count() from tab",
                null,
                false,
                true
        );
    });
}

@Test
public void testQuestDBStateMachineAtNonEmptyTableStateUpdate() throws Exception {
    assertMemoryLeak(() -> {
        createTab();
        executeInsert("insert into tab values (1, 'test');");
        executeUpdate("update tab set text = 'test2' where text = 'test'");
        assertSql("'tab'", "id\ttext\n" + "1\ttest2\n");
    });
}
```

### (5) `#testQuestDBStateMachineAtEmptyTableStateTruncate`

This test case assesses the transition from the `Empty Table` state to the `Empty Table` state through the `Truncate Table` action.

An empty table named "tab" was used for this test. The Truncate Table tab statement was executed to delete all data. Upon querying the "tab" in QuestDB, the actual response matched the expected outcome, showcasing the table was still empty.

| Attribute                | Content                                                      |
|--------------------------|--------------------------------------------------------------|
| The statement I executed | create table tab (id int, text string)<br>truncate table tab |
| The pattern I queried    | tab                                                          |
| The expected response    | id\ttext\n                                                   |
| The actual response      | id\ttext\n                                                   |

### (6) `#testQuestDBStateMachineAtNonEmptyTableStateTruncate`

This test case assesses the transition from the `Non-empty Table` state to the `Empty Table` state through the `Truncate Table` action.

A table named "tab" was used for this test. Initially, data was inserted into the table through the execution of the INSERT statement, and table “tab” became non-empty. Next, The Truncate Table tab statement was executed to delete all data. The actual response upon querying the "tab" in QuestDB matched the expected outcome, showcasing the table is empty again.

| Attribute                | Content                                                                                            |
|--------------------------|----------------------------------------------------------------------------------------------------|
| The statement I executed | create table tab (id int, text string)<br>insert into tab values (1, 'test')<br>truncate table tab |
| The pattern I queried    | tab                                                                                                |
| The expected response    | id\ttext\n                                                                                         |
| The actual response      | id\ttext\n                                                                                         |

```java
public void createTab() throws SqlException {
    compiler.compile("create table tab (id int, text string)", sqlExecutionContext);
}

@Test
public void testQuestDBStateMachineAtEmptyTableStateTruncate() throws Exception {
    assertMemoryLeak(() -> {
        createTab();
        Assert.assertEquals(TRUNCATE, compiler.compile("truncate table tab;", sqlExecutionContext).getType());
        assertSql("'tab'", "id\ttext\n");
        assertQuery(
                "count\n" +
                        "0\n",
                "select count() from tab",
                null,
                false,
                true
        );
    });
}

@Test
public void testQuestDBStateMachineAtNonEmptyTableStateTruncate() throws Exception {
    assertMemoryLeak(() -> {
        createTab();
        executeInsert("insert into tab values (1, 'test')");
        Assert.assertEquals(TRUNCATE, compiler.compile("truncate table tab;", sqlExecutionContext).getType());
        assertSql("'tab'", "id\ttext\n");
        assertQuery(
                "count\n" +
                        "0\n",
                "select count() from tab",
                null,
                false,
                true
        );
    });
}
```