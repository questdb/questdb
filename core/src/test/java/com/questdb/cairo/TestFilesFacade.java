package com.questdb.cairo;

import com.questdb.std.FilesFacadeImpl;

public abstract class TestFilesFacade extends FilesFacadeImpl {
    public abstract boolean wasCalled();
}
