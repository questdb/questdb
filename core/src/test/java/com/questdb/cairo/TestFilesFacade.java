package com.questdb.cairo;

import com.questdb.misc.FilesFacadeImpl;

public abstract class TestFilesFacade extends FilesFacadeImpl {
    public abstract boolean wasCalled();
}
