/******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 ******************************************************************************/
package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class DataSyncFailureClassificationTest extends AbstractTest {

    @Test
    public void testSynchronousDurabilityOperationsAreClassified() {
        final FilesFacade ff = new FilesFacadeImpl();
        assertClassified("fsync", () -> ff.fsync(-1));
        assertClassified("fdatasync", () -> ff.fdatasync(-1));
        assertClassified("syncfs", () -> ff.syncfs(-1));
        assertClassified("fsyncAndClose", () -> ff.fsyncAndClose(-1));
        assertClassified("msync", () -> ff.msync(1, 1, false));
    }

    @Test
    public void testAsynchronousMsyncIsNotClassifiedAsDurabilityFailure() {
        final FilesFacade ff = new FilesFacadeImpl();
        final CairoException exception = Assert.assertThrows(
                CairoException.class,
                () -> ff.msync(1, 1, true)
        );
        Assert.assertFalse(exception.isDataSyncFailure());
    }

    private static void assertClassified(String operation, Runnable call) {
        final CairoException exception = Assert.assertThrows(CairoException.class, call::run);
        Assert.assertTrue(operation + " must be classified", exception.isDataSyncFailure());
        Assert.assertEquals(operation, exception.getDataSyncOperation());
    }
}
