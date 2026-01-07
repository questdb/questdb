package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;

@FunctionalInterface
public interface CursorFactoryProvider {
    /**
     * Supplies a freshly built factory so fuzz cases can vary inputs across tables.
     */
    RecordCursorFactory get() throws SqlException;
}
