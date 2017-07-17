package com.questdb.cairo;

/**
 *
 */
@FunctionalInterface
interface RowFunction {
    TableWriter.Row newRow(long timestamp);
}
