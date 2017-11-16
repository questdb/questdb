package com.questdb.cutlass.receiver.parser;

final class Utf8RepairContinue extends RuntimeException {
    final static Utf8RepairContinue INSTANCE = new Utf8RepairContinue();
}
