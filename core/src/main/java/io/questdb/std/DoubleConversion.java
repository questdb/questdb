package io.questdb.std;

class DoubleConversion {
    static {
        Os.init();
    }

    static native String append(double value, int scale);
}
