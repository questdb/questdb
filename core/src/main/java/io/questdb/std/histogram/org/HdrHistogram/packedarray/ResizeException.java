package io.questdb.std.histogram.org.HdrHistogram.packedarray;

class ResizeException extends Exception {
    private int newSize;

    ResizeException(final int newSize) {
        this.newSize = newSize;
    }

    int getNewSize() {
        return newSize;
    }
}
