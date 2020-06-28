package io.questdb.cutlass.http;

public class MultipartParserState {
    public long headerEnd;
    public int read;
    public boolean newRequest;
    public boolean multipartRetry;
    public long start;
    public long buf;
    public int bufRemaining;
    public int state;
//
//    public void saveHttpContextState(long headerEnd, int read, boolean newRequest) {
//        this.multipartRetry = true;
//        this.headerEnd = headerEnd;
//        this.read = read;
//        this.newRequest = newRequest;
//    }

    public void saveFdBufferPosition(long start, long buf, int bufRemaining, int state) {
        this.state = state;
        this.multipartRetry = true;
        this.start = start;
        this.buf = buf;
        this.bufRemaining = bufRemaining;
    }
}
