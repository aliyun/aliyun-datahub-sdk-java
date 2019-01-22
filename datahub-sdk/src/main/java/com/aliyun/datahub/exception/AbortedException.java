package com.aliyun.datahub.exception;

/**
 * SDK operation aborted exception.
 */
public class AbortedException extends DatahubClientException {
    private static final long serialVersionUID = 1L;

    public AbortedException(String message, Throwable t) {
        super(message, t);
    }

    public AbortedException(Throwable t) {
        super("", t);
    }

    public AbortedException(String message) {
        super(message);
    }

    public AbortedException() {
        super("");
    }
}
