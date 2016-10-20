package com.etisalat.log.common;

public class LogQueryException extends Exception {
    private static final long serialVersionUID = 1L;
    private int msgCode;

    public LogQueryException(Throwable cause) {
        super(cause);
    }

    public LogQueryException(String msg) {
        super(msg);
    }

    public LogQueryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public LogQueryException(String msg, int msgCode) {
        super(msg);
        this.msgCode = msgCode;
    }

    public int getMsgCode() {
        return this.msgCode;
    }
}
