package com.aliyun.datahub.client.exception;

public class DatahubClientException extends RuntimeException {
    private int httpStatus = 5001;
    private String requestId;
    private String errorCode;
    private String errorMessage;

    public DatahubClientException(String message) {
        super(message);
        this.errorMessage = message;
    }

    public DatahubClientException(int httpStatus, String requestId, String errorCode, String errorMessage) {
        super(errorMessage);
        this.httpStatus = httpStatus;
        this.requestId = requestId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public DatahubClientException(DatahubClientException ex) {
        this(ex.getHttpStatus(), ex.getRequestId(), ex.getErrorCode(), ex.getErrorMessage());
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String getMessage() {
        return "[httpStatus:" + httpStatus + ", requestId:" +
                requestId + ", errorCode:" + errorCode +
                ", errorMessage:" + errorMessage + "]";
    }
}