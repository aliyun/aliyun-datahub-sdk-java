package com.aliyun.datahub.exception;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.rest.DatahubHttpHeaders;

/**
 * Extension of DatahubClientException that represents an error response returned
 * by an Datahub service. Receiving an exception of this type indicates that
 * the caller's request was correctly transmitted to the service, but for some
 * reason, the service was not able to process it, and returned an error
 * response instead.
 * 
 * DatahubServiceException provides callers several pieces of
 * information that can be used to obtain more information about the error and
 * why it occurred. In particular, the errorType field can be used to determine
 * if the caller's request was invalid, or the service encountered an error on
 * the server side while processing it.
 */
public class DatahubServiceException extends DatahubClientException {
    private static final long serialVersionUID = 1L;

    /**
     * The unique Datahub identifier for the service request the caller made. The
     * Datahub request ID can uniquely identify the Datahub request, and is used for
     * reporting an error to Datahub support team.
     */
    private String requestId;

    /**
     * The Datahub error code represented by this exception (ex:
     * InvalidParameterValue).
     */
    private String errorCode;

    /**
     * The error message as returned by the service.
     */
    private String errorMessage;

    /**
     * The HTTP status code that was returned with this error
     */
    private int statusCode;

    /**
     * Constructs a new DatahubServiceException with the specified message.
     *
     * @param errorMessage An error message describing what went wrong.
     */
    public DatahubServiceException(String errorMessage) {
        super((String) null);
        this.errorMessage = errorMessage;
    }

    /**
     * Constructs a new DatahubServiceException with the specified message and
     * exception indicating the root cause.
     *
     * @param errorMessage An error message describing what went wrong.
     * @param cause        The root exception that caused this exception to be thrown.
     */
    public DatahubServiceException(String errorMessage, Exception cause) {
        super(null, cause);
        this.errorMessage = errorMessage;
    }

    /**
     * Constructs a new DatahubServiceException with the specified response.
     *
     * @param errorCode The Datahub error code represented by this exception.
     * @param errorMessage An error message describing what went wrong.
     * @param response A response received from server.
     */
    public DatahubServiceException(String errorCode, String errorMessage, Response response) {
        super((String) null);
        this.requestId = response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.statusCode = response.getStatus();
    }

    /**
     * Sets the Datahub requestId for this exception.
     *
     * @param requestId The unique identifier for the service request the caller made.
     */
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * Returns the Datahub request ID that uniquely identifies the service request
     * the caller made.
     *
     * @return The Datahub request ID that uniquely identifies the service request
     * the caller made.
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Sets the Datahub error code represented by this exception.
     *
     * @param errorCode The Datahub error code represented by this exception.
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Returns the Datahub error code represented by this exception.
     *
     * @return The Datahub error code represented by this exception.
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * @return the human-readable error message provided by the service
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Sets the human-readable error message provided by the service.
     * 
     * NOTE: errorMessage by default is set to the same as the message value
     * passed to the constructor of DatahubServiceException.
     *
     */
    public void setErrorMessage(String value) {
        errorMessage = value;
    }

    /**
     * Sets the HTTP status code that was returned with this service exception.
     *
     * @param statusCode The HTTP status code that was returned with this service
     *                   exception.
     */
    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * Returns the HTTP status code that was returned with this service
     * exception.
     *
     * @return The HTTP status code that was returned with this service
     * exception.
     */
    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getMessage() {
        return getErrorMessage()
                + " (Status Code: " + getStatusCode()
                + "; Error Code: " + getErrorCode()
                + "; Request ID: " + getRequestId() + ")";
    }
}
