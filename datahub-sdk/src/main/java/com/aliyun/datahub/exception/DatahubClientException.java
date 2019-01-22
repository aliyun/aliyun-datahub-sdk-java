package com.aliyun.datahub.exception;


/**
 * Base exception class for any errors that occur while attempting to use an Datahub
 * client from Datahub SDK for Java.
 * 
 * Error responses from services will be handled as DatahubServiceExceptions.
 * This class is primarily for errors that occur when unable to get a response
 * from a service. For example, if a caller tries to use a client to make a service
 * call, but no network connection is present, an DatahubClientException will be
 * thrown to indicate that the client wasn't able to successfully make the
 * service call, and no information from the service is available.
 * 
 * Note : If the SDK is able to parse the response; but doesn't recognize the
 * error code from the service, an DatahubServiceException is thrown
 * 
 * Callers should typically deal with exceptions through DatahubServiceException,
 * which represent error responses returned by services. DatahubServiceException
 * has much more information available for callers to appropriately deal with
 * different types of errors that can occur.
 *
 * @see DatahubServiceException
 */
public class DatahubClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new DatahubClientException with the specified message, and root
     * cause.
     *
     * @param message An error message describing why this exception was thrown.
     * @param t       The underlying cause of this exception.
     */
    public DatahubClientException(String message, Throwable t) {
        super(message, t);
    }

    /**
     * Creates a new DatahubClientException with the specified message.
     *
     * @param message An error message describing why this exception was thrown.
     */
    public DatahubClientException(String message) {
        super(message);
    }

    public DatahubClientException(Throwable t) {
        super(t);
    }
}
