package com.craiglowery.java.jobmgr;

/**
 * Created by craig on 2/22/2017.
 */
public class JobManagerException extends Exception {
    public JobManagerException() {
        super();
    }
    public JobManagerException(String message) {
        super(message);
    }
    public JobManagerException(String message, Throwable cause) {
        super(message,cause);
    }
}
