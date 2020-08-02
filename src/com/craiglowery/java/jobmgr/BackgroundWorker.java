package com.craiglowery.java.jobmgr;

/**
 * Created by craig on 2/23/2017.
 */
public abstract class BackgroundWorker {

    /**
     * Performs the actual work for the job and returns an Object as the result.
     * @return The result.
     * @throws Exception Thrown to indicate an error during the work.
     */
    abstract public Object work() throws Exception;

}
