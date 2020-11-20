package com.craiglowery.java.vlib.clients.server.job;

/**
 * Provides special exception processing for vlib server jobs.
 */
public class VlibServerException extends Exception {

    private VlibRepositoryResponse vlibRepositoryResponse =null;
    private String log=null;
    private String prepend=""; //Exception messages from the chain of exceptions.

    public VlibRepositoryResponse getRepositoryResponse() {
        return vlibRepositoryResponse;
    }

    public VlibServerException(String msg) {
        super(msg);
    }


    /**
     * Constructs ServerException with a message and information derived from a {@link VlibRepositoryResponse}.
     * @param msg The message.
     * @param vlibRepositoryResponse The {@link VlibRepositoryResponse} from which additional message data will be drawn.
     */
    public VlibServerException(String msg, VlibRepositoryResponse vlibRepositoryResponse) {
        super(msg+(vlibRepositoryResponse ==null?"":(": "+ vlibRepositoryResponse.getResponseSummary())));
        this.vlibRepositoryResponse = vlibRepositoryResponse;
    }

    /**
     * Constructs a ServerException with a message and information derived from a {@link Throwable} object.
     * @param msg
     * @param cause The {@link Throwable} from which additional message data will be drawn.
     */
    public VlibServerException(String msg, Throwable cause) {
        super(msg+(cause!=null?(": "+cause.getMessage()):""),cause);
    }

    /**
     * Constructs a ServerException with a formatted (parameterized) message and information derived from a {@link Throwable} object.
     * @param msg The message format string.
     * @param cause The {@link Throwable} from which additional message data will be drawn.
     * @param arguments Arguments for the {@code msg} format to use.
     */
    public VlibServerException(String msg, Throwable cause, Object ... arguments) {
        super(String.format(msg,arguments)+(cause!=null?": "+cause.getMessage():""),cause);
    }

    /**
     * Constructs a ServerException with a formatted (parameterized) message and information derived from a {@link VlibRepositoryResponse} object.
     * @param msg The message format string.
     * @param vlibRepositoryResponse The {@link VlibRepositoryResponse} from which additional message data will be drawn.
     * @param arguments Arguments for the {@code msg} format to use.
     */
    public VlibServerException(String msg, VlibRepositoryResponse vlibRepositoryResponse, Object ... arguments) {
        super(String.format(msg+(vlibRepositoryResponse ==null?"":(": "+ vlibRepositoryResponse.getResponseSummary())),arguments));
        this.vlibRepositoryResponse = vlibRepositoryResponse;
    }

    /**
     * Constructs a ServerException with a formatted (parameterized) message and information derived from a {@link Throwable} object
     * and from a {@link VlibRepositoryResponse} object.
     * @param msg The message format string.
     * @param cause The {@link Throwable} from which additional message data will be drawn.
     * @param vlibRepositoryResponse The {@link VlibRepositoryResponse} from which additional message data will be drawn.
     * @param arguments Arguments for the {@code msg} format to use.
     */
    public VlibServerException(String msg, Throwable cause, VlibRepositoryResponse vlibRepositoryResponse, Object ... arguments) {
        super(String.format(msg,arguments)+(cause!=null?": "+cause.getMessage():""),cause);
        this.vlibRepositoryResponse = vlibRepositoryResponse;
    }

    /**
     * Constructs a ServerException with a formatted (parameterized) message.
     * @param msg The message format string.
     * @param arguments Arguments for the {@code msg} format to use.
     */
    public VlibServerException(String msg, Object ... arguments) {
        super(String.format(msg,arguments));
    }

    /**
     * Returns the log information stored inside this exception.
     * @return
     */
    public String getLog() {

        if (log!=null)
            return log;
        else
            return("No log is available");
    }

    /**
     * Attaches a log string to this exception.
     * @param log
     */
    public void attacheLog(String log) {
        this.log=log;
    }

    /**
     * Sets a prefix that will be prepended to the exception's message
     * when returned by {@link #getMessage}.
     *
     * @param s The message to prepend what has already been collected.
     */
    public void prepend(String s) {
        prepend = s +": "+prepend;
    }

    /**
     * Returns the exception's message value, prepended with an additional string if one is set
     * using {@link #prepend}.
     */
    @Override
    public String getMessage() {
        //We'll put our message first
        StringBuffer sb= new StringBuffer(prepend);
        sb.append(super.getMessage());
        return sb.toString();
    }
    public static void throwNormalizedException(VlibServerLogger l, Exception e) throws VlibServerException {
        VlibServerException se;
        if (!(e instanceof VlibServerException)) {
            se = new VlibServerException("Unexpected exception",e);
        } else {
            se = (VlibServerException) e;
        }
        Exception tracer = new Exception();
        se.prepend(tracer.getStackTrace()[1].getMethodName());
        throw l.incorporateLog(se);
    }

    public String dump() {
        //Print the message
        StringBuffer sb = new StringBuffer(this.getMessage());
        if (vlibRepositoryResponse==null) {
            sb.append("Repository response: NONE\n");
        } else {
            sb.append(vlibRepositoryResponse.getResponseSummary());
        }
        //Print the stack trace
        sb.append(VlibServerJob.getStackTraceAsString(this));
        //Add the log
        sb.append("\n\n--LOG--\n\n").append(log==null?"":log);
        return sb.toString();
    }

}
