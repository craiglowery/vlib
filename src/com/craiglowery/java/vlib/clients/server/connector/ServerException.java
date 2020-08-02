package com.craiglowery.java.vlib.clients.server.connector;

/**
 * Created by Craig on 2/2/2016.
 */
public class ServerException extends Exception {

    private RepositoryResponse repositoryResponse=null;
    private String log=null;
    private String prepend="";

    public RepositoryResponse getRepositoryResponse() {
        return repositoryResponse;
    }

    public ServerException(String msg) {
        super(msg);
    }

    public void prepend(String s) {
        prepend = s +": "+prepend;
    }

    public ServerException(String msg, RepositoryResponse repositoryResponse) {
        super(msg+(repositoryResponse==null?"":(": "+repositoryResponse.getResponseSummary())));
        this.repositoryResponse=repositoryResponse;
    }

    public ServerException(String msg, Throwable cause) {
        super(msg+(cause!=null?(": "+cause.getMessage()):""),cause);
    }

    public ServerException(String msg, Throwable cause, Object ... arguments) {
        super(String.format(msg,arguments)+(cause!=null?": "+cause.getMessage():""),cause);
    }

    public ServerException(String msg, RepositoryResponse repositoryResponse, Object ... arguments) {
        super(String.format(msg+(repositoryResponse==null?"":(": "+repositoryResponse.getResponseSummary())),arguments));
        this.repositoryResponse=repositoryResponse;
    }

    public ServerException(String msg, Throwable cause, RepositoryResponse repositoryResponse, Object ... arguments) {
        super(String.format(msg,arguments)+(cause!=null?": "+cause.getMessage():""),cause);
        this.repositoryResponse=repositoryResponse;
    }

    public ServerException(String msg, Object ... arguments) {
        super(String.format(msg,arguments));
    }

    public String getLog() {

        if (log!=null)
            return log;
        else
            return("No log is available");
    }

    public void attacheLog(String log) {
        this.log=log;
    }

    @Override
    public String getMessage() {
        StringBuffer sb= new StringBuffer(prepend);
        sb.append(super.getMessage());
        return sb.toString();
    }

}
