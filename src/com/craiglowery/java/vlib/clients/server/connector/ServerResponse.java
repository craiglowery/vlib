package com.craiglowery.java.vlib.clients.server.connector;

/**
 * Created by Craig on 2/4/2016.
 */
public class ServerResponse<T> {
    private T result=null;
    private String log=null;

    public ServerResponse(T result, String log) {
        this.result=result;
        this.log=log;
    }

    public T getResult() {
        return result;
    }

    public String getLog() {
        return log==null?"":log;
    }

}
