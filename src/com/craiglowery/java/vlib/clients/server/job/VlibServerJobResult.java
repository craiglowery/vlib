package com.craiglowery.java.vlib.clients.server.job;

/**
 * Wraps an untyped object which is the actual data returned from the server.
 */
public class VlibServerJobResult {
    Object serverInteractionResult;

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    String resultType;

    public Object getServerInteractionResult() {
        return serverInteractionResult;
    }

    public void setServerInteractionResult(Object result) {
        this.serverInteractionResult = result;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    String log="EMPTY";

    public VlibServerJobResult(Object result) {
        this.serverInteractionResult =result;
        resultType = result.getClass().getSimpleName();
    }



}
