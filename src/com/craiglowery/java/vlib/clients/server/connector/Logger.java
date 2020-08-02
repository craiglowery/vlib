package com.craiglowery.java.vlib.clients.server.connector;

import org.w3c.dom.Document;

import java.io.PrintStream;

/**
 * Created by Craig on 2/2/2016.
 */
public class Logger {

    StringBuilder sb = new StringBuilder();
    ServerConnectorTask sct=null;
    private PrintStream printStream = null;


    public Logger(PrintStream logToStream) {
        super();
        printStream = logToStream;
    }

    public Logger(boolean logToStdErr) {
        super();
        printStream = System.err;
    }

    public Logger() {
        super();
    }

    public Logger(ServerConnectorTask t) {
        sct=t;
    }

    public void setPrintSream(PrintStream ps){
        printStream=ps;
    }

    public void status(String msg) {
        log(msg);
        sct.status(msg);
    }

    public void status(String msg, Object... args) {
        String s = String.format(msg,args);
        log(s);
        sct.status(s);
    }

    public void log(String msg) {
        sb.append(msg).append("\n");
        if (printStream!=null)
            printStream.println(msg);
    }

    public void log(String fmt, Object... args) {
        String msg = String.format(fmt,args);
        sb.append(msg).append("\n");
        if (printStream!=null)
            printStream.println(msg);
    }

    public void log(Document xml) {
        if (xml==null)
            log("--NULL xml document--");
        else
        try {
            log(ServerConnector.prettyXml(xml));
        } catch (Exception e) {
            log("--could not append XML document: "+e.getMessage()+"--");
        }
    }

    /**
     * Logs an exception.
     * @param e
     */
    public void log(Exception e) {
        Throwable t = e;
        int indent=0;
        StringBuffer sb=new StringBuffer();
        while (t!=null) {
            sb.append(" ");
            log(sb.toString()+"=> "+t.getMessage());
            t=t.getCause();
        }
/*        try (StringWriter sw = new StringWriter();
             PrintWriter pw = new PrintWriter(sw)) {
            e.printStackTrace(pw);
            log(sw.toString());
        } catch (IOException ioe) {
        }
        */
    }

    public void log(RepositoryResponse rr) {
        if (rr==null)
            sb.append("--NULL repository response--\n");
        else
            sb.append(rr.getResponseSummary()).append("\n");
    }

    public void logAndThrow(ServerException e) throws ServerException {
        log(e);
        e.attacheLog(this.toString());
        sct.status("EXCEPTION: "+e.getMessage());
        throw e;
    }

    @Override
    public String toString() {
        return sb.toString();
    }

}
