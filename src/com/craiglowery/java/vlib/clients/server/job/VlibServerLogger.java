package com.craiglowery.java.vlib.clients.server.job;


import org.w3c.dom.Document;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * <p>Object that collects log entries for a {@link VlibServerJob}.  The job calls the logger's
 * methods to append log lines.  The log is collected and can be returned as a string at any
 * time. {@link VlibServerJob}s get the logger's string at the end of the
 * {@link VlibServerJob#worker} method, and placing it in the {@link VlibServerJobResult} object
 * that is returned to the calling client.  The log can optionally be echoed to a print stream
 * in real time (as items are logged).</p>
 *
 * <ul>
 *     <li>{@link #setPrintStream} sets a print stream to echo the log to.</li>
 *     <li>{@link #log} methods provide different ways to log entries,
 *        from simple message strings to formatted strings and stack traces of
 *        exceptions.
 *     <li>{@link #incorporateLog(VlibServerException)} Takes an {@link VlibServerException}
 *      and embeds the current log result as a string in the exception's log attribute.</li>
 *     <li>{@link #toString}</li> returns the log's current contents.
 * </ul>
 */
public class VlibServerLogger {


    StringBuilder sb = new StringBuilder();
    VlibServerJob job=null;
    private PrintStream printStream = null;

    /**
     * Constructs a new logger and associates with the specified job.
     * @param job
     */
    public VlibServerLogger(VlibServerJob job) {
        this.job=job;
    }

    /**
     * ALlows logging to also be directed to a print stream.  Logging to a print stream
     * is turned off by default.
     * @param ps The print stream copy log entries to, or {@code null} to turn off the print stream function.
     */
    public void setPrintStream(PrintStream ps){
        printStream=ps;
    }

     public void log(String msg) {
        StringBuffer msb = new StringBuffer("#").append(job.getId()).append(" ").append(job.getDescription())
                .append(": ").append(msg).append("\n");
        sb.append(msb);
        if (printStream!=null) {
            printStream.print(msb);
            printStream.flush();
        }
    }

    /**
     * Logs a message with format arguments.
     * @param fmt
     * @param args
     */
    public void log(String fmt, Object... args) {
        log(String.format(fmt,args));
    }

    /**
     * Dumps an XML document to the log.
     * @param xml
     */
    public void log(Document xml) {
        if (xml==null)
            log("--NULL xml document--");
        else
        try {
            log(job.prettyXml(xml));
        } catch (Exception e) {
            log("--could not append XML document: "+e.getMessage()+"--");
        }
    }

    /**
     * Logs an exception by outputting it's stack trace.
     * @param e
     */
    public void log(Exception e) {
        try (   StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw) ) {
            e.printStackTrace(pw);
            log(sw.toString());
        } catch (Exception ex) {
            //Best effort
        }
    }

    /**
     * Logs a repository response summary
     * @param rr
     */
    public void log(VlibRepositoryResponse rr) {
        if (rr==null)
            log("--NULL repository response--\n");
        else
            log(rr.getResponseSummary()+"\n");
    }

    /**
     * Logs a {@link VlibServerException}, attaches the log output to it,  and returns it.
     * @param e
     * @throws VlibServerException
     */
    public VlibServerException incorporateLog(VlibServerException e)  {
        log(e);                            //Add the exception to the log
        e.attacheLog(this.toString());     //Attach the log to the exception
        job.setServerJobStatus("EXCEPTION: "+e.getMessage()); //Set the job status
        return e;
    }

    /**
     * Returns the contents of the log as a string.
     *
     * @return
     */
    @Override
    public String toString() {
        return sb.toString();
    }

}
