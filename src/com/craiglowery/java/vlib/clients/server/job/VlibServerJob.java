package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.jobmgr.ExecuteUponReturn;
import com.craiglowery.java.jobmgr.Job;
import org.apache.http.Header;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

public abstract class VlibServerJob<T_DATA> extends Job<VlibServerJobResult,VlibServerJobStatus> implements Closeable
{
    //----STATIC--------------------------------------------------------------

    public static VlibServer getServer() {
        return server;
    }

    public static void setServer(VlibServer server) {
        VlibServerJob.server = server;
    }

    private static VlibServer server=null;


    //----CONSTRUCTOR---------------------------------------------------------
    /**
     * Constructs a new job.  The client should use these constructors of a concrete implementation
     * of a descendent class when submitting a job to the job manager.
     *
     * @param description A string description of the job.
     */
    public VlibServerJob(String description, ExecuteUponReturn returnCode) {
        super(description,returnCode);
        initialize();
    }

    public VlibServerJob(String description)
    {
        super(description);
        initialize();
    }

    private void initialize()
    {
        if (server==null) {
            throw new Error("No server has been set in the VlibServerJob class");
        }
        documentBuilderFactory=DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        logger = new VlibServerLogger(this);
        //logger.setPrintStream(System.out);
    }

    /** The accumulating log entries for this job. See {@link VlibServerLogger} **/
    VlibServerLogger logger;

    /** For building XML documents **/
    DocumentBuilderFactory documentBuilderFactory =null;

    //----SERVER JOB STATUS---------------------------------------------

    public String getServerJobStatus() {
        return serverJobStatus;
    }

    /**
     * Sets the server job status (the status of the interaction with the vlib server)
     * and also logs the change.
     * @param serverJobStatus
     */
    public void setServerJobStatus(String serverJobStatus) {
        logger.log("ServerJob Status Change: "+serverJobStatus);
        this.serverJobStatus = serverJobStatus;
    }

    public void setServerJobStatus(String format, Object ... args) {
        setServerJobStatus(String.format(format,args));
    }

    private String serverJobStatus = "";

    //----ABSTRACTS---------------------------------------------------------------------------

    T_DATA dataResult;
    protected abstract T_DATA computeDataResult() throws VlibServerException;
    public T_DATA getDataResult()
    {
        return dataResult;
    }

    /**
     * Performs the work in connecting with the vlib server.  Implementations of this
     * method can write logging trails to the {@link #logger} object.
     * @return
     * @throws Exception
     */
    protected VlibServerJobResult performServerWork() throws Exception {
        dataResult = computeDataResult();
        return new VlibServerJobResult(dataResult);
    }


    //----OVERRIDES---------------------------------------------------------------------------

    /**
     * Called by the job to perform work on a separate thread, this concrete implementation
     * wraps the actual server work with logging capabilities and exception handling.
     * Subclasses of {@link VlibServerJob} must implement {@link #performServerWork} to do
     * the actual communication with the server and return a result.  The log will then be added
     * to the result and returned.
     *
     * @return
     */
    @Override
    public VlibServerJobResult worker() throws Exception {
        VlibServerJobResult result=null;
        try {
            result = performServerWork();
            result.setLog(logger.toString());
        } catch (VlibServerException e) {
            throw e;
        } catch (Exception e) {
             VlibServerException.throwNormalizedException(logger,e);
        }
        return result;
    }

    //----HTTP CONNECTION STUFF------------------------------------------------------------

    /** A private HTTP client to be used by this job **/
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    /** Used to identify the type of request **/
    public enum HttpMethod {GET, PUT, POST, DELETE};

    /**
     * Executes an HTTP request with optional XML content, returning an object
     * holding any returned XML document, the response status and headers.  The method
     * composes the appropriate request, according to the method specified, then
     * calls {@link #executeRequest}.
     *
     * @param method  One of GET, PUT, POST, or DELETE
     * @param uriSuffix The resource to target, which will be appended to the base URI defined when
     *                  this object was constructed.
     * @param payload Optional XML payload to send.
     * @return The salient response components.
     * @throws Exception
     */
    public VlibRepositoryResponse getXmlResponse(HttpMethod method, String uriSuffix, Document payload) throws VlibServerException {
        HttpUriRequest request = null;
        String uri = server.getRootURI()+uriSuffix;
        switch (method) {
            case DELETE:
                request = new HttpDelete(uri);
                break;
            case GET:
                request = new HttpGet(uri);
                break;
            case PUT:
                HttpPut put = new HttpPut(uri);
                put.setEntity(new InputStreamEntity(createInputStream(payload), ContentType.APPLICATION_XML));
                request = put;
                break;
            case POST:
                HttpPost post = new HttpPost(uri);
                post.setEntity(new InputStreamEntity(createInputStream(payload), ContentType.APPLICATION_XML));
                request = post;
                break;
        }
        return executeRequest(request);
    }

    /**
     * Special purpose requests used to put a block of bytes as part of a file upload.
     * @param uriSuffix The resource to target, which will be appended to the base URI defined when
     *                  this object was constructed.
     * @param buf The buffer of bytes to send.
     * @param count The number of bytes to send.
     * @return The salient response components.
     * @throws Exception
     */
    VlibRepositoryResponse putChunk(String uriSuffix, byte[] buf, int count) throws Exception {
        HttpPut put = new HttpPut(server.getRootURI()+uriSuffix);
        try (InputStream is = new ByteArrayInputStream(buf, 0, count)) {
            put.setEntity(new ByteArrayEntity(buf, 0, count, ContentType.APPLICATION_OCTET_STREAM));
            return executeRequest(put);
        } catch (IOException e) {
            throw new Exception("IOerror while putting chunk", e);
        }
    }

    /**
     * Executes a request using the existing http client, which is unique to this job.
     * @param request
     * @return
     * @throws VlibServerException
     */
    VlibRepositoryResponse executeRequest(HttpUriRequest request) throws VlibServerException {

        request.addHeader("Authorization",(server.getAuthheader()));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            //Let's get any XML there might be, and the statusCode
            Header[] contentTypeHeaders = response.getHeaders("Content-Type");
            Document doc = null;
            if (contentTypeHeaders!=null && contentTypeHeaders.length>0 && contentTypeHeaders[0].getValue().startsWith("application/xml")) {
                try {
                    doc = getDocumentBuilder().parse(response.getEntity().getContent());
                } catch (Exception e) {
                    throw new VlibServerException("An error occurred while reading/parsing XML content from the server.", e);
                }
            }
            return new VlibRepositoryResponse(doc, response.getStatusLine(), response.getAllHeaders());
        } catch (IOException e) {
            throw new VlibServerException("Error closing HTTP response object", e);
        }

    }

    /**
     * A helper method for {@link #getXmlResponse} that creates an input stream from an XML document, usually
     * to provide it as an upload source in PUT or POST.
     * @param doc The source document to connect to the input stream.
     * @return The input stream connected to the XML document.
     * @throws VlibServerException
     */
    public InputStream createInputStream(Document doc) throws VlibServerException
    {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Source xmlSource = new DOMSource(doc);
            Result outputTarget = new StreamResult(outputStream);
            TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (Exception e) {
            throw new VlibServerException("Could not create stream for outbound XML document", e);
        }
    }

    DocumentBuilder getDocumentBuilder() throws Exception
    {
            return documentBuilderFactory.newDocumentBuilder();
    }

    @Override
    public void close()
    {
        try {
            httpClient.close();
        } catch (Exception e) { /* best effort */ }
    }

    public String getLog()
    {
        return logger.toString();
    }

    //----STATICS--------------------------------------------------------------------------

    /**
     * Returns a string in which an XML document has been formatted with indentation.
     * @param xml The XML document to be formatted.
     * @return A string with the formatted XML.
     * @throws Exception
     */
    public static String prettyXml(Document xml) throws Exception {
        try (Writer out = new StringWriter()) {
            Transformer tf = TransformerFactory.newInstance().newTransformer();
            tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            tf.setOutputProperty(OutputKeys.INDENT, "yes");
            tf.transform(new DOMSource(xml), new StreamResult(out));
            return out.toString();
        } catch (Exception e) {
            throw new Exception("error while pretty-printing XML document", e);
        }
    }

    /**
     * Returns a string representation of the {@link Throwable}'s stack strack.
     * @param t
     * @return
     */
    public static String getStackTraceAsString(Throwable t) {
        try (   StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw) ) {
            return sw.toString();
        } catch (Exception e) {
            return String.format("%s: %s",e.getClass().getName(),e.getMessage());
        }
    }



}
