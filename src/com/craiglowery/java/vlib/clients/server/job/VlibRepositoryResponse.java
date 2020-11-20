package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.common.Util;
import org.apache.http.Header;
import org.apache.http.StatusLine;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Created by Craig on 2/2/2016.
 */
public class VlibRepositoryResponse {
    public Document xml;
    public StatusLine statusLine;
    public Header[] headers;

    public VlibRepositoryResponse(Document xml, StatusLine statusLine, Header[] headers) {
        this.xml = xml;
        this.statusLine = statusLine;
        this.headers = headers;
    }

    /**
     * Returns a concise summary of the server response with HTTP code and phrase,
     * the root tag of the xml (if any), and the error code and description if present.
     * @return
     */
    public String getResponseSummary() {
        StringBuilder sb =
            new StringBuilder(
                String.format("HTTP %s %d (%s)",
                        statusLine.getProtocolVersion().toString(),
                        statusLine.getStatusCode(),
                        statusLine.getReasonPhrase()));
        if (xml==null) {
            sb.append(" -no content");
        } else {
            try {
                XPath xp = XPathFactory.newInstance().newXPath();
                if ((xp.compile("/result").evaluate(xml, XPathConstants.NODE)!=null)) {
                    Node tln = (Node)xp.compile("result").evaluate(xml,XPathConstants.NODE);
                    if (tln==null) throw new XPathExpressionException("No top level node");
                    sb.append(" <"+tln.getNodeName()+">");
                } else if ((xp.compile("/error").evaluate(xml, XPathConstants.NODE)!=null)) {
                    String code = (String)xp.compile("/error/code/text()").evaluate(xml,XPathConstants.STRING);
                    String description = (String)xp.compile("/error/description/text()").evaluate(xml,XPathConstants.STRING);
                    if (code==null || code.equals("")) code="unknown";
                    if (description==null || description.equals("")) description="unknown";
                    sb.append(String.format(" ERROR #%s: %s",code,description));
                } else {
                    sb.append(" -unknown root element");
                }
            } catch (XPathExpressionException e) {
                sb.append(" -unparseable content");
            }
        }
        return sb.toString();
    }

    public StatusLine getStatusLine() {
        return statusLine;
    }

    public int getStatusCode() {
        return statusLine.getStatusCode();
    }

    /**
     * Checks the status of the repository response to see if it is in the range specified. If it is NOT in the
     * range, then a ServerException is thrown with the messages and arguments provided.
     * @param low The low end of the range (inclusive) of acceptable response status codes.
     * @param high The highend of the range (inclusive) of acceptable response stauts codes.
     * @param msg The String.format format argument.
     * @param args Arguments to supply to String.format
     */
    public void checkStatus(int low, int high, String msg, Object ... args) throws VlibServerException {
        int statusCode=getStatusCode();
        if (statusCode>=low && statusCode<=high)
            return;
        throw new VlibServerException(msg,this,args);
    }


    /**
     * Checks that the status code is 200. If not, it throws a ServerException.
     */
    public void enforce200(String msg) throws VlibServerException {
        int statusCode=getStatusCode();
        if (statusCode!=200)
            throw new VlibServerException("HTTP Status 200 expected - %d returned : %s",this,statusCode, msg);
    }

    public void enforce200(Document xml) throws VlibServerException {
        try {
            enforce200(Util.xmlDocumentToString(xml));
        } catch (Exception e) {
            throw new VlibServerException("Unable to transform XML - " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(statusLine.toString()).append("\n");
        for (Header h : headers) {
            sb.append(h.getName())
                    .append(": ")
                    .append(h.getValue())
                    .append("\n");
        }
        if (xml != null)
            try {
                sb.append(VlibServerJob.prettyXml(xml)).append("\n");
            } catch (Exception e) {
                sb.append("Error formatting XML:\n" + e.getMessage());
            }
        return sb.toString();
    }
}
