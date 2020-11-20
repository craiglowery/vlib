package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.jobmgr.ExecuteUponReturn;
import com.craiglowery.java.vlib.clients.core.Tag;
import com.craiglowery.java.vlib.clients.server.connector.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.apache.http.client.utils.URIBuilder;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class VlibServerJob_GetTagValues extends VlibServerJob<List<String>>{

    String tagName;

    public VlibServerJob_GetTagValues( String tagName) {
        super("Get tag values for "+tagName);
        this.tagName=tagName;
    }



    public List<String> computeDataResult() throws VlibServerException {
        List<String> values = null;
        try {
            tagName = (tagName == null) ? "" : tagName.trim();
            if (tagName.equals(""))
                throw new VlibServerException("A tag name was not specified");
            setServerJobStatus("Getting tag values for tag name '" + tagName + "'");
            values = new ArrayList<>();
            setServerJobStatus("Getting tag values for tag "+tagName+" from server");
            String baseUri =  "/tags/"+tagName;
            logger.log("Base URI=" + baseUri);
            VlibRepositoryResponse rr = getXmlResponse(HttpMethod.GET, baseUri, null);
            if (rr.xml == null)
                throw new VlibServerException("No xml was returned", rr);
            rr.enforce200(Util.xmlDocumentToString(rr.xml));
            setServerJobStatus("Parsing response");
            try {
                XPath xp = XPathFactory.newInstance().newXPath();
                NodeList nodes = (NodeList) xp.compile("/result/tag/value").evaluate(rr.xml, XPathConstants.NODESET);
                for (int x = 0; x < nodes.getLength(); x++) {
                    values.add(nodes.item(x).getTextContent());
                }
            } catch (XPathExpressionException e) {
                throw new VlibServerException("XPath error", e);
            }

        } catch (Exception e) {
           VlibServerException.throwNormalizedException(logger,e);
        }
        return values;

    }


}
