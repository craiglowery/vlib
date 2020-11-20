package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.jobmgr.ExecuteUponReturn;
import com.craiglowery.java.vlib.clients.core.Tag;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class VlibServerJob_GetTagNames extends VlibServerJob<List<Tag>>{

    public VlibServerJob_GetTagNames() {
        super("Get tag names");
    }


    public List<Tag> computeDataResult() throws VlibServerException {
        try {
            XPath xp = XPathFactory.newInstance().newXPath();
            setServerJobStatus("Getting tag names from server");
            String baseUri =  "/tags?excludevalues=true";
            logger.log("Base URI=" + baseUri);
            List<Tag> tags = new ArrayList<>();
            VlibRepositoryResponse rr = getXmlResponse(VlibServerJob.HttpMethod.GET, baseUri, null);
            if (rr.xml == null)
                throw new VlibServerException("No xml was returned", rr);
            rr.enforce200(Util.xmlDocumentToString(rr.xml));
            setServerJobStatus("Parsing response");
            try {
                NodeList nodes = (NodeList) (xp.compile("/result/tags/tag")).evaluate(rr.xml, XPathConstants.NODESET);
                XPathExpression expName = xp.compile("@name");
                XPathExpression expType = xp.compile("@type");
                XPathExpression expDescription = xp.compile("@description");
                XPathExpression expBrowsingPriority = xp.compile("@browsing_priority");
                for (int x = 0; x < nodes.getLength(); x++) {
                    Node node = nodes.item(x);
                    String name = (String) expName.evaluate(node, XPathConstants.STRING);
                    String type = (String) expType.evaluate(node, XPathConstants.STRING);
                    String description = (String) expDescription.evaluate(node, XPathConstants.STRING);
                    String browsingPriority = (String) expBrowsingPriority.evaluate(node, XPathConstants.STRING);
                    tags.add(new Tag(name, description, type, browsingPriority));
                }
            } catch (XPathExpressionException e) {
                throw new VlibServerException("XPath error", e);
            }

            setServerJobStatus("Returning");
            return tags;

        } catch (Exception e) {
           VlibServerException.throwNormalizedException(logger,e);
        }
        return null; //never is reached
    }


}
