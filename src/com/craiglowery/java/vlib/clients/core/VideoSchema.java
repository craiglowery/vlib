package com.craiglowery.java.vlib.clients.core;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Craig on 2/25/2016.
 */
public class VideoSchema {
    public enum TYPE_KEY {STRING, INTEGER, DOUBLE, BOOLEAN, TIMESTAMP};
    Map<String,TYPE_KEY> typeMap = new HashMap<>();
    Map<String,Boolean> readonlyMap = new HashMap<>();

    /** Constructs a new schema object from an XML document describing it.
     *
     * @param doc The XML document describing the schema.  The document will be searched
     *   for a {@code <schema>} element, from which  {@code <attribute>} elements will be
     *   scanned.
     */
    public VideoSchema(Document doc) throws Exception {
        //Find the first schema object
        XPathFactory factory = XPathFactory.newInstance();
        XPath xp = factory.newXPath();
        Element schemaNode = (Element)(xp.compile("/result/schema").evaluate(doc, XPathConstants.NODE));
        if (schemaNode==null)
            throw new Exception("schema element not found in document");
        NodeList attributeNodes = (NodeList)(xp.compile("attribute").evaluate(schemaNode,XPathConstants.NODESET));
        if (attributeNodes==null || attributeNodes.getLength()==0)
            throw new Exception("no attributes found in schema");
        for (int x=0; x<attributeNodes.getLength(); x++) {
            Element attributeElement = (Element)(attributeNodes.item(x));
            String name = attributeElement.getAttribute("name");
            if (name==null)
                throw new Exception("missing attribute name on element #"+x);
            String typeString = attributeElement.getAttribute("type");
            if (typeString==null)
                throw new Exception("missing attribute type on element #"+x);
            TYPE_KEY type=null;
            switch (typeString) {
                case "string": type= TYPE_KEY.STRING; break;
                case "integer": type= TYPE_KEY.INTEGER; break;
                case "double": type= TYPE_KEY.DOUBLE; break;
                case "boolean": type= TYPE_KEY.BOOLEAN; break;
                case "timestamp": type= TYPE_KEY.TIMESTAMP; break;
            }
            if (type==null)
                throw new Exception("unknown attribute type '"+typeString+"' on element #"+x);
            String readonlyString = attributeElement.getAttribute("readonly");
            if (readonlyString==null || readonlyString.equals(""))
                readonlyString="true";
            Boolean readonly= com.craiglowery.java.common.Util.parseBoolean(readonlyString);
            if (readonly==null)
                throw new Exception("unparseable boolean value for readonly on element #"+x);
            addAttribute(name,type,readonly);
        }
    }

    /** Adds a new attribute name, type, readonly flag to the schema
     *
     * @param name The attribute to create.
     * @param type The TYPE_KEY of the attribute's type.
     * @param readonly {@code true} if the attribute is readonly.
     */
    public void addAttribute(String name, TYPE_KEY type, boolean readonly) {
        typeMap.put(name,type);
        readonlyMap.put(name,readonly);
    }

    /** Test for existence of an attribute name in the schema.
     *
     * @param name The attribute name to test.
     * @return True if the attribute name exists.
     */
    public boolean isAttribute(String name) {
        return typeMap.containsKey(name);
    }

    /** Determines the type key for an attributes type.
     *
     * @param name The attribute name of interest.
     * @return The {@code TYPE_KEY} of the attribute, or {@code null} if the
     * attribute name is {@code null} or not in the schema.
     */
    public TYPE_KEY getType(String name) {
        return name==null ? null : typeMap.get(name);
    }

    public Boolean isReadonly(String name) {
        return name==null ? true : readonlyMap.get(name);
    }

}
