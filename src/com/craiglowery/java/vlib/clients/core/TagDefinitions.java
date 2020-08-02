package com.craiglowery.java.vlib.clients.core;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Craig on 3/2/2016.
 */
public class TagDefinitions {

    Map<String,Tag> tags = new HashMap<String,Tag>();

    /** Creates an empty tag definitions object **/
    public TagDefinitions() {
        super();
    }

    /** Creates a tag definitions object from an XML document.  The document
     * must have a {@code <tags>} element that is structured like so:<p>
     * <pre>
     *     {@code <tags>}
     *       {@code <tag name="name" type="type" description="description" browsing_priority="priority">}
     *          {@code <value>value</value>}
     *          {@code ...}
     *       {@code </tag>}
     *     {@code </tags>}
     * </pre>
     * @param document  XML document to scan.
     * @throws Exception if there is a problem with the document, or the XPATH expression.
     */
    public TagDefinitions(Document document) throws Exception {
        super();
        try {
            XPath xp = XPathFactory.newInstance().newXPath();
            Element elTags = (Element)xp.compile("/result/tags").evaluate(document, XPathConstants.NODE);
            if (elTags==null)
                throw new Exception("Could not load tag definitions - no tags element in document");
            NodeList tagNodes = (NodeList)xp.compile("tag").evaluate(elTags, XPathConstants.NODESET);
            if (tagNodes==null || tagNodes.getLength()==0)
                return;
            int numTags=tagNodes.getLength();
            for (int x=0; x<numTags; x++) {
                Element elTag = (Element)tagNodes.item(x);
                String name = elTag.getAttribute("name");
                if (name==null || name.length()==0)
                    throw new Exception(String.format("Could not load tag definition for tag #%d - no name",x));
                if (isTagName(name))
                    throw new Exception("Could not load tag definition for duplicate tag '"+name+"'");
                String type = elTag.getAttribute("type");
                if (type==null || type.length()==0)
                    throw new Exception(String.format("Could not load tag definition for tag #%d - no type",x));
                String description = elTag.getAttribute("description");
                String browsing_priority = elTag.getAttribute("browsing_priority");
                Tag tag = new Tag(name,description,type,browsing_priority);
                addTag(tag);
                NodeList valueNodes = (NodeList)xp.compile("value").evaluate(elTag,XPathConstants.NODESET);
                if (valueNodes!=null) {
                    int numValues = valueNodes.getLength();
                    for (int y=0; y<numValues; y++) {
                        addTagValue(name,((Element)valueNodes.item(y)).getTextContent());
                    }
                }
            }
        } catch (XPathExpressionException e) {
            throw new Exception("Could not load tag definitions - XPath expression error",e);
        }
    }

    /** Determines if a tag name has been defined.
     *
     * @param name  The tag name in question.
     * @return {@code true} if the tag has been defined.
     */
    public boolean isTagName(String name) {
        return tags.containsKey(name);
    }

    /** Determines if a particular value has been defined for a tag.
     *
     * @param name The tag name in question.
     * @param value The value in question.
     * @return {@code true} if the tag name is defined, and value is also defined as one of its possible values.
     */
    public boolean isTagValue(String name, String value) {
        Tag tag = tags.get(name);
        if (tag!=null) {
            return tag.getValues().contains(value);
        }
        return false;
    }

    /**
     * Returns a collection of defined tag names.
     * @return Colleciton of tag names.
     */
    public Collection<String> getTagNames() {
        return tags.keySet();
    }

    /**
     * Retrieves a {@code Tag} object by name.
     * @param name The name of the tag of interest.
     * @return The {@code Tag} object, or null if there is no such tag object by that name.
     */
    public Tag getTag(String name) {
        return tags.get(name);
    }

    /**
     * Returns a collection of values defined for a tag.
     * @param name The tag name in question.
     * @return A collection of values for the named tag, or null if the tag does not exist.
     */
    public Collection<String> getTagValues(String name) {
        Tag tag = tags.get(name);
        if (tag!=null) {
            return tag.getValues();
        }
        return null;
    }

    /**
     * Adds a tag object to the collection, overwriting the previous one with this name if
     * one exists.
     * @param tag The {@code Tag} object to put into the collection.
     */
    public void addTag(Tag tag) {
        tags.put(tag.name,tag);
    }

    /**
     * Adds a value to the list of defined values for a tag.  The request is ignored if there is no
     * such tag by that name.
     * @param name The name of the tag.
     * @param value The value.
     */
    public void addTagValue(String name, String value) {
        Tag tag = getTag(name);
        if (tag!=null)
            tag.addValue(value);
    }
}
