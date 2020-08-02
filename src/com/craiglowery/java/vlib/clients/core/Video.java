package com.craiglowery.java.vlib.clients.core;

import com.craiglowery.java.common.Util;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;


/**
 * Created by Craig on 2/25/2016.
 */
public class Video extends Observable  {

    private  interface Job {
         String invoke();
    }

    private long handle;
    private String title, originalTitle;
    private Map<String,Object> attributes = new HashMap<>();
    private Set<NameValuePair> tags = new HashSet<>();
    private Set<NameValuePair> originalTags;
    private Map<String,String> tagsForDisplay = new HashMap<>();
    private VideoSchema schema;
    private TagDefinitions tagdefs;
    private static XPath xp = XPathFactory.newInstance().newXPath();
    private String stringRepresentation="noset";
    private static Stack<Job> undoStack = new Stack<>();


    private static XPathExpression xpeAttributes = null;

    /** Creates a new video object using the provided XML {@code <object>}
     * element and associating it with the provided schema.
     * @param elObject The XML element describing the video object. Should have both
     *                 {@code <attributes>} and {@code <tags>} elements.
     * @param schema The schema used to interpret the attributes.
     */
    public Video(Element elObject, VideoSchema schema, TagDefinitions tagdefs) throws Exception {
        this.schema=schema;
        this.tagdefs=tagdefs;
        if (xpeAttributes==null)
            xpeAttributes = xp.compile("attributes/*");
        NodeList attributeNodes = (NodeList)(xpeAttributes.evaluate(elObject,XPathConstants.NODESET));
        if (attributeNodes==null || attributeNodes.getLength()==0)
            throw new Exception("object has no attributes");
        int numAttributes=attributeNodes.getLength();
        for (int x=0; x<numAttributes; x++) {
            Element elAttribute = (Element)attributeNodes.item(x);
            String attribute = elAttribute.getNodeName();
            VideoSchema.TYPE_KEY type = schema.getType(attribute);
            if (type==null)
                throw new Exception("object attribute '"+attribute+"' is not in the schema");
            String value = elAttribute.getTextContent();
            switch (type) {
                case BOOLEAN:
                    Boolean b= Util.parseBoolean(value);
                    if (b==null)
                        throw new Exception("boolean value cannot be parsed from '"+value
                                +"' for attribute '"+attribute+"'");
                    attributes.put(attribute,b);
                    break;
                case DOUBLE:
                    try {
                        attributes.put(attribute, Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        throw new Exception("double value cannot be parsed from '"+value
                                +"' for attribute '"+attribute+"'");
                    }
                    break;
                case INTEGER:
                    try {
                        attributes.put(attribute, Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        throw new Exception("long value cannot be parsed from '"+value
                                +"' for attribute '"+attribute+"'");
                    }
                    break;
                case STRING :
                    attributes.put(attribute,value);
                    break;
                case TIMESTAMP :
                    try {
                        attributes.put(attribute, Instant.parse(value));
                    } catch (DateTimeParseException e) {
                        throw new Exception("timestamp value cannot be parsed from '"+value
                                +"' for attribute '"+attribute+"'");
                    }
                    break;
            }
        }

        if (!attributes.containsKey("handle") || !attributes.containsKey("title"))
            throw new Exception("object missing handle and/or title attributes, which are required");
        try {
            Object o = attributes.get("handle");
            if (! (o instanceof Long))
                throw new Exception("handle attribute must be an integer");
            handle = (Long)o;
            o = attributes.get("title");
            if (! (o instanceof String))
                throw new Exception("title attribute must be a String");
            title=(String)o;
        } catch (Exception e) {
            throw new Exception("unexpected exception during mapping of mandatory attributes",e);
        }

        Element elTags = (Element)(xp.compile("tags").evaluate(elObject,XPathConstants.NODE));
        if (elTags!=null) {
            NodeList tagsNodes =(NodeList)(xp.compile("*").evaluate(elTags,XPathConstants.NODESET));
            if (tagsNodes!=null) {
                int numTags = tagsNodes.getLength();
                for (int x=0; x<numTags; x++) {
                    Element elTag = (Element)tagsNodes.item(x);
                    String name = elTag.getAttribute("name");
                    if (name==null || name.length()==0)
                        throw new Exception("tag at position "+x+" has no name attribute");
                    String value = elTag.getAttribute("value");
                    if (value==null || value.length()==0)
                        continue;  //Ignore empty values
                    tags.add(new NameValuePair(name,value));
                }
            }
        }
        makeClean();
        computeTitleForDisplay();
        computeTagsForDisplay();
    }

    private void computeTitleForDisplay() {
        stringRepresentation = String.format("%6d - %s",handle,title);
    }

    private void computeTagsForDisplay() {
        tagsForDisplay.clear();
        for (NameValuePair nvp : tags) {
            String value = tagsForDisplay.get(nvp.name);
            tagsForDisplay.put(nvp.name,value==null?nvp.value:(value+"\n"+nvp.value));
        }
    }

    @Override
    public String toString() {
        return stringRepresentation;
    }

    /**
     * Sets a tag assignment for a video.
     * @param name The name of the tag to set.
     * @param value The value of the tag to set.
     * @return {@code true} if the assignment was not previously set.
     * @throws Exception If the assignment references an undefined name or value.
     */
    public boolean tag(String name, String value) throws Exception {
        //If it is a category tag, then enforce the value list
        if (tagdefs.tags.get(name).type== Tag.TagType.CATEGORY && !tagdefs.isTagValue(name,value))
            throw new Exception(String.format("Invalid tagging assignment: '%s=%s'",Util.nwp(name),Util.nwp(value)));
        NameValuePair nvp = new NameValuePair(name,value);
        if (tags.add(nvp)) {
            undoStack.push(
                    () ->
                    {
                        tags.remove(nvp);
                        reflectChanges();
                        return String.format("Tag %s=%s for handle %d",name,value,handle);
                    }
            );
            reflectChanges();
            return true;
        }
        return false;
    }

    private void reflectChanges() {
        computeTagsForDisplay();
        setChanged();
        notifyObservers();
    }

    /**
     * Removes a tag assignment for a video.
     * @param name The name of the tag to remove.
     * @param value The value of the tag to remove.
     * @return {@ocde true} if the assignment was previously set.  False if it was not set, or
     *    the name or value is not valid.
     */
    public boolean untag(String name, String value) {
        if (name==null || value==null)
            return false;
        NameValuePair nvp = new NameValuePair(name,value);
        if (tags.remove(nvp)) {
            undoStack.push( () -> {
                tags.add(nvp);
                reflectChanges();
                return String.format("Untag %s=%s for handle %d",name,value,handle);
            } );
            reflectChanges();
            return true;
        }
        return false;
    }

    /**
     * Removes all tag assignments for a specific tag name for a video.
     * @param name The tag name to remove.
     * @return {@code true} if there were any assignemnts for this tag name previously set.
     */
    public boolean untag(String name) {
        final boolean[] rval=new boolean[]{false};
        if (name!=null) {
            Stack<NameValuePair> undoGroup = new Stack<NameValuePair>();
            new LinkedList<>(tags).stream()
                    .forEach(

                            (nvp) -> {
                                if (nvp.name.equals(name)) {
                                    rval[0]=true;
                                    tags.remove(nvp);
                                    undoGroup.push(nvp);
                                }
                            }

                    );

            undoStack.push(

                    () -> {
                        for (NameValuePair nvp : undoGroup) {
                            tags.add(nvp);
                        }
                        reflectChanges();
                        return String.format("Untag all %s for handle %d",name,handle);
                    }

            );

            if (rval[0]) {
                reflectChanges();
            }
        }
        return rval[0];
    }

    /**
     * Removes all tag assignments for a video.
     * @return True if there were any tag assignments previously set.
     */
    public boolean untag() {
        boolean rval = tags.size()>0;
        if (rval) {
            HashSet<NameValuePair> undoGroup = new HashSet<>(tags);  //Make a clone
            tags.clear();
            undoStack.push(
                    () ->  {
                        tags = undoGroup;
                        reflectChanges();
                        return String.format("Remove all tags for handle %d",handle);
                    }
            );
            reflectChanges();
        }
        return rval;
    }

    public int cardinality(String tagName) {
        return (int)tags.stream().filter(

                (nvp) -> {return nvp.name.equals(tagName);}

                ).count();
    }

    public boolean isSingleValued(String tagName) {
        return cardinality(tagName)==1;
    }

    public boolean isMultiValued(String tagName) {
        return cardinality(tagName)>1;
    }

    public boolean setTitle(String t) {
        t=t.trim();
        if (!title.equals(t)) {
            String oldTitle = title;
            undoStack.push(
                    () -> {
                        String newTitle=title;
                        title = oldTitle;
                        reflectChanges();
                        return String.format("Change title to '%s' for handle %d",newTitle,handle);
                    }
            );
            title=t;
            reflectChanges();
            return true;
        }
        return false;
    }

    /**
     * Determines if tag assignemnts have changed since the video object was loaded from the server.
     * @return {@code true} if there are differences.
     */
    public boolean hasDirtyTags() {
        return !(tags.equals(originalTags));
    }

    /** Determines if any attributes have been modified since construction. **/
    public boolean hasDirtyAttributes() {
        return !title.equals(originalTitle);
    }

    /** Determines if any changs have been made since constructionl **/
    public boolean isDirty() {
        return hasDirtyTags() || hasDirtyAttributes();
    }

    /** Returns the handle **/
    public long getHandle() {
        return handle;
    }

    /** Returns the title **/
    public String getTitle() {
        return title;
    }

    /** Returns the set of name value pairs assigned to this video. **/
    public Set<NameValuePair> getTags() {
        return tags;
    }

    /** Returns a potentially multi-line string of a tags values
     *
     * @param tagName The tag name of interest.
     * @return A string with each tag value separated by a newline character.
     */
    public String getTagValueString(String tagName) {
        return tagsForDisplay.get(tagName);
    }

    /**
     * Caches current values of editable attributes and tag assignments as "original" values,
     * effectively making this video clean of any changes.
     */
    public void makeClean() {
        originalTags = new HashSet<>(tags);
        originalTitle=title;
        setChanged();
        notifyObservers();
    }

    public void revert() {
        HashSet<NameValuePair> oldTags = new HashSet<>(tags);
        String oldTitle = title;
        undoStack.push(
                () -> {
                    tags=oldTags; title=oldTitle;
                    reflectChanges();
                    return String.format("Revert changes for handle %d",handle);
                }
        );
        tags = new HashSet<>(originalTags);
        title=originalTitle;
        reflectChanges();
    }

    /**
     * Pop the undo stack and execute the task, returning the string message it
     * produces.
     *
     * @return
     */
    static public String undo() {
        if (undoStack.size()==0)
            return "Nothing to undo";
        Job j = undoStack.pop();
        String message= "UNDO: "+j.invoke();
        return message;
    }

    /**
     * Clears the undo stack to empty.
     */
    static public void clearUndo() {
        undoStack.clear();
    }

}
