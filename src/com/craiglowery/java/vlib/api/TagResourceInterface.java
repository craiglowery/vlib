package com.craiglowery.java.vlib.api;
import java.io.IOException;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import com.craiglowery.java.vlib.common.Util;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.repository.RepositoryManager;
import com.craiglowery.java.vlib.repository.Tag_valuesTuple;
import com.craiglowery.java.vlib.repository.TagsTuple;


/**
 * Implements the REST API: Tag Resource Interface.  The the design
 * document for details.
 *
 */

@Path("tags")
public class TagResourceInterface {
	
	private interface elTagMaker  {
		Element action(TagsTuple tt) throws U_Exception;
	}

	private Response tagGetter(String sexcludevalues, String tagname) {
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			if (sexcludevalues==null)
				sexcludevalues="false";
			final boolean excludevalues=
				Util.parseBoolean(sexcludevalues);  //Throws U_Exception.BadParameter
			Element elTags = response.doc.createElement("tags");
			
			elTagMaker maker = (tt) -> {
				Element elTag = response.doc.createElement("tag");
				elTag.setAttribute("description",Util.nullWrap(tt.description));
				elTag.setAttribute("type",Util.nullWrap(tt.type));
				elTag.setAttribute("name", tt.name);
				elTag.setAttribute("browsing_priority",Util.nullWrap(tt.browsing_priority));
				if (!excludevalues) {
					for (Tag_valuesTuple tvt : rm.getTagValues(tt.name)) {
						Element elValue = response.doc.createElement("value");
						elValue.appendChild(response.doc.createTextNode(tvt.value));
						elTag.appendChild(elValue);
					}
				}
				return elTag;
			};
			
			if (tagname!=null) {
				return response.Success(maker.action(rm.getTag(tagname)));
			} else {
				for (TagsTuple tt : rm.getTags()) {
						elTags.appendChild(maker.action(tt));
					}
				return response.Success(elTags);
			}
		} catch (U_Exception e) {
			if (e.errorCode==U_Exception.ERROR.BadParameter)
				return response.Failure(new AE(AE.ERR_BAD_PARAMETER,sexcludevalues,e),Status.BAD_REQUEST);
			if (e.errorCode==U_Exception.ERROR.NoSuchTagName)
				return response.Failure(new AE(AE.ERR_BAD_PARAMETER,tagname,e),Status.BAD_REQUEST);
			return response.Failure(e); 
		} catch (Exception e) {
			return response.Failure(e);
		}
	}
	
	@GET
	@Path("")
	@Produces("application/xml")
	public Response getTags(
		@QueryParam("excludevalues") @DefaultValue("false") String sexcludevalues) 
	{
		return tagGetter(sexcludevalues,null);
	}
	
	@GET
	@Path("{tagname}")
	@Produces("application/xml")
	public Response getTag(
			@PathParam("tagname") String tagname,
			@QueryParam("excludevalues") @DefaultValue("false") String sexcludevalues) 
		{
			return tagGetter(sexcludevalues,tagname);
		}
	
	@GET
	@Path("{tagname}/{value}")
	@Produces("application/xml")
	public Response getValue(
			@PathParam("tagname") String tagname,
			@PathParam("value") String value)
	{
		tagname = tagname.trim();
		value = value.trim();
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			if (rm.tagValueExists(tagname, value)) {
				Element elValue = response.doc.createElement("value");
				elValue.appendChild(response.doc.createTextNode(value));
				return response.Success(elValue);
			} 
			return response.Failure(new AE(AE.ERR_UNKNOWN_TAG_NAME_OR_VALUE,tagname+"="+value,null),Status.NOT_FOUND);
		}catch (Exception e) {
			return response.Failure(e);
		}
	}
	

	/**
	 * Create or update a tag and tag value definitions. The uploaded XML should be a 
	 * single {@code <tags/>} element with one or more subordinate 
	 * {@code <tag/>} elements.  For each {@code <tag/>}, if any 
	 * {@code <value/>} elements are included, then those values will also be created.<p>  
	 * @param input The incoming XML document, which should be of the form:<p>
	 * <pre>
	 *   {@code <tags>}
	 *      {@code <tag name="tagname" type="Category|Sequence|Entity" browsing_priority="int" description="desc">}
	 *         {@code <value>value</value>}
	 *         {@code ...}
	 *      {@code </tag>}
	 *      {@code ...}
	 *   {@code </tags>}
	 * </pre>
	 * @return An XML document of an {@code <success newtags="count" newvalues="count">} 
	 * element or
	 * an {@code <error/>} element.
	 */
	@PUT
	@Path("")
	@Consumes("application/xml")
	@Produces("application/xml")public Response putTags(
			InputStream input) 
	{
		XmlResponse response = new XmlResponse();

		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			//Validate incoming content
			//
			// 1. Is it XML that we can parse into a document?
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			dbf.setIgnoringComments(true);
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document docin = db.parse(input);
			// 2. Is it rooted with an <tags> element? 
			XPath xp = XPathFactory.newInstance().newXPath();
			if (xp.compile("/tags").evaluate(docin,XPathConstants.NODE)==null)
				throw new U_Exception(ERROR.ParserError,"missing tags root element");
			// Get all the tag subordinate elements
			NodeList tags = (NodeList)xp.compile("/tags/tag").evaluate(docin, XPathConstants.NODESET);
			try {
				rm.startTransaction();
				for (int x=0; x<tags.getLength(); x++) {
					Element elTag = (Element)(tags.item(x));
					// Should have a name attribute
					String name = elTag.getAttribute("name").trim();
					if (name.length()==0 || !Util.isPrintableCharacters(name))
						throw new U_Exception(ERROR.ParserError,"tag element missing valid name attribute",null);
					// Should have a type attribute
					String type = elTag.getAttribute("type");
					if (!type.equals("Category") && !type.equals("Entity") && !type.equals("Sequence"))
						throw new U_Exception(ERROR.ParserError,"tag element missing valid type attribute", null);
					// Description is optional
					String description = elTag.getAttribute("description");
					// Browsing priority defaults to 0
					String sbrowsing_priority = elTag.getAttribute("browsing_priority");
					int browsing_priority=0;
					if (sbrowsing_priority.length()>0)
						try {
							browsing_priority=Integer.parseInt(sbrowsing_priority);
						} catch (Exception e) {
							throw new U_Exception(ERROR.BadParameter,"browsing_priority",e);
						}
					//Add the tag if it doesn't already exist
					rm.createTag(name, type, description, browsing_priority);
					//If there are value elements, add them
					NodeList values = (NodeList)xp.compile("value").evaluate(elTag, XPathConstants.NODESET);
					for (int y=0; y<values.getLength(); y++) {
						Node nodeValue = values.item(y).getFirstChild();
						if (nodeValue==null || nodeValue.getNodeType()!=Node.TEXT_NODE)
							throw new U_Exception(ERROR.BadParameter,"value is null or not a text node");
						String value = nodeValue.getNodeValue().trim();
						if (value==null || value.length()==0 || !Util.isPrintableCharacters(value))
							throw new U_Exception(ERROR.BadParameter,"value is null or has invalid characters",null);
						rm.createTagValue(name, value);
					}
				}
				rm.commitTransaction();
				Element elSuccess = response.doc.createElement("success");
				return response.Success(elSuccess,Status.OK);
			} finally {
				if (rm.transactionInProgress()) {
					try {
						rm.rollbackTransaction();
					} catch (Exception e) {
						/* best effort */
					}
				}
			}
		} catch (U_Exception e) {
			if (e.errorCode==U_Exception.ERROR.ParserError)
				return response.Failure(new AE(AE.ERR_XML_PARSE_ERROR,"XML upload document cannot be parsed",e),Status.BAD_REQUEST);
			if (e.errorCode==U_Exception.ERROR.NoSuchTagName || e.errorCode==U_Exception.ERROR.BadParameter)
				return response.Failure(new AE(AE.ERR_BAD_PARAMETER,"",e),Status.BAD_REQUEST);
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(e);
		} 
	}

	/**
	 * Deletes tagging information.  All values and the tag name will be targeted for 
	 * deletion.  No changes are made if any of the values proposed for deletion are in 
	 * use in a tagging assignment to an object.  Either all deletions will be made 
	 * as described above, or nothing will be changed.
	 * @return An XML document of an {@code <success/>} 
	 * element or
	 * an {@code <error/>} element.
	 */
	@DELETE
	@Path("{name}")
	@Consumes("application/xml")
	@Produces("application/xml")
	public Response deleteTag(
			@PathParam("name") String name
			) 
	{
		XmlResponse response = new XmlResponse();
		name = name==null?"" : name.trim();
		if (name.equals(""))
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"name cannot be empty or null"),Status.BAD_REQUEST);
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
				if (!rm.tagExists(name))
					return response.Failure(AE.ae(AE.ERR_UNKNOWN_TAG_NAME_OR_VALUE,name),Status.NOT_FOUND);
				rm.deleteTag(name);
				Element elSuccess = response.doc.createElement("success");
				return response.Success(elSuccess);
		} catch (Exception e) {
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
		}
	}

	/**
	 * Deletes tagging information.  Only the specified name=value pairing will 
	 * be targeted for deletion. If this is the last value defined for the tag, 
	 * the tag will still exist even after the value has been deleted. No changes 
	 * are made if the value proposed for deletion is in use in a tagging assignment to an object.  
	 * @return An XML document of an {@code <success/>} 
	 * element or
	 * an {@code <error/>} element.
	 */
	@DELETE
	@Path("{name}/{value}")
	@Consumes("application/xml")
	@Produces("application/xml")
	public Response deleteValue(
			@PathParam("name") String name,
			@PathParam("value") String value
			) 
	{
		XmlResponse response = new XmlResponse();
		name = name==null?"" : name.trim();
		value = value==null?"" : value.trim();
		if (name.equals("") || value.equals(""))
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"neither name nor value can be empty or null"),Status.BAD_REQUEST);
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
				if (!rm.tagValueExists(name, value))
					return response.Failure(AE.ae(AE.ERR_UNKNOWN_TAG_NAME_OR_VALUE,name),Status.NOT_FOUND);
				rm.deleteTagValue(name, value);
				Element elSuccess = response.doc.createElement("success");
				return response.Success(elSuccess);
		} catch (Exception e) {
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
		}
	}

}
