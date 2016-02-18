package com.craiglowery.java.vlib.api;

/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.Util;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.repository.ObjectsTuple;
import com.craiglowery.java.vlib.repository.RepositoryManager;
import com.craiglowery.java.vlib.repository.VersionsTuple;
import com.craiglowery.java.vlib.repository.Object_tagsTuple;
import com.craiglowery.java.vlib.tuple.SelectionTransformer;

/**
 * Implements the REST API: Object Resource Interface.  The the design
 * document for details.
 *
 */
@Path("objects")
public class ObjectResourceInterface {
	
	@Context
	/** Injects information about the actual URI used to access this service **/
	public static UriInfo uriInfo;
	

	/**
	 * Utility function to generate XML that describes an object, all of its
	 * versions and tags into an {@code <object>} element.<p>
	 * @param handle Handle of the object to describe.
	 * @param rm A repository manager object to answer our queries to the DB.
	 * @param doc The XML document which will own the XML elements.
	 * @return An XML {@code <object/>} element.
	 * @throws U_Exception
	 */
	private Element describeObjectIntoXml(int handle, RepositoryManager rm, Document doc) 
		throws U_Exception
	{
		Element elObject = doc.createElement("object");
		elObject.setAttribute("handle", Integer.toString(handle));
		
		Element elVersions = doc.createElement("versions");
		boolean current=true;
		for (VersionsTuple vt : rm.getVersions(handle)) {
			Element elVersion = doc.createElement("version");
			elVersion.setAttribute("versioncount", Integer.toString(vt.versioncount));
			elVersion.setAttribute("current",current?"true":"false");
			current=false;
			rm.describeObjectIntoXmlElement(vt, elVersion, uriInfo.getAbsolutePath().toString());
			elVersions.appendChild(elVersion);
		}
		elObject.appendChild(elVersions);
		
		Element elTags = doc.createElement("tags");
		String lastTagName = null;
		Element elTag = null;
		for (Object_tagsTuple tt : rm.getTagValuesForObject(handle)) {
			if (lastTagName==null || !tt.name.equals(lastTagName)) {
				elTag = doc.createElement("tag");
				elTag.setAttribute("name",tt.name);
				elTags.appendChild(elTag);
				lastTagName = tt.name;
			}
			Element elValue = doc.createElement("value");
			elValue.appendChild(doc.createTextNode(tt.value));
			elTag.appendChild(elValue);
		}
		elObject.appendChild(elTags);
		
		return elObject;
	}
	
	@GET
	@Path("")
	@Produces("application/xml")
	/**
	 * REST API entry point for the GET /object URI. Dumps all objects, 
	 * including their complete version history and tags.
	 * @return An XML {@code <objects/>} element.
	 */
	public Response getAllObjects() {
		XmlResponse response = new XmlResponse();
		Element elObjects = response.doc.createElement("objects");
		
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()){
			SelectionTransformer<ObjectsTuple> st = new SelectionTransformer<ObjectsTuple>() {
				public boolean action(ObjectsTuple ot) throws U_Exception {
					elObjects.appendChild(describeObjectIntoXml(ot.handle, rm, response.doc));
				return true;
				}
			};
			
			rm.applyToObjects("true", st);
		} catch (Exception e) {
			return response.Failure(e);
		} 
		return response.Success(elObjects);
	}
	
	@GET
	@Path("{shandle:[0-9]+}")
	@Produces("application/xml")
	/**
	 * REST API entry point for the GET /object/{handle} URI.
	 * @param shandle The inject path parameter of the object handle sought.
	 * @return An XML {@code <object/>} element.
	 */
	public Response getObject(
			@PathParam("shandle") String shandle
			) {
		XmlResponse response = new XmlResponse();
		int handle = 0;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		Element elObject=null;
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()){

			elObject = describeObjectIntoXml(handle, rm, response.doc);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchHandle)
				return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,shandle,e),Status.NOT_FOUND);
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(e);
		}
		return response.Success(elObject);
	}

	@GET
	@Path("schema")
	@Produces("application/xml")
	/**
	 * REST API entry point for the GET /object/schema URI.
	 * @return An XML {@code <schema/>} element.
	 */
	public Response getObjectSchema() 
	{
		XmlResponse response = new XmlResponse();
		
		
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()){
			return response.Success(rm.getObjectSchema(response.doc),Status.OK);
		} catch (Exception e) {
			return response.Failure(e);
		}
	}

	@GET
	@Path("{shandle:[0-9]+}/download")
	@Produces("application/octet-stream")
	/**
	 * REST API entry point for the GET /object/handle/download URI.
	 * @param shandle The injected path parameter of the object sought.
	 * @param sversioncount The optional injected query parameter of 
	 * the version number.
	 * @return
	 */
	public Response getObjectContent(@PathParam("shandle") String shandle,
						 	  @QueryParam("versioncount") @DefaultValue("") String sversioncount) {
		XmlResponse response = new XmlResponse();
		int handle = 0;
		Integer versioncount = null;

		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,"handle='"+shandle+"'",e),Status.BAD_REQUEST);
		}
		
		if (!sversioncount.equals(""))
		try {
			versioncount=Integer.parseUnsignedInt(sversioncount);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,"versioncount='"+sversioncount+"'",e),Status.BAD_REQUEST);			
		}
		
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()){
			List<VersionsTuple> lvt = rm.getVersions(handle);
			if (lvt.size()==0)
				throw new U_Exception(ERROR.NoSuchVersion,"There are no versions");
			VersionsTuple vt = null;
			if (versioncount!=null) {
				for (VersionsTuple x : lvt)
					if (versioncount.equals(x.versioncount)) {
						vt = x;
						break;
					}
			} else
				vt = lvt.get(0);
			if (vt==null)
				throw new U_Exception(ERROR.NoSuchVersion,"versioncount='"+sversioncount+"'");
			//We have located the version record.  Now to stream the contents
			//of the file as our result.
			//1. Does the file exist and can we open it?
			File f=new File(vt.path);
			InputStream tempis = null;
			try {
				tempis = new FileInputStream(vt.path);
			} catch (Exception e) {
				throw new U_Exception(ERROR.Unexpected,"content file is missing");
			}
			final InputStream is = tempis; 
			//2. Create a lambda function to do the copy
			final int BUFSZ = 5096;
			StreamingOutput output = (OutputStream os) -> {				
				byte[] buf = new byte[BUFSZ];
				int bytesread = 0;
				try {
				while ( (bytesread=is.read(buf)) >=0 ) {
					os.write(buf,0,bytesread);
				}
				//The above exits only on end-of-file
				is.close();
				os.close();
				} catch (Exception e) {
					throw new RuntimeException("Unexpected runtime exception",e);
				}
				
			};
			
			//Build the response
			ResponseBuilder builder = Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).entity(output);
			String filename = Util.sanitizedFilename(f.getName());
			if (filename.length()==0)
				filename="download.bin";
			builder.header("Content-Disposition",
						   "attachment; filename=\""+filename+"\"");
			return builder.build();
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchHandle)
				return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,shandle,e),Status.NOT_FOUND);
			if (e.errorCode==ERROR.NoSuchVersion)
				return response.Failure(new AE(AE.ERR_UNKNOWN_VERSION,sversioncount,e),Status.NOT_FOUND);
			return response.Failure(e);
		}  catch (Exception e) {
			return response.Failure(e);
		}
	}
	
	
	@PUT
	@Path("{shandle:[0-9]+}")
	@Consumes("application/xml")
	@Produces("application/xml")
	/**
	 * REST API entry point for PUT /object/{handle}
	 * @param shandle
	 * @param input
	 * @return
	 */
	public Response putObject(
			@PathParam("shandle") String shandle,
			InputStream input
			) {
		XmlResponse response = new XmlResponse();

		int handle = 0;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		String transUuid = "-noid-";   //This is for debugging via stderr
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			VersionsTuple vt = rm.getLatestVersion(handle);
			//Validate incoming content
			//
			// 1. Is it XML that we can parse into a document?
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			dbf.setIgnoringComments(true);
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document docin = db.parse(input);
			// 2. Is it rooted with an <object> element? Does the handle match?
			XPath xp = XPathFactory.newInstance().newXPath();
			String scheckhandle =  xp.compile("/object/@handle").evaluate(docin);
			if (scheckhandle==null)
				throw new U_Exception(ERROR.BadParameter,"Missing 'object' root element or 'handle' attribute");
			try {
				int checkhandle=Integer.parseUnsignedInt(scheckhandle);
				if (checkhandle!=handle)
					throw new U_Exception(ERROR.BadParameter,"Handle in XML document does not match handle in the URL");
			} catch (NumberFormatException e) {
				throw new U_Exception(ERROR.BadParameter,"Invalid 'handle' attribute value: '"+scheckhandle+"'");
			}
			// 3. Start a transaction with the resource manager
			try {
				rm.startTransaction();
				transUuid = UUID.randomUUID().toString();
				System.err.println("***TRANSACTION START - "+transUuid);
			// 4. If it has a <versions> element with a sub-element <version> that has current="true" attribute
				Node node = (Node) xp.compile("/object/versions/version[@current='true']").evaluate(docin,XPathConstants.NODE);
				if (node!=null) {
			//      If it has a title element and the title is different
					String title = xp.compile("attributes/title").evaluate(node);
					if (!title.equals(vt.title)) {
			//          Update the title in the tuple
						vt.title=title;
			//          Post the tuple to the database
						rm.putVersion(vt);
					}
				}
			// 4. If it has a <tags> element
				node = (Node) xp.compile("/object/tags").evaluate(docin, XPathConstants.NODE);
				if (node!=null) {
			// 		 Delete all tag pairs for this handle
					 for (Object_tagsTuple vtt : rm.getTagValuesForObject(handle))
						 rm.untagObject(handle, vtt.name, vtt.value);
			//       For each <tag> element, attempt to apply the tag.
					 NodeList nodes = (NodeList) xp.compile("tag").evaluate(node,XPathConstants.NODESET);
					 if (nodes!=null) {
						 for (int x=0; x<nodes.getLength(); x++) {
							 NamedNodeMap attrs = nodes.item(x).getAttributes();
							 Node nname=null;
							 Node nvalue=null;
							 String name=null;
							 String value=null;
							 if (attrs==null 
							   | (nname=attrs.getNamedItem("name"))==null
							   | (nvalue=attrs.getNamedItem("value"))==null
							   | (name=nname.getNodeValue()).equals("")
							   | (value=nvalue.getNodeValue()).equals("")
							   )
								 throw new U_Exception(ERROR.BadParameter,"Malformed tag element");
							 rm.tagObject(handle, name, value);
						 }
					 }

				}
			// 5. Commit the transaction
				rm.commitTransaction();
				System.err.println("***TRANSACTION COMMITTED - "+transUuid);
				transUuid="-noid-";
			// 6. Make sure to rollback the transaction if we fail for any reason
			} finally {
				if (rm.transactionInProgress())
					try {
						rm.rollbackTransaction();
						System.err.println("***TRANSACTION ROLLEDBACK - "+transUuid);
						transUuid="-noid-";
					} catch (Exception e) {/*best effort*/}
			}
			
			if (!transUuid.equals("-noid-"))
				System.err.println("***TRANSACTION ORPHANED!!! - "+transUuid);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchHandle)
				return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,shandle,e),Status.NOT_FOUND);
			if (e.errorCode==ERROR.ParserError | e.errorCode==ERROR.BadParameter)
				return response.Failure(new AE(AE.ERR_XML_PARSE_ERROR,"",e),Status.BAD_REQUEST);
			if (e.errorCode==ERROR.NoSuchTagValuePair)
				return response.Failure(new AE(AE.ERR_UNKNOWN_TAG_NAME_OR_VALUE,"",e),Status.BAD_REQUEST);
			return response.Failure(e);
		} catch (SAXException e) {
			return response.Failure(new AE(AE.ERR_XML_PARSE_ERROR,"",e),Status.BAD_REQUEST);
		} catch (IOException e) {
			return response.Failure(new AE(AE.ERR_INTERNAL_IO_ERROR,"",e),Status.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			return response.Failure(e);
		}
		Element elSuccess = response.doc.createElement("success");
		return response.Success(elSuccess);
	}

	@DELETE
	@Path("{shandle:[0-9]+}/delete")
	@Produces("application/xml")
	/**
	 * REST API entry point for DELETE /object/{handle}/delete
	 * @param shandle
	 * @return
	 */
	public Response deleteObject(
			@PathParam("shandle") String shandle,
			@QueryParam("force") @DefaultValue("false") String sforce
			) {
		XmlResponse response = new XmlResponse();
		int handle = 0;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		if (sforce==null) 
			sforce="false";
		if (!sforce.equals("true") && !sforce.equals("false"))
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,sforce,null),Status.BAD_REQUEST);
		boolean force=sforce.equals("true");
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			rm.retireObject(handle, force);
		} catch (U_Exception e) {
			switch (e.errorCode) {
			case NoSuchHandle: return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,shandle,e));
			case InconsistentDatabase: return response.Failure(new AE(AE.ERR_INCONSISTENT_DATABASE,"",e));
			default: return response.Failure(new AE(AE.ERR_UNSPECIFIED,"",e));
			}
		} catch (Exception e) {
			/** shouldn't happen **/
		}
		return response.Success(response.doc.createElement("success"));
	}
	
	@DELETE
	@Path("{shandle:[0-9]+}/rollback")
	@Produces("application/xml")
	/**
	 * REST API entry point for DELETE /object/{handle}/rollback
	 * @param shandle
	 * @return
	 */
	public Response rollbackObject(
			@PathParam("shandle") String shandle) 
	{
		XmlResponse response = new XmlResponse();
		int handle = 0;
		Instant timestamp=null;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(new AE(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			timestamp=rm.rollbackObjectToPreviousVersion(handle);
		} catch (U_Exception e) {
			switch (e.errorCode) {
			case NoSuchHandle: return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,shandle,e));
			case InconsistentDatabase: return response.Failure(new AE(AE.ERR_INCONSISTENT_DATABASE,"",e));
			default: return response.Failure(new AE(AE.ERR_UNSPECIFIED,"",e));
			}
		} catch (Exception e) {
			return response.Failure(new AE(AE.ERR_UNEXPECTED,"during version rollback",e));
		}
		Element elSuccess = response.doc.createElement("success");
		elSuccess.setAttribute("imported", timestamp.toString());
		return response.Success(elSuccess);
	}
	
	@PUT
	@Path("{shandle:[0-9]+}/tags/tag/{name}/{value}")
	@Produces("application/xml")
	/**
	 * REST API entry point.
	 * @param shandle
	 * @param input
	 * @return
	 */
	public Response putObjectTag(
			@PathParam("shandle") String shandle,
			@PathParam("name") String name,
			@PathParam("value") String value,
			@QueryParam("autocreate") @DefaultValue("no") String sautocreate
			) {
		XmlResponse response = new XmlResponse();

		shandle=Util.nullWrap(shandle).trim();
		int handle = 0;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		name=Util.nullWrap(name).trim();
		if (name.length()==0)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid tag name"),Status.BAD_REQUEST);
		value=Util.nullWrap(value).trim();
		if (name.length()==0)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid tag value"),Status.BAD_REQUEST);
		boolean autocreate=false;
		try {
			autocreate=Util.parseBoolean(Util.nullWrap(sautocreate));
		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid boolean value for autocreate"),Status.BAD_REQUEST);
		}

		
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			try {
				rm.startTransaction();
				if (autocreate)
					rm.createTagValue(name, value);
				rm.tagObject(handle, name, value);
				rm.commitTransaction();
			} catch (U_Exception e) {
				if (e.errorCode==ERROR.NoSuchHandle)
					return response.Failure(new AE(AE.ERR_UNKNOWN_HANDLE,"'"+shandle+"'",e),Status.NOT_FOUND);
				if (e.errorCode==ERROR.ParserError | e.errorCode==ERROR.BadParameter)
					return response.Failure(new AE(AE.ERR_XML_PARSE_ERROR,"",e),Status.BAD_REQUEST);
				if (e.errorCode==ERROR.NoSuchTagValuePair)
					return response.Failure(new AE(AE.ERR_UNKNOWN_TAG_NAME_OR_VALUE,"",e),Status.BAD_REQUEST);
				return response.Failure(e);
			} finally {
				if (rm.transactionInProgress()) 
					try { rm.rollbackTransaction(); } catch (Exception e) { }
			}
		} catch (Exception e) {
			/* best effort */

		}
		Element elSuccess = response.doc.createElement("success");
		return response.Success(elSuccess);
	}

	@DELETE
	@Path("{shandle:[0-9]+}/tags/tag/{name}/{value}")
	@Produces("application/xml")
	public Response deleteObjectTag(
		@PathParam("shandle") String shandle,
		@PathParam("name") String name,
		@PathParam("value") String value
		) {
		XmlResponse response = new XmlResponse();
	
		shandle=Util.nullWrap(shandle).trim();
		int handle = 0;
		try {
			handle=Integer.parseUnsignedInt(shandle);
		} catch (NumberFormatException e) {
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,shandle,e),Status.BAD_REQUEST);
		}
		name=Util.nullWrap(name).trim();
		if (name.length()==0)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid tag name"),Status.BAD_REQUEST);
		value=Util.nullWrap(value).trim();
		if (name.length()==0)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid tag value"),Status.BAD_REQUEST);
		
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			rm.untagObject(handle, name, value);
		} catch (U_Exception e) {
			return response.Failure(e);
		} catch (Exception e) {
			/* ignore */
		}
		Element elSuccess = response.doc.createElement("success");
		return response.Success(elSuccess);
	}


}
