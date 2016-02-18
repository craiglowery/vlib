package com.craiglowery.java.vlib.api;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.craiglowery.java.vlib.common.Config;
import com.craiglowery.java.vlib.common.ConfigurationKey;
import com.craiglowery.java.vlib.common.LambdaTwoStrings;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.common.Util;
import com.craiglowery.java.vlib.common.XP;
import com.craiglowery.java.vlib.repository.RepositoryManager;
import com.craiglowery.java.vlib.repository.VersionsTuple;


/**
 * Implements the REST API: Upload Resource Interface.  The the design
 * document for details.
 *
 */

@Path("upload")
public class UploadResourceInterface {
	
	@Context
	/** Injects information about the actual URI used to access this service **/
	private UriInfo uriInfo;

	private String callerid = "user0";  //Need to replace with injected caller ID
	

	
	@POST
	@Path("")
	@Produces("application/xml")
	public Response createUR(
			@QueryParam("handle") @DefaultValue("0") int handle 
			) {

		XmlResponse response = new XmlResponse();
		try {
			UR ur = new UR(handle);
			ur.copyToXml();
			String redirect = uriInfo.getAbsolutePath().toString()+"/"+ur.key;
			return response.Redirect(redirect,ur.getXmlRoot(response.doc));
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.BadParameter)
				return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,e));
			if (e.errorCode==ERROR.NoSuchHandle)
				return response.Failure(AE.ae(AE.ERR_UNKNOWN_HANDLE,e));
			return response.Failure(AE.ae(AE.ERR_UNSPECIFIED,e.errorCode.name(),e));
		} catch (Exception e) {
			return response.Failure(new AE(AE.ERR_UNEXPECTED,"while creating new blob file",e));
		}
	}

	@GET
	@Path("{key}")
	@Produces("application/xml")
	public Response getUR(
			@PathParam("key") String key,
			@QueryParam("computechecksum") @DefaultValue("no") String scomputechecksum
			) {
		XmlResponse response = new XmlResponse();
		key = key.trim();
		if (key.length()==0)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"empty or null key",null),Status.BAD_REQUEST);
		try {
			UR ur = new UR(key);
			boolean computechecksum=Util.parseBoolean(scomputechecksum);
			if (computechecksum && (ur.checksum==null || ur.checksum.equals(""))) {
				try {
					ur.checksum=Util.computeChecksum(URBaseFilename(key));
				} catch (Exception e) {
					return response.Failure(AE.ae(AE.ERR_UNEXPECTED,"While computing checksum",e));
				}
			}
			ur.persist();
			Element elUr = ur.getXmlRoot(response.doc);
			return response.Success(elUr);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.ParserError)
				return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"computechecksum="+scomputechecksum,e),Status.BAD_REQUEST);
			if (e.errorCode==ERROR.IOError)
				return response.Failure(new AE(AE.ERR_UNKNOWN_UR_KEY,key,e),Status.BAD_REQUEST);
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(e);
		}
	}
	

	@Context
	HttpHeaders headers;
	
	@PUT
	@Path("{key}/{offset}")
	@Consumes("application/octet-stream")
	@Produces("application/xml")
	public Response putBlock(
			@PathParam("key") String key,
			@PathParam("offset") int offset,
			@QueryParam("computechecksum") @DefaultValue("no") String scomputechecksum,
			InputStream input
			) {
		XmlResponse response = new XmlResponse();
		long maxfilesize=0;
		try {
			maxfilesize = Config.getLong(ConfigurationKey.MAX_FILE_UPLOAD_SIZE);
			
		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,"Configuration error - MAX_FILE_UPLOAD_SIZE not defined",e));
		}
		// Is the offset valid
		if (offset<0) 
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"invalid offset"),Status.BAD_REQUEST);
		// Did they send anything? How big is it?
		int contentLength=0;
		try {
			contentLength = Integer.parseInt(headers.getHeaderString("Content-Length"));
		} catch (Exception e) {
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"Invalid or missing HTTP header 'Content-Length'",e),Status.BAD_REQUEST);
		}
		//Would the size of the file exceed the max if we wrote this?

		if (offset+contentLength>maxfilesize)
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"Resulting file size exceeds allowable maximum of "+Long.toString(maxfilesize)),Status.BAD_REQUEST);
		// Is the key valid? Do the files exist?
		UR ur = null;
		try {
			ur = new UR(key);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchFile)
				return response.Failure(AE.ae(AE.ERR_UNKNOWN_UR_KEY,e),Status.NOT_FOUND);
		}
		// Seek and write
		File f = null;
		try {
			f=new File(URBaseFilename(ur.key));
			Util.copyStreamToFileAtPosition(input, f, offset);
		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_INTERNAL_IO_ERROR,e));
		}
		// Update and persist the UR
		ur.checksum="";
		ur.lastactivity=Instant.now();
		ur.size=f.length();
		try {
			ur.persist();
		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_INTERNAL_IO_ERROR,e));
		}
		// Return the UR
		Element root = null;
		try {
			root = ur.getXmlRoot(response.doc);
		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_INTERNAL_IO_ERROR,e));
		}
		return response.Success(root);
	}
	

	@POST
	@Path("{key}")
	@Consumes("application/xml")
	@Produces("application/xml")
	public Response finalizeUR(
			@PathParam("key") String key,
			@QueryParam("duplicatecheck") @DefaultValue("yes") String sduplicatecheck,
			InputStream input
			) {
		
		XmlResponse response = new XmlResponse();
		boolean duplicatecheck;
		if (sduplicatecheck==null)
			sduplicatecheck="yes";
		try {
			duplicatecheck = Util.parseBoolean(sduplicatecheck);
		} catch (U_Exception e1) {
			return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"duplicatecheck='"+sduplicatecheck+"'"),Status.BAD_REQUEST);
		}
		UR ur = null;
		try {
			ur = new UR(key);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchFile)
				return response.Failure(AE.ae(AE.ERR_UNKNOWN_UR_KEY,e),Status.NOT_FOUND);
		}
		UriBuilder redirector=null;
		try {
		 redirector = 
				uriInfo
				.getBaseUriBuilder()
				.path(ObjectResourceInterface.class)
				.path(ObjectResourceInterface.class.getMethod("getObject", String.class));
		} catch (NoSuchMethodException e) {
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,"URI building failed",e));
		}

		try {
			Document doc = Util.buildXmlFromInput(input);
			XP xp = new XP(doc);
			//Must have an /upload/filename element that is 
			//  1. non empty/ non-null
			//  2. Has no path separators in it "/" or "\"
			//  3. Has a valid media extension
			String filename = xp.el_text("/upload/filename").trim();
			if (filename.equals("") ||
				filename.contains("/") ||
				filename.contains("\\") ||
				Util.endsWithKnownVideoExtension(filename)==null)
				return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"Invalid filename: '"+filename+"'"));
			ur.filename=filename;
			//Is there a title specified?
			String title = xp.el_text("upload/title").trim();
			//Derive if not, and there wasn't one carried over from the previous version
			if (!title.equals("")) {
				ur.title=title;
			} else if (ur.title.equals(""))
				ur.title=Util.deriveTitle(filename);
			//Try to identify the URI we'll return later - we won't be able
			//to return an error after we invoke the repository
			try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
				if (ur.handle!=0) {
					rm.updateObject(ur.handle, ur.f_content.getAbsolutePath(),ur.filename, ur.title, duplicatecheck);
				} else {
					ur.handle=rm.createObject(ur.f_content.getAbsolutePath(), ur.filename, title, duplicatecheck);
				}
			} catch (U_Exception e) {
				switch (e.errorCode) {
					case PotentialDuplicate: return response.Failure(AE.ae(AE.ERR_DUPLICATE,e),Status.BAD_REQUEST);
					default: return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
				}
			} catch (Exception e) {
				return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
			}

		} catch (U_Exception e) {
			return response.Failure(AE.ae(AE.ERR_XML_PARSE_ERROR,"Uploaded XML not parseable",e),Status.BAD_REQUEST);
		}
		/** Make a best effort to clean up **/
		try {
			ur.f_content.delete();
			ur.f_xml.delete();
		} catch (Exception e) {
			/* ignore */
		}
		Node elUpload = null;
		try {
			ur.copyToXml();
			elUpload=ur.getXmlRoot(response.doc);
		} catch (U_Exception e) {
			elUpload=response.doc.createTextNode("Upload descriptor not available - "+e.getMessage());
		}
		String redirect = uriInfo.getBaseUri().toString();
		try {
			redirect=redirector.build(ur.handle).toString();
		} catch (Exception e) {}
		return response.Redirect(redirect, elUpload);
	}
	
	@DELETE
	@Path("{key}")
	@Produces("application/xml")
	public Response cancelUR(
			@PathParam("key") String key
			) {
		XmlResponse response = new XmlResponse();
		UR ur = null;
		try {
			ur = new UR(key);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.NoSuchFile)
				return response.Failure(AE.ae(AE.ERR_UNKNOWN_UR_KEY,e),Status.NOT_FOUND);
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
		}
		try {
			ur.f_content.delete();
			ur.f_xml.delete();
		} catch (Exception e) {
			/* ignore */
		}
		Element elSuccess=response.doc.createElement("success");
		return response.Success(elSuccess);
	}
	
//--------------- UR Object nested class
	
	private String URBaseFilename(String key) throws U_Exception {
		return (new File(Config.getString(ConfigurationKey.SUBDIR_REPO_UPLOAD),key+"-"+callerid)).getAbsolutePath();
	}

	private interface GetHelper {
		String op(String name) throws U_Exception;
	}

	private class UR {
		DocumentBuilder db;
		Document doc;
		String key;
		int handle;
		String filename;
		String title;
		Instant initiated;
		Instant lastactivity;
		long size;
		String checksum;
		
		File f_content;
		File f_xml;
		
		/** Creates a new UR with given handle and persists it.
		 * 
		 * @param handle If 0, then this UR will create a new object and
		 * a handle will be assigned at finalization. If non-zero, then
		 * a new version will be created and the handle must exist
		 * in the repository.
		 * @throws U_Exception
		 */
		public UR(int handle) throws U_Exception {
			try {
				title="";
				if (handle!=0) {
					try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
						if (!rm.objectExists(handle))
							throw new U_Exception(ERROR.NoSuchHandle,Integer.toString(handle));
						VersionsTuple vt = rm.getLatestVersion(handle);
						title=vt.title;
					} catch (U_Exception e) {
						throw e;
					} catch (Exception e) {
						throw new U_Exception(ERROR.Unexpected,e);
					}
				}
				int tries=10;
				do {
					key=UUID.randomUUID().toString().substring(0, 8);
					String basename = URBaseFilename(key);
					f_content = new File(basename);
					f_xml = new File(basename+".xml");
					try {
						if (f_content.createNewFile() && f_xml.createNewFile())
							break;
					} catch (IOException e) {
						/* ignore */
					}
					if (--tries == 0)
						throw new U_Exception(ERROR.FileError,"Unable to create temporary files after multiple tries");
				} while (tries>0);
				this.handle=handle;
				filename="";
				initiated=Instant.now();
				lastactivity=Instant.now();
				size=0;
				checksum="";				
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				dbf.setValidating(false);
				dbf.setIgnoringComments(true);
				db = dbf.newDocumentBuilder();
				doc = db.newDocument();
				persist();
			} catch (ParserConfigurationException e) {
				throw new U_Exception(ERROR.ParserError,e);				
			}
		}
		
		public UR(String key) throws U_Exception {
			try {
				String bfn = URBaseFilename(key);
				f_content = new File(bfn);
				f_xml = new File(bfn+".xml");
				if (!f_content.isFile() || !f_xml.isFile())
					throw new U_Exception(ERROR.NoSuchFile,key);
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				dbf.setValidating(false);
				dbf.setIgnoringComments(true);
				db = dbf.newDocumentBuilder();
				doc = db.parse(f_xml.getAbsolutePath());
				copyFromXml();
			} catch (ParserConfigurationException | SAXException e) {
				throw new U_Exception(ERROR.ParserError,e);
			} catch (IOException e) {
				throw new U_Exception(ERROR.IOError,e);
			} 
		}

		public void copyToXml() {
			doc = db.newDocument();
			Element elUr = doc.createElement("upload");
			doc.appendChild(elUr);
			
			LambdaTwoStrings i = (name,value) -> {
				Element el = doc.createElement(name);
				el.appendChild(doc.createTextNode(value));
				elUr.appendChild(el);
			};
			
			i.op("key", key);
			i.op("handle", Integer.toString(handle));
			i.op("filename", filename);
			i.op("title", title);
			i.op("initiated", initiated.toString());
			i.op("lastactivity", lastactivity.toString());
			i.op("size", Long.toString(size));
			i.op("checksum", checksum);
			
			
		}

		public Element getXmlRoot(Document doc) throws U_Exception {
			try {
				XPath xp = XPathFactory.newInstance().newXPath();
				Element el=(Element)(xp.compile("/upload").evaluate(this.doc,XPathConstants.NODE));
				return (Element)doc.importNode(el, true);
			} catch (XPathExpressionException e) {
				throw new U_Exception(ERROR.ParserError,e);
			}
		}
		
		public void copyFromXml() throws U_Exception {
			XPath xp = XPathFactory.newInstance().newXPath();

			
			GetHelper i = (name) -> {
				Element el=null;
				try {
					el = (Element)(xp.compile("/upload/"+name).evaluate(doc,XPathConstants.NODE));
				} catch (XPathExpressionException e) {
					throw new U_Exception(ERROR.Unexpected,"Xpath expression error",e);				
				}
				if (el==null)
					throw new U_Exception(ERROR.ParserError,"missing element at /upload/"+name);
				Node t = el.getFirstChild();
				if (t==null) return "";
				if (t.getNodeType()!=Node.TEXT_NODE)
					throw new U_Exception(ERROR.ParserError,"missing element at /upload/"+name);
				return t.getNodeValue();
			};
			
			try {
				key = i.op("key");
				handle = Integer.parseInt(i.op("handle"));
				filename = i.op("filename");
				title = i.op("title");
				initiated = Instant.parse(i.op("initiated"));
				lastactivity = Instant.parse(i.op("lastactivity"));
				size=Long.parseLong(i.op("size"));
				checksum=i.op("checksum");
			} catch (NumberFormatException | DateTimeParseException e) {
				throw new U_Exception(ERROR.ParserError,"while loading UR: "+key,e);
			}
		}		
		public void persist() throws U_Exception {
			copyToXml();
			try {
				TransformerFactory tFactory = TransformerFactory.newInstance();
			    Transformer transformer = tFactory.newTransformer();
			    DOMSource source = new DOMSource(doc);
			    File f = new File(URBaseFilename(key)+".xml");
			    try (OutputStream os = new FileOutputStream(f,false)) {
			    	StreamResult result = new StreamResult(os);
			    	transformer.transform(source,result);
			    } catch (IOException e) {
			    	throw new U_Exception(ERROR.IOError,e);
			    }
			} catch (TransformerException e) {
				throw new U_Exception(ERROR.ParserError,e);
			}		    
		}
		
	}
	
}
