package com.craiglowery.java.vlib.api;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;

import org.w3c.dom.*;

/**
 * A class for building an Xml based response to be returned in web application exceptions.<p>
 * 
 * After creating an instance, you will get a Response object for use with a web application exception
 * by calling either Success or Error.<p>
 * 
 * If via Success, you may provide content to be returned inside a 'result' element using this object's publicly
 * exposed doc object.
 * 
 * <pre>
 *       {@code <result>}
 *           {@code <element_you_provide/>}
 *       {@code </result>}
 * </pre>
 * 
 * If via Failure, you must provide an error code from the ApiError object and an optional Throwable
 * which is the cause of the failure. An 'error' element will be returned.
 * 
 * <pre>
 *       {@code <error>}
 *           {@code <code>}<i>errorcode</i>{@code </code>}
 *           {@code <description>}<i>local-specific error message</i>{@code </description>}
 *           {@code <cause>}<i>additional details</i>{@code </cause>}
 *       {@code </error>}
 * </pre>
 * 
 */
public class XmlResponse {
	 
	public Document doc = null;
	
	/**
	 * Creates a new instance of a response object.  Call its Success or Failure methods
	 * to create a Response object for JAX-RS returns.  Use its {@code doc} attribute
	 * to create XML entities to be included in the returned XML document.
	 * @throws ParserConfigurationException
	 */
	public XmlResponse()  
	{
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder docBuilder;
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException pce) {
			return;
		}
		doc = docBuilder.newDocument();
	}	
	
	/**
	 * Builds a response with the value of the 'result' root element being the content return.
	 * The HTTP Response code will be set to OK if statusCode is {@code null}. The
	 * content, if not null, will be made the subordinate child node of the root element.
	 * @param content  The XML fragment to be returned inside the {@code result} element.
	 * @param statusCode The HTTP status code to return. A value of null means OK.
	 * @return A JAX-RS {@code Response} object.
	 */
	public Response Success(Node content, StatusType statusCode) {
		Element elResult = doc.createElement("result");
		
		if (content!=null) {
			elResult.appendChild(content);
		}
		if (statusCode==null)
			statusCode=Status.OK;
		elResult.setAttribute("status", statusCode.toString());
		doc.appendChild(elResult);
		return Response.status(statusCode).type(MediaType.APPLICATION_XML_TYPE).entity(new DOMSource(doc)).build();
	}
	
	/**
	 * Builds a response with the value of the 'result' root element being the content return.
	 * The HTTP Response code will be set to OK. The content, if not null, will be made the 
	 * subordinate child node of the root element.
	 * @param content  The XML fragment to be returned inside the {@code result} element.
	 * @return A JAX-RS {@code Response} object.
	 */
	public Response Success(Node content) {
		return Success(content,null);
	}
	
	
	/**
	 * Returns a response object that indicates a failure has occurred.
	 * @param errorCode The {@code ApiError.ERR_*} code value.
	 * @param cause Detail of the cause of the error, or {@code null} if not available.
	 * @param statusCode The HTTP status code, or {@code null} if the code is {@code 500 Internal Server Error}.
	 * @return A JAX-RS {@code Response} object.
	 */
	public Response Failure(AE error, Status statusCode) {
		
		if (statusCode==null)
			statusCode=Status.INTERNAL_SERVER_ERROR;

		Element elError = doc.createElement("error");
		elError.setAttribute("status", statusCode.toString());
		error.Xml(elError);
		doc.appendChild(elError);
		return Response.status(statusCode).type(MediaType.APPLICATION_XML_TYPE).entity(new DOMSource(doc)).build();
	}

	public Response Failure(AE apierror) {
		return Failure(apierror,Status.INTERNAL_SERVER_ERROR);
	}

	/**
	 * Returns a response object that indicates a non-specific (generic) failure has occurred.
	 * @param exception If not null, an optional exception.
	 * @return A JAX-RS {@code Response} object.
	 */
	 public Response Failure(Throwable cause) {
		 return Failure(new AE(AE.ERR_UNSPECIFIED,null,cause));
	 }
	 
	 /**
	  * Returns a response object that indicates a non-specific (generic) failure with an unspecified cause
	  * has occured.
  	  * @return A JAX-RS {@code Response} object.
	  */
	 public Response Failure() {
		 return Failure(new AE(AE.ERR_UNSPECIFIED,null,null));
	 }
	 
	 public Response Redirect(String URL, Node content) {
		ResponseBuilder b = Response.status(Status.SEE_OTHER);
		b.header("Location", URL);
		if (content!=null) {
			Element elResult = doc.createElement("result");
			if (content!=null) {
				elResult.appendChild(content);
			}
			elResult.setAttribute("status", Status.SEE_OTHER.getReasonPhrase());
			doc.appendChild(elResult);
			b.entity(new DOMSource(doc));
		}
		return b.build();
	 }

}