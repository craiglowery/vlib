package com.craiglowery.java.vlib.api;

/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 * Notes: The localization mechanism is very simple and should be replaced
 *        with something more scalable if ever placed into wide-use production.
 */

import java.io.PrintWriter;
import java.io.StringWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


/***
 * Used to describe errors passed back through the REST API.
 */
public class AE extends Exception {

	private static final long serialVersionUID = 2868954980870134815L;

	/** The index of messages for this localization **/
	private static int regioncodeindex=0;
	
	/** The total number of error codes defined **/
	public static int NUM_ERRORS =11;
	
	/** The total number of region codes defined **/
	public static int NUM_REGIONS = 1;
	
	private static String[] regioncodes = { "EN" };  //Add others to this list
	private static int EN=0;
	
	/** Arrays of localized messages for various supported languages **/
	public static String[][] messages = new String[regioncodes.length][NUM_ERRORS];
	
	/** List of error codes.  Should include values from 0..NUM_ERRORS-1 **/
	public static final int ERR_UNSPECIFIED = 0;
	public static final int ERR_UNKNOWN_HANDLE = 1;
	public static final int ERR_BAD_PARAMETER = 2;
	public static final int ERR_UNKNOWN_VERSION = 3;
	public static final int ERR_XML_PARSE_ERROR = 4;
	public static final int ERR_INTERNAL_IO_ERROR = 5;
	public static final int ERR_UNKNOWN_TAG_NAME_OR_VALUE = 6;
	public static final int ERR_INCONSISTENT_DATABASE = 7;
	public static final int ERR_UNEXPECTED = 8;
	public static final int ERR_UNKNOWN_UR_KEY =  9;
	public static final int ERR_DUPLICATE = 10;
	
	static { //Populate the message array here
		messages[EN][ERR_UNSPECIFIED] = "Unspecified error";
		messages[EN][ERR_UNKNOWN_HANDLE] = "Unknown object handle";
		messages[EN][ERR_BAD_PARAMETER] = "Bad parameter";
		messages[EN][ERR_UNKNOWN_VERSION] = "Unknown object version";
		messages[EN][ERR_XML_PARSE_ERROR] = "XML parse error";
		messages[EN][ERR_INTERNAL_IO_ERROR] = "Internal I/O error";
		messages[EN][ERR_UNKNOWN_TAG_NAME_OR_VALUE] = "Unknown tag name or value";
		messages[EN][ERR_INCONSISTENT_DATABASE] = "Operation not completed due to database inconsistencies";
		messages[EN][ERR_UNEXPECTED] = "An unexpected error occurred caused by system failure or coding error";
		messages[EN][ERR_UNKNOWN_UR_KEY] = "Unknown upload resource key";
		messages[EN][ERR_DUPLICATE] = "Duplicate content";
	}

	/** The error code represented by this exception **/
	int errorcode=0;
	String detail="";	

	/**
	 * Create a new exception for this API error.
	 * 
	 * @param errorcode  The numeric code, drawn from {@code ApiError.ERR_*}
	 * @param cause The {@code Throwable} describing what caused the error.
	 */
	public AE(int errorcode, String detail, Throwable cause) {
		super(cause);
		if (errorcode<0 || errorcode>=NUM_ERRORS)
			errorcode=ERR_UNSPECIFIED;
		this.errorcode=errorcode;
		if (detail!=null)
			this.detail=detail;
	}
	
	@Override
	/**
	 * Returns a localized message for this error.  
	 */
	public String getMessage() {
		if 	(errorcode>=0 && errorcode<NUM_ERRORS && messages[regioncodeindex][errorcode]!=null)
			return messages[regioncodeindex][errorcode];
		return "unavailable";
	}
	
	/**
	 * Sets the language for localized messages.
	 * @param regioncode The language code as a string, such as {@code EN}
	 */
	public static void setLanguage(String regioncode) {
		if (regioncode.equals("EN"))
			regioncodeindex=0;
		else
			throw new RuntimeException("Unknown/unsupported language "+regioncode);
	}
	
	/**
	 * Returns the currently set region code for messages.
	 * @return The current region code.
	 */
	public static String getLanguage() {
		return regioncodes[regioncodeindex];
	}
	
	/**
	 * Returns a list of region codes supported.
	 * @return String of comma separated codes.
	 */
	public static String getSupportedLanguages() {
		StringBuffer sb = new StringBuffer(regioncodes[0]);
		for (int i=1; i<regioncodes.length; i++)
			sb.append(",").append(regioncodes[regioncodeindex]);
		return sb.toString();
	}
	
	/**
	 * Returns an XML representation of the error object.
	 * @param doc The Xml document object owning the returned fragment.
	 * @return An Xml element which is the root of the fragment.
	 */
	public void Xml(Element elError) {
		Document doc = elError.getOwnerDocument();
		Element elCode = doc.createElement("code");
		elCode.appendChild(doc.createTextNode(Integer.toString(errorcode)));
		elError.appendChild(elCode);
		
		Element elDescription = doc.createElement("description");
		elDescription.appendChild(doc.createTextNode(getMessage()));
		elError.appendChild(elDescription);
		
		Element elDetail = doc.createElement("detail");
		elDetail.appendChild(doc.createTextNode(detail));
		elError.appendChild(elDetail);
		
		Element elCause = doc.createElement("cause");
		Throwable cause = getCause();
		if (cause!=null) {
			java.io.StringWriter sw = new StringWriter();
			java.io.PrintWriter pw = new PrintWriter(sw, true);
			cause.printStackTrace(pw);
			elCause.appendChild(doc.createTextNode(sw.toString()));
		}
		elError.appendChild(elCause);
		
	}
	
	/**
	 * An XML representation of all error codes and messages.
	 * @param doc The Xml document object owning the returned fragment.
	 * @return An Xml element which is the root of the fragment.
	 */
	static public Element allMessagesXml(Document doc) {
		Element elErrors = doc.createElement("errors");
		
		for (int code=0; code<NUM_ERRORS; code++) {
			Element elError = doc.createElement("error");
			
			Element elCode = doc.createElement("code");
			elCode.appendChild(doc.createTextNode(Integer.toString(code)));
			elError.appendChild(elCode);
			
			for (int region=0; region<NUM_REGIONS; region++) {
				Element elDescription = doc.createElement("description");
				elDescription.setAttribute("regioncode",regioncodes[region]);
				elDescription.appendChild(doc.createTextNode(messages[region][code]));
				elError.appendChild(elDescription);
			}
			elErrors.appendChild(elError);
		}		
		
		return elErrors;
	}
	
	//Convenience methods
	public static AE ae(int errorcode, String detail, Throwable cause) {
		return new AE(errorcode, detail, cause);
	}
	
	public static AE ae(int errorcode) {
		return new AE(errorcode,"",null);
	}
	
	public static AE ae(int errorcode, String detail) {
		return new AE(errorcode,detail,null);
	}
	
	public static AE ae(int errorcode, Throwable cause) {
		return new AE(errorcode,"",cause);
	}
	
	
}
