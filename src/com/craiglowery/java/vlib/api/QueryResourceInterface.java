package com.craiglowery.java.vlib.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.w3c.dom.Element;

import com.craiglowery.java.vlib.common.Config;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.Util;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.repository.Object_tagsTuple;
import com.craiglowery.java.vlib.repository.RepositoryManager;
import com.craiglowery.java.vlib.repository.VersionsTuple;


/**
 * Implements the REST API: Query Resource Interface.  The the design
 * document for details.
 *
 */

@Path("query")
public class QueryResourceInterface {
	
	@GET
	@Path("")
	@Produces("application/xml")
	/**
	 * REST API entry point for /errors/catalog
	 * @return XML of the error catalog.
	 */
	public Response query(
			@QueryParam("select") @DefaultValue("") String select,
			@QueryParam("where") @DefaultValue("") String where,
			@QueryParam("orderby") @DefaultValue("") String orderby,
			@QueryParam("includetags") @DefaultValue("no") String sincludetags
			) 
	{
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()){
			//Sanitize parameters
			select = select==null?"":select.trim();
			where = where==null?"":where.trim();
			orderby = orderby==null?"":orderby.trim();
			sincludetags = sincludetags==null?"":sincludetags.trim();
			
			//Validate "select"
			Set<String> selectedAttributesS = new TreeSet<String>(Arrays.asList(Util.parseIdentifierList(select)));
			Set<String> validAttributesS = new TreeSet<String>(Arrays.asList(rm.getClientQueryParameterList()));
			if (selectedAttributesS.size()>0) {
				for (String attribute : selectedAttributesS)
					if (!validAttributesS.contains(attribute))
						return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"unknown query attribute: "+attribute),Status.BAD_REQUEST);
			} else
				selectedAttributesS=validAttributesS;
			
			//validate where
			if (where.equals(""))
				where="true";
				
			//Validate "includetags"
			Boolean includetags=false;
			try {
				includetags=Util.parseBoolean(sincludetags);
			} catch(U_Exception e) {
				return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,"includetags",e),Status.BAD_REQUEST);
			}
			Element elQuery=response.doc.createElement("query");
			elQuery.setAttribute("select", String.join(", ",selectedAttributesS));
			elQuery.setAttribute("where",where);
			elQuery.setAttribute("orderby", orderby);
			elQuery.setAttribute("includetags", includetags?"yes":"no");
			
			Element elObjects=response.doc.createElement("objects");
			elQuery.appendChild(elObjects);
			
			for (VersionsTuple vt : rm.processQuery(where, orderby)) {
				Element elObject=response.doc.createElement("object");
				elObjects.appendChild(elObject);
				Element elAttributes=response.doc.createElement("attributes");
				elObject.appendChild(elAttributes);
				for (String attribute : selectedAttributesS) {
					Element elAttribute = response.doc.createElement(attribute);
					Object value = vt.getAttributeValue(attribute);
					if (value!=null)
						elAttribute.setTextContent(value.toString());
					elAttributes.appendChild(elAttribute);
				}
				if (includetags) {
					Element elTags = response.doc.createElement("tags");
					elObject.appendChild(elTags);
					for (Object_tagsTuple ott : rm.getTagValuesForObject(vt.handle)) {
						Element elTag=response.doc.createElement("tag");
						elTag.setAttribute("name",ott.name);
						elTag.setAttribute("value",ott.value);
						elTags.appendChild(elTag);
					}
				}
			}
			
			return response.Success(elQuery);
		} catch (U_Exception e) {
			if (e.errorCode==ERROR.ExpressionError)
				return response.Failure(AE.ae(AE.ERR_BAD_PARAMETER,e),Status.BAD_REQUEST);
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(AE.ae(AE.ERR_UNEXPECTED,e));
		}
	}

	
}
