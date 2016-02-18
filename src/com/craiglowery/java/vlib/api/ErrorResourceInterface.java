package com.craiglowery.java.vlib.api;

/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Implements the REST API: Error Resource Interface.  The the design
 * document for details.
 *
 */

@Path("errors")
public class ErrorResourceInterface {
	
	
	@GET
	@Path("catalog")
	@Produces("application/xml")
	/**
	 * REST API entry point for /errors/catalog
	 * @return XML of the error catalog.
	 */
	public Response getErrorCatalog() 
	{
		XmlResponse response = new XmlResponse();
		return response.Success(AE.allMessagesXml(response.doc));
	}
	
}
