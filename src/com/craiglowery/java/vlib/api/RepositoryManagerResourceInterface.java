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

import com.craiglowery.java.vlib.repository.RepositoryManager;

/**
 * Implements the REST API: RepositoryManager Resource Interface.
 *
 */

@Path("repositorymanager")
public class RepositoryManagerResourceInterface {
	
	
	@GET
	@Path("status")
	@Produces("application/xml")
	/**
	 * REST API entry point reporting status of the repository manager static
	 * class state.
	 * @return XML of the current RM table.
	 */
	public Response getStatus() 
	{
		XmlResponse response = new XmlResponse();
		return response.Success(RepositoryManager.statusXml(response.doc));
	}
	
}
