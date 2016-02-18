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

import org.w3c.dom.Element;

import com.craiglowery.java.vlib.common.Config;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.repository.RepositoryManager;

/**
 * Implements the REST API: Error Resource Interface.  The the design
 * document for details.
 *
 */

@Path("configuration")
public class ConfigurationResourceInterface {
	
	
	@GET
	@Path("current")
	@Produces("application/xml")
	/**
	 * REST API entry point for /errors/catalog
	 * @return XML of the error catalog.
	 */
	public Response getConfiguration() 
	{
		XmlResponse response = new XmlResponse();
		try {
			Element elConfiguration = Config.getConfigurationXml(response.doc);
			return response.Success(elConfiguration);
		} catch (U_Exception e) {
			return response.Failure(e);
		}
	}

	@GET
	@Path("test")
	@Produces("application/xml")
	/**
	 * REST API entry point for /errors/catalog
	 * @return XML of the error catalog.
	 */
	public Response getTest() 
	{
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			return response.Success(rm.countTestula(response.doc));
		} catch (Exception e) {
			return response.Failure(e);
		}
	}


}
