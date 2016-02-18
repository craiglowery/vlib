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

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.repository.RepositoryManager;

/**
 * Implements the REST API: Admin Resource Interface.  
 *
 */

@Path("admin")
public class AdminResourceInterface {
	
	
	@GET
	@Path("UnusedTagsReport")
	@Produces("application/xml")
	public Response getUnusedTagsReport() 
	{
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			return response.Success(rm.scrubTags(response.doc, true));
		} catch (U_Exception e) {
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(e);
		}
	}
	
	@GET
	@Path("ScrubUnusedTags")
	@Produces("application/xml")
	public Response scrubUnusedTags() 
	{
		XmlResponse response = new XmlResponse();
		try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
			return response.Success(rm.scrubTags(response.doc, false));
		} catch (U_Exception e) {
			return response.Failure(e);
		} catch (Exception e) {
			return response.Failure(e);
		}
	}
		
	
	
}
