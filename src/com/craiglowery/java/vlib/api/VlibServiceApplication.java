package com.craiglowery.java.vlib.api;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import com.craiglowery.java.vlib.common.Util;

import java.util.Set;
import java.util.HashSet;

/**
 * JAX-RS setup for the REST API.
 *
 */
@ApplicationPath("/api")
public class VlibServiceApplication extends Application {

	private Set<Object> singletons = new HashSet<Object>();
	private Set<Class<?>> resources = new HashSet<Class<?>>();
	
	public VlibServiceApplication() {
		Util.initialize();
		resources.add(ErrorResourceInterface.class);		
		resources.add(ObjectResourceInterface.class);	
		resources.add(TagResourceInterface.class);	
		resources.add(UploadResourceInterface.class);
		resources.add(ConfigurationResourceInterface.class);
		resources.add(RepositoryManagerResourceInterface.class);
		resources.add(AdminResourceInterface.class);
		resources.add(QueryResourceInterface.class);
	}
	
	@Override
	public Set<Class<?>> getClasses() {
		return resources;
	}
	
	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}
	
}
