package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import com.craiglowery.java.vlib.common.U_Exception;

/**
 * This class should be extended to provide a resource model for attaching to 
 * back-end persistence systems, like SQL databases or non-structured table stores.
 * It typically is created by providing credentials and URI to a constructor, and closing
 * those things when "close" is called.  It should also support the semantic of 
 * transactions.  
 * 
 * An object subclassed from this class will be passed to tuple wiring objects when
 * performing tuple operations.
 *
 */
public abstract class PersistenceConnection implements AutoCloseable {
	
	public PersistenceConnection() throws U_Exception {
	}
	
	public abstract void startTransaction() throws U_Exception;
	
	public abstract void commitTransaction() throws U_Exception;
	
	public abstract void rollbackTransaction() throws U_Exception;
	
	public abstract boolean transactionInProgress() throws U_Exception;
	
	public abstract boolean isValid() throws U_Exception;
	
	public abstract void close();

}
