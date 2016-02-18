package com.craiglowery.java.vlib.repository;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.time.Instant;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.tuple.*;

/** 
 * Tuple that maps to the "object" backing table in the backing store.  It's
 * basically a handle and the import date of the current version.
 * 
 * See Tuple for details on the Tuple facility.
 *
 */
public class ObjectsTuple extends Tuple {

	
	static { try {
		registerSubclass(ObjectsTuple.class);
	} catch (U_Exception e) {
		throw new RuntimeException(e);
	} }
	
	@PrimaryKey	@DefaultOnInsert 	public Integer handle=-1;
	@Attribute	 					public Instant imported=Instant.ofEpochMilli(0);

}