package com.craiglowery.java.vlib.repository;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.tuple.PrimaryKey;
import com.craiglowery.java.vlib.tuple.Tuple;
/** 
 * Tuple that maps to the "objectags" backing table in the backing store.  It's
 * basically a handle, a tag name, and a tag value.  When objects are tagged
 * and untagged, this is the table that is altered.
 * 
 * See Tuple for details on the Tuple facility.
 *
 */
public class Object_tagsTuple extends Tuple {
	
	static { try {
		registerSubclass(Object_tagsTuple.class);
	} catch (U_Exception e) {
		throw new RuntimeException(e);
	} }

	
	@PrimaryKey		public String 	name="";
	@PrimaryKey		public String	value="";
	@PrimaryKey		public Integer	handle=-1;
}