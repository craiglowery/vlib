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
 * Tuple that maps to the "tagvalues" backing table in the backing store.  It's
 * just name and value pairs, which define all the allowable name=value pairs
 * one can tag objects with.
 * 
 * See Tuple for details on the Tuple facility.
 *
 */
public class Tag_valuesTuple extends Tuple {

	static { try {
		registerSubclass(Tag_valuesTuple.class);
	} catch (U_Exception e) {
		throw new RuntimeException(e);
	} }


	@PrimaryKey		public String 	name="";
	@PrimaryKey		public String	value="";
}