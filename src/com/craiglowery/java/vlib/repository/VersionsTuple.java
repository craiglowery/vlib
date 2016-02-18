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
import com.craiglowery.java.vlib.tuple.Attribute;
import com.craiglowery.java.vlib.tuple.PrimaryKey;
import com.craiglowery.java.vlib.tuple.Tuple;
/** 
 * Tuple that maps to the "versions" backing table in the backing store.  It's
 * basically the "object" representation that end users see, with all the 
 * attributes that describe a version of content.
 * 
 * See Tuple for details on the Tuple facility.
 *
 */
public class VersionsTuple extends Tuple {
	
	static { try {
		registerSubclass(VersionsTuple.class);
	} catch (U_Exception e) {
		throw new RuntimeException(e);
	} }

	
	@PrimaryKey 	public Integer 		handle=-1;
	@PrimaryKey 	public Instant 		imported=Instant.ofEpochMilli(0);
	@Attribute 		public Long 		length=-1L;
	@Attribute 		public String 		sha1sum="";
	@Attribute  	public String 		title="";
	@Attribute 		public String 		path="";
	@Attribute 		public String 		copiedfrom="";
	@Attribute		public Long			inode=-1L;
	@Attribute		public Instant		hm_lastseen=Instant.ofEpochMilli(0);
	@Attribute		public Boolean		hm_missing=false;
	@Attribute		public Instant 		hm_lastfingerprinted=Instant.ofEpochMilli(0);
	@Attribute		public Boolean		hm_unhealthy=false;
	@Attribute		public Boolean		hm_lengthmismatch=false;
	@Attribute		public Long			hm_linkcount=-1L;
	@Attribute		public Instant		hm_lastsuccessfulvalidation=Instant.ofEpochMilli(0);
	@Attribute		public Instant		hm_lastvalidationattempt=Instant.ofEpochMilli(0);
	@Attribute		public Boolean		hm_corrupt=false;
	@Attribute		public String		hm_message="Newly constructed";
	@Attribute		public Boolean		hm_healthchanged=false;
	@Attribute		public String		hm_lastobservedchanges="";
	@Attribute		public Integer		versioncount=0;
}