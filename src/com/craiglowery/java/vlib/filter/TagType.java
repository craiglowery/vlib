package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.util.TreeSet;

/**
 * Implements the TagType by extending the TreeSet<String> class.
 *
 */
public class TagType {

	private TreeSet<String>[] ts;

	/** Create from an array of String **/
	@SuppressWarnings("unchecked")
	public TagType(String...strings) {
		super();
		ts = new TreeSet[2];
		ts[0]=new TreeSet<String>();
		ts[1]= new TreeSet<String>();
		for (String s : strings) {
			ts[0].add(s);
			ts[1].add(s.toLowerCase());
		}
	}
	
	@SuppressWarnings("unchecked")
	public TagType(TreeSet<String> tscs) {
		super();
		ts = new TreeSet[2];
		ts[0]=tscs;
		ts[1]= new TreeSet<String>();
		for (String s : tscs)
			ts[1].add(s.toLowerCase());
		}
	
	public TagType(TreeSet<String>[] ts) {
		super();
		this.ts = ts;
	}
	
	public boolean contains(String s, boolean ignoreCase) {
		if (ts==null)
			return false;
		if (ignoreCase)
		   return ts[1].contains(s.toLowerCase());
		return ts[0].contains(s);
	}
	
	public boolean equals(TagType other, boolean ignoreCase) {
		if (ts==other.ts)
			return true;
		if (ts==null || other.ts==null)
			return false;
		if (ignoreCase)
			return ts[1].equals(other.ts[1]);
		return ts[0].equals(other.ts[0]);
	}
	
	@Override
	public String toString() {
		if (ts==null)
			return "[]";
		StringBuffer sb = new StringBuffer();
		for (String s : ts[0]) {
			sb.append(sb.length()==0?"[":",").append(s);
		}
		if (sb.length()==0) 
			return "[]";
		sb.append("]");
		return sb.toString();
	}


}