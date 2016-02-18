package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.time.Instant;
import java.time.ZoneId;

import com.craiglowery.java.vlib.tuple.filterexp.SmartDateTimeParser;

/**
 * Implements the TimeStampType using java.time.Instant and constructors and
 * methods to wrap it.
 *
 */
public class TimeStampType {
	public Instant instant;
	
	/** Create from an instant instance **/
	public TimeStampType(Instant i) {
		instant=i;
	}
	
	/** Create with value of current time **/
	public TimeStampType() {
		instant=Instant.now();
	}
	
	/** Create by parsing from a string 
	 * @throws ExprException **/
	public TimeStampType(String s) throws java.lang.RuntimeException {
		try {
			instant=SmartDateTimeParser.parse(s);
		} catch (Exception e) {
			throw new RuntimeException("TimeStamp parsing error: "+e.getMessage());
		}
	}

	public int compareTo(TimeStampType o) {
		return instant.compareTo(((TimeStampType)o).instant);
	}
	
	@Override
	public String toString() {
		return java.time.LocalDateTime.ofInstant(instant,ZoneId.systemDefault()).toString();
	}
}