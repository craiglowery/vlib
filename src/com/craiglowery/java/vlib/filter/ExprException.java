package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

/** Thrown by the filter expression subsystem **/
public class ExprException extends Exception {
	
	private static final long serialVersionUID = 8816243705685111045L;

	public ExprException(String message) {
		super(message);
	}
	
	public ExprException(String message, Throwable cause) {
		super(message,cause);
	}
	
	public String getMessage() {
		return "Expression exception: "+super.getMessage();
	}
}