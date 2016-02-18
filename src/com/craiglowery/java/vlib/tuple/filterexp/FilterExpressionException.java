package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
public class FilterExpressionException extends Exception {
	
	private static final long serialVersionUID = -8368777312667417938L;

	public FilterExpressionException(String message, Object...args) {
		super(String.format(message,args));
	}
	
	public FilterExpressionException(Throwable cause) {
		super(cause);
	}
	
	public FilterExpressionException(String message, Throwable cause, Object...args) {
		super(String.format(message,args),cause);
	}
	
}