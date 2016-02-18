package com.craiglowery.java.vlib.db;

/**
 * Thrown by VlibDB methods upon error, indicating a problem interacting with the JDBC or
 * with the underlying database at the JDBC level.
 */
public class VlibDBException extends Exception {
	

	private static final long serialVersionUID = -6857264438746180805L;

	/**
	 * Creates a new exception
	 * @param message A descriptive message.
	 */
	public VlibDBException(String message) {
		super(message);
	}
	
	/**
	 * Creates a new exception
	 * @param message A descriptive message.
	 * @param inner The cause of this exception being thrown.
	 */
	public VlibDBException(String message, Exception inner) {
		super(message,inner);	
	}
	
	/**
	 * Returns the name of the method that threw this exception.
	 * @return The name of the method that threw this exception.
	 */
	public String thrownBy() {
		StackTraceElement[] st = getStackTrace();
		return st.length<1 ? "(unknown source)":st[0].getMethodName();
	}
	
	/**
	 * Returns a formatted message for the exception, and any inner exceptions.
	 */
	public String getMessage() {
		Throwable cause = getCause();
		return "VlibDBException: " +thrownBy()+": "+super.getMessage()+ (cause!=null?"\ncaused by "+cause.getMessage():"");
	}
	
}
