package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
	/**
	 * Exceptions specific to parsing errors.
	 */
public class ExprParsingException extends ExprException {
	

	private static final long serialVersionUID = -4041905741536524795L;
	int yycolumn=0;
	String expression="";
	
	public ExprParsingException(String message, String expression, int yycolumn) {
		super(message);
		this.expression = expression;
		this.yycolumn = yycolumn;
	}
	
	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder(super.getMessage());
		sb.append("\n").append(expression).append("\n");
		for (int x=yycolumn; x>0; x--)
			sb.append(' ');
		sb.append("^\n");
		return sb.toString();
	}
	
}