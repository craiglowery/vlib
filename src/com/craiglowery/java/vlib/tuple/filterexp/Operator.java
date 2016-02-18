package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
public enum Operator {
	EQUAL_TO,                     		// ==   =
	NOT_EQUAL_TO,            			// !=   <>
	LESS_THAN,							// <
	LESS_THAN_OR_EQUAL_TO,				// <=
	GREATER_THAN,						// >
	GREATER_THAN_OR_EQUAL_TO,			// >=
	IS_SUBSTRING_OF,							// $
	CI_EQUAL_TO,                     	// ~==   ~=
	CI_NOT_EQUAL_TO,            		// ~!=   ~<>
	CI_LESS_THAN,						// ~<
	CI_LESS_THAN_OR_EQUAL_TO,			// ~<=
	CI_GREATER_THAN,					// ~>
	CI_GREATER_THAN_OR_EQUAL_TO,		// ~>=
	CI_IS_SUBSTRING_OF,						// ~$
}
