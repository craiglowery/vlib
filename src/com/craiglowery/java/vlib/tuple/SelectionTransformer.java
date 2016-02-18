package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import com.craiglowery.java.vlib.common.U_Exception;

/** This interface is used for functions, typically lambda's,
 * that a table adapter calls on an object-by-object basis
 * to process the results of a query, rather than return an entire
 * collection of objects.  See the {@code applySelection} method
 * in {@code TableAdapter}.
 * @param <T> The type of tuple the backing table.
 */
public interface SelectionTransformer<T extends Tuple>  {
	/**
	 * Performs an action on the tuple and returns true if the
	 * caller should continue retrieving tuples from the source.
	 * @param tuple
	 * @return True if caller should continue retrieving tuples.
	 */
	public boolean action(T tuple) throws U_Exception;
}
