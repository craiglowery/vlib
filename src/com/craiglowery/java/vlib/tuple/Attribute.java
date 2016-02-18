package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** An annotation used in the Tuple subsystem to indicate that
 * an object's attribute is one the repository should map to
 * the backend data store.
 */

public
@Retention(RetentionPolicy.RUNTIME)
@interface Attribute {}