package com.craiglowery.java.vlib.tuple;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** An annotation used in the Tuple subsystem to indicate that
 * an attribute's value should not be set on insert, and that
 * the default value assigned by the backing store should be
 * used instead.  This is usually needed in cases where the
 * backing store assigns unique identifiers, such as serial
 * numbers.
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultOnInsert {

}
