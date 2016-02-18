package com.craiglowery.java.vlib.tuple;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Designates that a tuple's field should be ignored by the table adapter
 * when mapping to fields in the backing store. 
 * 
 */
public
@Retention(RetentionPolicy.RUNTIME)
@interface Ignore {

}
