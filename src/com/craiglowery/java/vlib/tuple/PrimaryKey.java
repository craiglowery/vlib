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

/** This annotation is used in Tuple's to identify attributes that are
 * also part of the primary key group for the table.  Specifying the
 * {@code @PrimaryKey} annotation implies {@code @Attribute}.
 *
 */
public
@Retention(RetentionPolicy.RUNTIME)
@interface PrimaryKey {}