package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import static org.junit.Assert.*;

import org.junit.Test;

/** JUnit test for the configuration subsystem. **/
public class ConfigTest {

	@Test
	public void testIsConfigured() {
		assertTrue(Config.isConfigured());
	}
	
	@Test
	public void showConfiguration() {

	}


}
