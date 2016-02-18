package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
public class PROG_AddUser {
	
	private static final String passkey="sd90uj*(3l";

	public static void main(String[] args) {
		try {

			
			
			Crypto.putPassword("session", "noissessession", passkey);
			System.out.println(Crypto.getPassword("session", passkey));
		} catch (U_Exception  e) {
			System.out.print(e.getMessage());
			e.printStackTrace(System.out);
			System.out.flush();
		}

	}

}
