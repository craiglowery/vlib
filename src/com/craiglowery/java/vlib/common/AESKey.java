package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */


import java.security.Key;
/**
 * Used to encrypt credentials for transmission and storage.  It is specifically
 * used to store encrypted database credentials in the configuration file
 * for the vlib system.  Only the vlib repository manager can decrypt the
 * credentials and then use them to gain access to the repository.
 */
public class AESKey implements Key {
	private static final long serialVersionUID = -2800596741425032564L;
	public AESKey(byte[] key) { this.key = key; }
	 private byte[] key;
	 @Override public String getAlgorithm() { return "AES"; }
	 @Override public byte[] getEncoded() { return key; }
	 @Override public String getFormat() {	return null;}
 }