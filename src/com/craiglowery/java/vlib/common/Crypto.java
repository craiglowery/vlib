package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.*;

import javax.crypto.*;
import javax.crypto.spec.*;

import com.craiglowery.java.vlib.common.U_Exception.ERROR;

//Need to revise the implementation to never store clear passwords in String, particularly in the
//configuration table (Config)
/** Implements encryption/decryption of database login credentials stored
 * in the vlib configuration file.
 */
public class Crypto {
    
     private static  String ALGO=null;
     private static  byte[] password=null;
     private static  byte[] salt=new byte[]{ 0x03, 0x32, -0x30, 0x56, -0x3A, 0x3F, 0x14, 0x19, -0x4A};
     private static  Base64.Encoder b64encoder=null;
     private static  Base64.Decoder b64decoder=null;
     private static boolean isConfigured = false;
     private static String dbauth_pwdfile = null;
     private static Pattern dbauth_pwdfile_pattern = null;
     
     private static final String passkey="sd90uj*(3l";
     private static final String DBAUTH_PWDFILE_RE = "^([^=]+)=(.*)$";

     private static Exception configurationException = null;
     
     static void initialize(){     
    	 try {
		     ALGO = Config.getString(ConfigurationKey.DB_AUTH_ALGORITHM);
		     password = Config.getString(ConfigurationKey.DB_AUTH_ALGORITHM_KEY).getBytes();
		     dbauth_pwdfile = Config.getString(ConfigurationKey.DB_AUTH_PWDFILE);
		     if (ALGO.length()!=0 && password.length!=0 && dbauth_pwdfile.length()!=0) {
			     b64encoder = Base64.getEncoder();
			     b64decoder = Base64.getDecoder();
			     dbauth_pwdfile_pattern = Pattern.compile(DBAUTH_PWDFILE_RE);
			     isConfigured=true;
		     }
    	 } catch (Exception e) { configurationException = e;}
     }
     
     private static Key computeKey() throws U_Exception {
 		try {
	 		MessageDigest digester = MessageDigest.getInstance("MD5");
	 		digester.update(password);
	 		digester.update(salt);
	 		byte[] key = digester.digest();
			return new SecretKeySpec(key,"AES");
		} catch (NoSuchAlgorithmException e) {
			throw new U_Exception(ERROR.EncryptionError,e);
		}
     }
     
     private static String encrypt(String Data) throws U_Exception {
    	 Key key = null;
		try {
			key = computeKey(); 
			Cipher cipher = Cipher.getInstance(ALGO);
			cipher.init(Cipher.ENCRYPT_MODE, key);
			byte[] encVal = cipher.doFinal(Data.getBytes());
			byte[] b64 = b64encoder.encode(encVal);
			return new String(b64,"US-ASCII");
		} catch (InvalidKeyException | NoSuchAlgorithmException
				| NoSuchPaddingException | IllegalBlockSizeException
				| BadPaddingException | UnsupportedEncodingException e) {
			throw new U_Exception(ERROR.EncryptionError,e);
		}

        
    }

    private static String decrypt(String encryptedData) throws U_Exception {
   	 Key key = null;
		try {
			key = computeKey(); 
			Cipher cipher = Cipher.getInstance(ALGO);
			cipher.init(Cipher.DECRYPT_MODE, key);
			byte[] decVal = b64decoder.decode(encryptedData); 
			byte[] clear = cipher.doFinal(decVal);
			return new String(clear,"UTF-8");
		} catch (InvalidKeyException | NoSuchAlgorithmException
				| NoSuchPaddingException | IllegalBlockSizeException
				| BadPaddingException | UnsupportedEncodingException e) {
			throw new U_Exception(ERROR.EncryptionError,e);
		}
    }
    
    /**
     * Returns the cleartext password associated with a user.
     * @param username The user for which the password is sought.
     * @param pkey A simple secret key needed to access this method.
     * @return The cleartext password, or null if no such user.
     * @throws U_Exception
     */
    public static String getPassword(String username, String pkey) throws U_Exception {
    	if (!isConfigured) throw new U_Exception(ERROR.ConfigurationError,"Crypto",configurationException);
    	if (!pkey.equals(passkey))
    		throw new U_Exception(ERROR.EncryptionError,"Invalid passkey");
    	File f = new File(dbauth_pwdfile);

    	synchronized (passkey) {
	    	try (java.io.BufferedReader in = new java.io.BufferedReader(new FileReader(f))) {
	    		String l = null;
	    		while ( (l=in.readLine()) != null ) {
	    			Matcher m = dbauth_pwdfile_pattern.matcher(l);
	    			if (!m.matches())
	    				throw new U_Exception(ERROR.FileError,"Corrupted dbauth password file");
	    			if (m.group(1).equals(username)) {
	    				return decrypt(m.group(2));
	    			}
	    		}
	    	} catch (IOException e) {
	    		throw new U_Exception(ERROR.IOError,e);
	    	}
    	}
    	return null;
    }
    
    private static Set<PosixFilePermission> OWNER_READWRITE = null;
    static {
    	OWNER_READWRITE = new HashSet<PosixFilePermission>();
    	OWNER_READWRITE.add(PosixFilePermission.OWNER_READ);
    	OWNER_READWRITE.add(PosixFilePermission.OWNER_WRITE);
    }
    
    /**
     * Updates a password for an existing user, or adds a new user to the authentication file.
     * @param username The username to update or to be added.
     * @param password The unencrypted password.
     * @param pkey Simple secret key necessary for access to this method.
     */    
    public static void putPassword(String username, String password, String pkey) throws U_Exception {
    	if (!isConfigured) throw new U_Exception(ERROR.ConfigurationError,"Crypto",configurationException);
    	if (!pkey.equals(passkey))
    		throw new U_Exception(ERROR.EncryptionError,"Invalid passkey");
    	
    	password = encrypt(password);
    	
    	File f_current = new File(dbauth_pwdfile);
		File f_new = new File(dbauth_pwdfile+".new");
		File f_old = new File(dbauth_pwdfile+".old");

    	synchronized (passkey) {
    		if (!f_current.exists()) try {
    			f_current.setReadable(true,true);
    			f_current.setWritable(true,true);
    			f_current.createNewFile();
    		} catch (IOException e) {
    			throw new U_Exception(ERROR.FileError,e);
    		}
    		
	    	try (java.io.BufferedReader in = new java.io.BufferedReader(new FileReader(f_current)); 
	    		 java.io.BufferedWriter out = new java.io.BufferedWriter(new FileWriter(f_new,false))) {
	    		
	    		Files.setPosixFilePermissions(f_new.toPath(), OWNER_READWRITE);
	    		
	    		String l = null;
	    		boolean userFound = false;
	    		while ( (l=in.readLine()) != null ) {
	    			Matcher m = dbauth_pwdfile_pattern.matcher(l);
	    			if (!m.matches())
	    				throw new U_Exception(ERROR.FileError,"Corrupted dbauth password file");
	    			String u = m.group(1);
	    			String p = m.group(2);
	    			if (u.equals(username)) {
	    				p=password;
	    				userFound=true;
	    			}
	    			out.write(u);
	    			out.write("=");
	    			out.write(p);
	    			out.newLine();
	    		}
	    		if (!userFound) {
	    			out.write(username);
	    			out.write("=");
	    			out.write(password);
	    			out.newLine();
	    		}
	    		in.close();
	    		out.close();
	    		
	    		//Aging
	    		if (f_old.exists() && !f_old.delete())
	    			throw new U_Exception(ERROR.FileError,"Deleting old dbauth file");
	    		if (!f_current.renameTo(f_old))
	    			throw new U_Exception(ERROR.FileError,"Renaming current dbauth file to old");
	    		if (!f_new.renameTo(f_current))
	    			throw new U_Exception(ERROR.FileError,"Renaming new dbauth file to current");
	    		f_current=f_new=f_old=null;
	    	} catch (IOException e) {
	    		if (f_new!=null) try { f_new.delete(); } catch (Exception ex) { /* best effort */ }
	    		throw new U_Exception(ERROR.IOError,e);
	    	}
    	}
    }
    
}